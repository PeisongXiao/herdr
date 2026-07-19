use std::io;

#[cfg(unix)]
use crate::api::schema::AgentStartTransport;
use crate::api::schema::{
    AgentAttachInfo, AgentInfo, AgentStartParams, AgentTransportInfo, PeerInfo,
};
use crate::detect::AgentState;
use crate::events::AppEvent;
use crate::layout::PaneId;

const REMOTE_MIRROR_STATE_SOURCE: &str = "herdr:remote-mirror";
const REMOTE_MIRROR_METADATA_SOURCE: &str = "herdr:remote-mirror:metadata";
static PENDING_PEER_CLEANUPS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn has_pending_peer_cleanup() -> bool {
    PENDING_PEER_CLEANUPS.load(std::sync::atomic::Ordering::Acquire) != 0
}

pub(crate) fn wait_for_pending_peer_cleanup(timeout: std::time::Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while has_pending_peer_cleanup() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    true
}

#[cfg(unix)]
struct PendingPeerCleanupGuard;

#[cfg(unix)]
impl PendingPeerCleanupGuard {
    fn new() -> Self {
        PENDING_PEER_CLEANUPS.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self
    }
}

#[cfg(unix)]
impl Drop for PendingPeerCleanupGuard {
    fn drop(&mut self) {
        PENDING_PEER_CLEANUPS.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[derive(Debug)]
pub(crate) struct RemoteAgentStart {
    pub(crate) agent: AgentInfo,
    pub(crate) delegation: crate::api::schema::TerminalDelegationInfo,
    pub(crate) attach_argv: Vec<String>,
    pub(crate) transport: AgentTransportInfo,
    pub(crate) peer: Option<PeerInfo>,
    pub(crate) bridge: Option<PeerBridgeRuntime>,
}

impl RemoteAgentStart {
    pub(crate) fn rollback(&mut self) {
        #[cfg(unix)]
        unix::rollback_start(self);

        #[cfg(not(unix))]
        if let Some(bridge) = self.bridge.as_mut() {
            bridge.stop(true);
        }
    }
}

#[derive(Debug)]
pub(crate) struct PeerBridgeRuntime {
    connection_id: String,
    #[cfg(unix)]
    child: std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
    #[cfg(not(unix))]
    child: Option<std::process::Child>,
    #[cfg(unix)]
    pub(crate) remote_api_socket: String,
    #[cfg(unix)]
    connection: SshConnection,
    #[cfg(unix)]
    remote_peer_id: String,
    #[cfg(unix)]
    stopped: bool,
    #[cfg(unix)]
    supervisor_cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(unix)]
    healthy: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(unix)]
    registration_lock: std::sync::Arc<std::sync::Mutex<()>>,
    #[cfg(all(test, unix))]
    test_noop_registration: bool,
}

#[derive(Debug, Default)]
pub(crate) struct PeerBridgeSet {
    bridges: Vec<PeerBridgeRuntime>,
}

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshConnection {
    pub(crate) target: String,
    pub(crate) ssh_args: Vec<String>,
    pub(crate) managed_control_path: Option<String>,
    pub(crate) session: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RemoteShellConnect {
    pub(crate) peer: PeerInfo,
    pub(crate) bridge: PeerBridgeRuntime,
    pub(crate) attach: AgentAttachInfo,
    pub(crate) delegation: crate::api::schema::TerminalDelegationInfo,
}

#[cfg(unix)]
#[derive(Debug, Clone)]
pub(crate) struct PeerBridgeRegistration {
    connection: SshConnection,
    remote_peer_id: String,
    remote_api_socket: String,
    cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    registration_lock: std::sync::Arc<std::sync::Mutex<()>>,
    #[cfg(test)]
    test_noop: bool,
}

#[cfg(unix)]
impl PeerBridgeRegistration {
    pub(crate) fn register(&self) -> io::Result<()> {
        #[cfg(test)]
        if self.test_noop {
            return Ok(());
        }
        if self.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "peer registration was cancelled",
            ));
        }
        let _guard = self
            .registration_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.cancelled.load(std::sync::atomic::Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "peer registration was cancelled",
            ));
        }
        unix::register_local_peer_on_remote(
            &self.connection,
            &self.remote_peer_id,
            &self.remote_api_socket,
        )
    }

    fn unregister_if_current(&self) -> io::Result<()> {
        let _guard = self
            .registration_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        unix::unregister_local_peer_on_remote_if_current(
            &self.connection,
            &self.remote_peer_id,
            &self.remote_api_socket,
        )
    }
}

impl Drop for PeerBridgeRuntime {
    fn drop(&mut self) {
        #[cfg(unix)]
        self.shutdown(false);

        #[cfg(not(unix))]
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl PeerBridgeRuntime {
    #[cfg(test)]
    pub(crate) fn test(connection_id: &str, peer_id: &str) -> Self {
        #[cfg(unix)]
        {
            Self {
                connection_id: connection_id.into(),
                child: std::sync::Arc::new(std::sync::Mutex::new(None)),
                remote_api_socket: "/tmp/herdr-test-peer.sock".into(),
                connection: SshConnection {
                    target: peer_id.into(),
                    ssh_args: Vec::new(),
                    managed_control_path: None,
                    session: None,
                },
                remote_peer_id: peer_id.into(),
                stopped: true,
                supervisor_cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                    false,
                )),
                healthy: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
                registration_lock: std::sync::Arc::new(std::sync::Mutex::new(())),
                test_noop_registration: true,
            }
        }

        #[cfg(not(unix))]
        {
            let _ = peer_id;
            Self {
                connection_id: connection_id.into(),
                child: None,
            }
        }
    }

    pub(crate) fn stop(&mut self, unregister_remote: bool) {
        #[cfg(unix)]
        self.shutdown(unregister_remote);

        #[cfg(not(unix))]
        {
            let _ = unregister_remote;
            if let Some(child) = self.child.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
            self.child = None;
        }
    }

    pub(crate) fn remote_peer_id(&self) -> Option<&str> {
        #[cfg(unix)]
        {
            Some(&self.remote_peer_id)
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    pub(crate) fn connection_id(&self) -> &str {
        &self.connection_id
    }

    pub(crate) fn is_healthy(&self) -> bool {
        #[cfg(unix)]
        {
            self.healthy.load(std::sync::atomic::Ordering::Acquire)
        }
        #[cfg(not(unix))]
        {
            true
        }
    }

    #[cfg(unix)]
    pub(crate) fn peer_info(&self) -> PeerInfo {
        unix::peer_info_for_bridge(self)
    }

    #[cfg(unix)]
    pub(crate) fn registration(&self) -> PeerBridgeRegistration {
        PeerBridgeRegistration {
            connection: self.connection.clone(),
            remote_peer_id: self.remote_peer_id.clone(),
            remote_api_socket: self.remote_api_socket.clone(),
            cancelled: std::sync::Arc::clone(&self.supervisor_cancelled),
            registration_lock: std::sync::Arc::clone(&self.registration_lock),
            #[cfg(test)]
            test_noop: self.test_noop_registration,
        }
    }
}

impl PeerBridgeSet {
    pub(crate) fn push(&mut self, bridge: PeerBridgeRuntime) -> String {
        let connection_id = bridge.connection_id().to_string();
        self.bridges.push(bridge);
        connection_id
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bridges.is_empty()
    }

    pub(crate) fn latest(&self) -> Option<&PeerBridgeRuntime> {
        self.bridges.last()
    }

    pub(crate) fn contains(&self, connection_id: &str) -> bool {
        self.bridges
            .iter()
            .any(|bridge| bridge.connection_id() == connection_id)
    }

    pub(crate) fn has_healthy_bridge(&self) -> bool {
        self.bridges.iter().any(PeerBridgeRuntime::is_healthy)
    }

    pub(crate) fn remove(&mut self, connection_id: &str) -> Option<PeerBridgeRuntime> {
        let index = self
            .bridges
            .iter()
            .position(|bridge| bridge.connection_id() == connection_id)?;
        Some(self.bridges.remove(index))
    }

    pub(crate) fn stop_all(&mut self, unregister_remote: bool) {
        for bridge in &mut self.bridges {
            bridge.stop(unregister_remote);
        }
        self.bridges.clear();
    }
}

impl Drop for PeerBridgeSet {
    fn drop(&mut self) {
        self.stop_all(true);
    }
}

#[cfg(unix)]
pub(crate) fn start(params: &AgentStartParams) -> io::Result<RemoteAgentStart> {
    unix::start(params)
}

#[cfg(unix)]
pub(crate) fn api_request(
    target: &str,
    ssh_args: &[String],
    managed_control_path: Option<&str>,
    session: Option<&str>,
    request: &crate::api::schema::Request,
) -> io::Result<serde_json::Value> {
    unix::api_request(target, ssh_args, managed_control_path, session, request)
}

#[cfg(unix)]
pub(crate) fn connect_shell(
    params: &crate::api::schema::PeerConnectSshParams,
) -> io::Result<RemoteShellConnect> {
    unix::connect_shell(params)
}

/// Ask the remote host to hand a delegated pane back into its own workspace
/// list. Used by automatic handoff on graceful server stop.
#[cfg(unix)]
pub(crate) fn handoff_delegated_terminal(
    peer: &PeerInfo,
    delegation: &crate::api::schema::TerminalDelegationInfo,
) -> io::Result<()> {
    unix::handoff_delegated_terminal(peer, delegation)
}

/// Re-acquire a pane that was handed off to its host by a previous graceful
/// server stop. `managed_control_path` comes from an interactive CLI retry
/// (`herdr remote-resume`); automatic re-acquire passes `None` and relies on
/// BatchMode-compatible SSH authentication (keys or agent).
#[cfg(unix)]
pub(crate) fn reacquire(
    record: &crate::remote_resume::ResumeRecord,
    managed_control_path: Option<String>,
) -> io::Result<RemoteAgentStart> {
    unix::reacquire(record, managed_control_path)
}

/// Roll back a failed re-acquire: abandon the uncommitted delegation and stop
/// the peer bridge. Unlike `RemoteAgentStart::rollback` this never closes a
/// remote tab, because re-acquire never creates one.
#[cfg(unix)]
pub(crate) fn rollback_reacquire(start: &mut RemoteAgentStart) {
    unix::rollback_reacquire(start)
}

#[cfg(unix)]
pub(crate) fn peer_id_for_connect_params(
    params: &crate::api::schema::PeerConnectSshParams,
) -> String {
    unix::peer_id_for_connect_params(params)
}

#[cfg(unix)]
pub(crate) fn local_peer_id() -> String {
    unix::local_peer_id()
}

#[cfg(not(unix))]
pub(crate) fn local_peer_id() -> String {
    "local".into()
}

#[cfg(not(unix))]
pub(crate) fn peer_id_for_connect_params(
    params: &crate::api::schema::PeerConnectSshParams,
) -> String {
    params.target.clone()
}

#[cfg(not(unix))]
pub(crate) fn start(_params: &AgentStartParams) -> io::Result<RemoteAgentStart> {
    Err(io::Error::other(
        "SSH agent transport is not supported on Windows yet",
    ))
}

#[cfg(not(unix))]
pub(crate) fn api_request(
    _target: &str,
    _ssh_args: &[String],
    _managed_control_path: Option<&str>,
    _session: Option<&str>,
    _request: &crate::api::schema::Request,
) -> io::Result<serde_json::Value> {
    Err(io::Error::other(
        "SSH API transport is not supported on Windows yet",
    ))
}

#[cfg(not(unix))]
pub(crate) fn connect_shell(
    _params: &crate::api::schema::PeerConnectSshParams,
) -> io::Result<RemoteShellConnect> {
    Err(io::Error::other(
        "SSH peer integration is not supported on Windows yet",
    ))
}

pub(crate) fn attach_argv_for_agent_attach(
    attach: &AgentAttachInfo,
    takeover: bool,
) -> Option<Vec<String>> {
    match attach {
        AgentAttachInfo::Ssh {
            target,
            ssh_args,
            managed_control_path,
            session,
            terminal_id,
            delegation,
        } => Some(ssh_attach_argv(
            target,
            ssh_args,
            managed_control_path.as_deref(),
            session.as_deref(),
            terminal_id,
            takeover,
            delegation.as_ref(),
        )),
        AgentAttachInfo::SshShell {
            target,
            ssh_args,
            managed_control_path,
            session,
            label,
        } => Some(ssh_shell_argv(
            target,
            ssh_args,
            managed_control_path.as_deref(),
            session.as_deref(),
            label.as_deref(),
        )),
    }
}

pub(crate) fn delegation_was_activated(attach: &AgentAttachInfo) -> io::Result<Option<bool>> {
    Ok(delegation_status(attach)?.map(|status| {
        matches!(
            status,
            crate::api::schema::TerminalDelegationStatus::Active
                | crate::api::schema::TerminalDelegationStatus::TakenOver
                | crate::api::schema::TerminalDelegationStatus::HandedOff
                | crate::api::schema::TerminalDelegationStatus::Terminated
        )
    }))
}

fn delegation_status(
    attach: &AgentAttachInfo,
) -> io::Result<Option<crate::api::schema::TerminalDelegationStatus>> {
    let AgentAttachInfo::Ssh {
        target,
        ssh_args,
        managed_control_path,
        session,
        delegation: Some(delegation),
        ..
    } = attach
    else {
        return Ok(None);
    };

    #[cfg(unix)]
    {
        unix::delegation_status(
            target,
            ssh_args,
            managed_control_path.as_deref(),
            session.as_deref(),
            delegation,
        )
        .map(Some)
    }

    #[cfg(not(unix))]
    {
        let _ = (target, ssh_args, managed_control_path, session, delegation);
        Ok(None)
    }
}

pub(crate) fn spawn_delegation_activation_watch(
    attach: AgentAttachInfo,
    event_tx: tokio::sync::mpsc::Sender<AppEvent>,
) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let worker_cancelled = std::sync::Arc::clone(&cancelled);
    let delegation_id = match &attach {
        AgentAttachInfo::Ssh {
            delegation: Some(claim),
            ..
        } => claim.delegation_id.clone(),
        _ => return cancelled,
    };
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(35);
        while !worker_cancelled.load(std::sync::atomic::Ordering::Acquire) && !event_tx.is_closed()
        {
            match delegation_status(&attach) {
                Ok(Some(crate::api::schema::TerminalDelegationStatus::Pending))
                    if std::time::Instant::now() >= deadline =>
                {
                    let _ =
                        event_tx.blocking_send(AppEvent::RemotePresentationActivationObserved {
                            delegation_id,
                            activated: false,
                        });
                    return;
                }
                Ok(Some(crate::api::schema::TerminalDelegationStatus::Pending)) => {}
                Ok(Some(crate::api::schema::TerminalDelegationStatus::Failed)) | Ok(None) => {
                    let _ =
                        event_tx.blocking_send(AppEvent::RemotePresentationActivationObserved {
                            delegation_id,
                            activated: false,
                        });
                    return;
                }
                Ok(Some(_)) => {
                    let _ =
                        event_tx.blocking_send(AppEvent::RemotePresentationActivationObserved {
                            delegation_id,
                            activated: true,
                        });
                    return;
                }
                Err(_) => {}
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });
    cancelled
}

pub(crate) fn abandon_unactivated_delegation(attach: &AgentAttachInfo) -> io::Result<()> {
    let AgentAttachInfo::Ssh {
        target,
        ssh_args,
        managed_control_path,
        session,
        delegation: Some(delegation),
        ..
    } = attach
    else {
        return Ok(());
    };

    #[cfg(unix)]
    {
        unix::abandon_delegation(
            target,
            ssh_args,
            managed_control_path.as_deref(),
            session.as_deref(),
            delegation,
        )
    }

    #[cfg(not(unix))]
    {
        let _ = (target, ssh_args, managed_control_path, session, delegation);
        Ok(())
    }
}

pub(crate) fn spawn_mirror(
    transport: AgentTransportInfo,
    local_pane_id: PaneId,
    event_tx: tokio::sync::mpsc::Sender<AppEvent>,
) -> std::sync::Arc<std::sync::atomic::AtomicBool> {
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    #[cfg(unix)]
    unix::spawn_mirror(
        transport,
        local_pane_id,
        event_tx,
        std::sync::Arc::clone(&cancelled),
    );

    #[cfg(not(unix))]
    {
        let _ = (transport, local_pane_id, event_tx);
    }
    cancelled
}

pub(crate) fn events_from_agent_info(
    local_pane_id: PaneId,
    info: &AgentInfo,
    fallback_agent: Option<&str>,
) -> Vec<AppEvent> {
    let agent_label = normalize_remote_agent_label([
        info.agent.as_deref(),
        info.display_agent.as_deref(),
        fallback_agent,
    ]);
    let title = normalize_remote_presentation_text(info.title.as_deref(), 80);
    let display_agent = normalize_remote_presentation_text(info.display_agent.as_deref(), 80);
    let custom_status = normalize_remote_presentation_text(info.custom_status.as_deref(), 32);
    let state_labels = normalize_remote_state_labels(&info.state_labels);
    vec![
        AppEvent::HookStateReported {
            pane_id: local_pane_id,
            source: REMOTE_MIRROR_STATE_SOURCE.to_string(),
            agent_label: agent_label.clone(),
            state: status_to_state(info.agent_status),
            message: None,
            custom_status: custom_status.clone(),
            seq: None,
            session_ref: None,
        },
        AppEvent::HookMetadataReported {
            pane_id: local_pane_id,
            source: REMOTE_MIRROR_METADATA_SOURCE.to_string(),
            agent_label: Some(agent_label),
            applies_to_source: Some(REMOTE_MIRROR_STATE_SOURCE.to_string()),
            clear_title: title.is_none(),
            clear_display_agent: display_agent.is_none(),
            clear_custom_status: custom_status.is_none(),
            clear_state_labels: state_labels.is_empty(),
            title,
            display_agent,
            custom_status,
            state_labels,
            seq: None,
            ttl: None,
        },
    ]
}

fn normalize_remote_agent_label<const N: usize>(candidates: [Option<&str>; N]) -> String {
    for candidate in candidates.into_iter().flatten() {
        let Some(candidate) = normalize_remote_presentation_text(Some(candidate), 80) else {
            continue;
        };
        if let Some(agent) = crate::detect::parse_agent_label(&candidate) {
            return crate::detect::agent_label(agent).to_string();
        }
        return candidate;
    }
    "remote".to_string()
}

fn normalize_remote_presentation_text(value: Option<&str>, max_chars: usize) -> Option<String> {
    let normalized = value?
        .trim()
        .chars()
        .filter(|ch| !ch.is_control())
        .take(max_chars)
        .collect::<String>();
    let normalized = normalized.trim();
    (!normalized.is_empty()).then(|| normalized.to_string())
}

fn normalize_remote_state_labels(
    labels: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    labels
        .iter()
        .filter_map(|(state, label)| {
            let state = state.trim().to_ascii_lowercase();
            if !matches!(
                state.as_str(),
                "idle" | "working" | "blocked" | "done" | "unknown"
            ) {
                return None;
            }
            normalize_remote_presentation_text(Some(label), 80).map(|label| (state, label))
        })
        .collect()
}

fn status_to_state(status: crate::api::schema::AgentStatus) -> AgentState {
    match status {
        crate::api::schema::AgentStatus::Idle | crate::api::schema::AgentStatus::Done => {
            AgentState::Idle
        }
        crate::api::schema::AgentStatus::Working => AgentState::Working,
        crate::api::schema::AgentStatus::Blocked => AgentState::Blocked,
        crate::api::schema::AgentStatus::Unknown => AgentState::Unknown,
    }
}

fn ssh_attach_argv(
    target: &str,
    ssh_args: &[String],
    managed_control_path: Option<&str>,
    session: Option<&str>,
    remote_terminal_id: &str,
    takeover: bool,
    delegation: Option<&crate::api::schema::TerminalDelegationClaim>,
) -> Vec<String> {
    let mut argv = vec![
        crate::ssh_integration::real_ssh_program()
            .display()
            .to_string(),
        "-tt".to_string(),
    ];
    argv.extend(ssh_args.iter().cloned());
    push_managed_control_args(&mut argv, managed_control_path);
    argv.push(target.to_string());
    argv.push(remote_terminal_attach_command(
        session,
        remote_terminal_id,
        takeover,
        delegation,
    ));
    argv
}

fn ssh_shell_argv(
    target: &str,
    ssh_args: &[String],
    managed_control_path: Option<&str>,
    session: Option<&str>,
    label: Option<&str>,
) -> Vec<String> {
    let mut argv = vec![
        crate::ssh_integration::real_ssh_program()
            .display()
            .to_string(),
        "-tt".to_string(),
        "-o".to_string(),
        "RemoteCommand=none".to_string(),
    ];
    argv.extend(ssh_args.iter().cloned());
    push_managed_control_args(&mut argv, managed_control_path);
    argv.push(target.to_string());
    argv.push(remote_terminal_shell_command(session, label));
    argv
}

fn push_managed_control_args(argv: &mut Vec<String>, managed_control_path: Option<&str>) {
    if let Some(path) = managed_control_path {
        argv.extend([
            "-o".to_string(),
            "ControlMaster=auto".to_string(),
            "-S".to_string(),
            path.to_string(),
        ]);
    }
}

fn remote_terminal_attach_command(
    session: Option<&str>,
    remote_terminal_id: &str,
    takeover: bool,
    delegation: Option<&crate::api::schema::TerminalDelegationClaim>,
) -> String {
    let mut command = remote_herdr_exec_prefix();
    if let Some(session) = session.filter(|session| !session.is_empty()) {
        command.push_str(" --session ");
        command.push_str(&shell_quote(session));
    }
    command.push_str(" terminal attach ");
    command.push_str(&shell_quote(remote_terminal_id));
    if takeover {
        command.push_str(" --takeover");
    }
    if let Some(delegation) = delegation {
        command.push_str(" --delegation ");
        command.push_str(&shell_quote(&delegation.delegation_id));
        command.push_str(" --delegation-epoch ");
        command.push_str(&delegation.epoch.to_string());
    }
    command
}

fn remote_terminal_shell_command(session: Option<&str>, label: Option<&str>) -> String {
    let mut command = remote_herdr_exec_prefix();
    if let Some(session) = session.filter(|session| !session.is_empty()) {
        command.push_str(" --session ");
        command.push_str(&shell_quote(session));
    }
    command.push_str(" terminal shell");
    if let Some(label) = label.filter(|label| !label.is_empty()) {
        command.push_str(" --label ");
        command.push_str(&shell_quote(label));
    }
    command
}

fn remote_herdr_exec_prefix() -> String {
    let script = format!(
        "{}\nexec \"$herdr_bin\" \"$@\"",
        remote_herdr_resolver_script()
    );
    format!("sh -c {} herdr", shell_quote(&script))
}

fn remote_herdr_resolver_script() -> String {
    let version = crate::build_info::version();
    format!(
        r#"herdr_bin=$(command -v herdr 2>/dev/null || true)
if [ -z "$herdr_bin" ] || [ ! -x "$herdr_bin" ]; then
  for candidate in \
    "$HOME/.local/bin/herdr" \
    "/opt/homebrew/bin/herdr" \
    "/usr/local/bin/herdr" \
    "/home/linuxbrew/.linuxbrew/bin/herdr" \
    "$HOME/.local/share/mise/installs/herdr/{version}/bin/herdr" \
    "$HOME/.local/share/mise/installs/github-ogulcancelik-herdr/{version}/herdr" \
    "$HOME/.nix-profile/bin/herdr" \
    "/etc/profiles/per-user/${{USER:-}}/bin/herdr" \
    "/nix/var/nix/profiles/default/bin/herdr" \
    "/run/current-system/sw/bin/herdr"
  do
    if [ -x "$candidate" ]; then herdr_bin=$candidate; break; fi
  done
fi
if [ -z "$herdr_bin" ] || [ ! -x "$herdr_bin" ]; then
  echo 'Herdr is not installed in PATH or a known per-user install location on the remote host.' >&2
  exit 127
fi"#
    )
}

fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '@' | '%' | '_' | '+' | '=' | ':' | ',' | '.' | '/' | '-'
                )
        })
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(unix)]
pub(crate) fn validate_remote_agent_selection(
    agent: Option<&str>,
    argv: &[String],
    prepare_integration: bool,
) -> Result<(Option<String>, Option<String>), String> {
    let agent_label = if let Some(agent) = agent {
        crate::detect::parse_agent_label(agent)
            .map(crate::detect::agent_label)
            .map(str::to_string)
            .map(Some)
            .ok_or_else(|| format!("unsupported agent label for --agent: {agent}"))?
    } else {
        argv.first()
            .and_then(|argv0| crate::detect::identify_agent(argv0))
            .map(crate::detect::agent_label)
            .map(str::to_string)
    };

    validate_remote_integration_label(agent_label, prepare_integration)
}

#[cfg(unix)]
fn validate_remote_integration_label(
    agent_label: Option<String>,
    prepare_integration: bool,
) -> Result<(Option<String>, Option<String>), String> {
    let integration_label = if prepare_integration {
        let label = agent_label.as_deref().ok_or_else(|| {
            "remote integration preparation needs --agent AGENT or a recognizable argv[0]; pass --no-remote-integration to skip it".to_string()
        })?;
        if integration_target_for_label(label).is_none() {
            return Err(format!(
                "agent {label} does not have a Herdr integration installer"
            ));
        }
        Some(label.to_string())
    } else {
        None
    };
    Ok((agent_label, integration_label))
}

#[cfg(unix)]
fn integration_target_for_label(label: &str) -> Option<crate::api::schema::IntegrationTarget> {
    use crate::api::schema::IntegrationTarget;
    match label {
        "pi" => Some(IntegrationTarget::Pi),
        "omp" => Some(IntegrationTarget::Omp),
        "claude" => Some(IntegrationTarget::Claude),
        "codex" => Some(IntegrationTarget::Codex),
        "copilot" => Some(IntegrationTarget::Copilot),
        "devin" => Some(IntegrationTarget::Devin),
        "droid" => Some(IntegrationTarget::Droid),
        "kimi" => Some(IntegrationTarget::Kimi),
        "opencode" => Some(IntegrationTarget::Opencode),
        "kilo" => Some(IntegrationTarget::Kilo),
        "hermes" => Some(IntegrationTarget::Hermes),
        "qodercli" => Some(IntegrationTarget::Qodercli),
        "cursor" => Some(IntegrationTarget::Cursor),
        _ => None,
    }
}

#[cfg(unix)]
mod unix {
    use std::io::{self, BufRead, BufReader, Write};
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::Duration;

    use serde::Deserialize;

    use super::*;
    use crate::api::schema::{PeerRegisterParams, PeerStatus, PeerTransportInfo, Request};

    const SSH_API_TIMEOUT: Duration = Duration::from_secs(8);
    const MAX_SSH_OUTPUT_BYTES: usize = 256 * 1024;
    const MAX_MIRROR_EVENT_LINE_BYTES: usize = 64 * 1024;
    const MIRROR_EVENT_QUEUE_CAPACITY: usize = 64;
    const MIRROR_INFO_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
    const UNMANAGED_FORWARD_FAILURE_GRACE: Duration = Duration::from_secs(1);
    const SSH_OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
    const SSH_PROCESS_KILL_GRACE: Duration = Duration::from_secs(1);

    pub(super) fn start(params: &AgentStartParams) -> io::Result<RemoteAgentStart> {
        let Some(AgentStartTransport::Ssh {
            target,
            ssh_args,
            managed_control_path,
            session,
            prepare_integration,
        }) = params.transport.as_ref()
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "remote agent start requires an SSH transport",
            ));
        };
        let (agent_label, integration_label) = validate_remote_agent_selection(
            params.agent.as_deref(),
            &params.argv,
            *prepare_integration,
        )
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
        let connection = SshConnection {
            target: target.clone(),
            ssh_args: ssh_args.clone(),
            managed_control_path: managed_control_path.clone(),
            session: Some(explicit_remote_session(session.as_deref())?),
        };
        validate_ssh_target(&connection.target)?;
        validate_managed_control_connection(&connection)?;
        validate_effective_ssh_config(&connection)?;
        ensure_remote_herdr(&connection)?;
        verify_remote_peer_capability(&connection)?;
        if let Some(label) = integration_label.as_deref() {
            install_remote_integration(&connection, label)?;
        }

        let (peer, mut bridge) = start_reverse_peer_bridge(&connection)?;
        if let Err(err) = bridge.registration().register() {
            let err = unmanaged_bridge_failure(&bridge.child, err);
            bridge.stop(true);
            return Err(err);
        }
        drain_unmanaged_bridge_stderr(&bridge.child);
        if let Err(err) = bridge.start_supervisor() {
            bridge.stop(true);
            return Err(err);
        }
        let remote_agent = match start_remote_agent(&connection, params) {
            Ok(agent) => agent,
            Err(err) => {
                bridge.stop(true);
                return Err(err);
            }
        };
        let owner_peer_id = local_peer_id();
        let delegation = match prepare_remote_terminal_claim(
            &connection,
            crate::api::schema::TerminalDelegateClaimParams {
                target: remote_agent.terminal_id.clone(),
                owner: crate::api::schema::TerminalPresentationOwner {
                    peer_id: owner_peer_id.clone(),
                    pane_id: "remote-agent".into(),
                    route: vec![owner_peer_id],
                },
                takeover: false,
                terminate_on_expire: true,
            },
        ) {
            Ok(delegation) => delegation,
            Err(err) => {
                let _ = close_remote_tab(&connection, &remote_agent.tab_id);
                bridge.stop(true);
                return Err(err);
            }
        };
        let delegation_claim = crate::api::schema::TerminalDelegationClaim {
            delegation_id: delegation.delegation_id.clone(),
            epoch: delegation.epoch,
        };
        let attach_argv = super::ssh_attach_argv(
            &connection.target,
            &connection.ssh_args,
            connection.managed_control_path.as_deref(),
            connection.session.as_deref(),
            &remote_agent.terminal_id,
            false,
            Some(&delegation_claim),
        );
        let transport = AgentTransportInfo::Ssh {
            target: connection.target.clone(),
            ssh_args: connection.ssh_args.clone(),
            managed_control_path: connection.managed_control_path.clone(),
            session: connection.session.clone(),
            remote_terminal_id: remote_agent.terminal_id.clone(),
            remote_pane_id: remote_agent.pane_id.clone(),
            remote_agent: remote_agent.agent.clone().or(agent_label),
            remote_cwd: remote_agent.cwd.clone(),
        };

        Ok(RemoteAgentStart {
            agent: remote_agent,
            delegation,
            attach_argv,
            transport,
            peer: Some(peer),
            bridge: Some(bridge),
        })
    }

    pub(super) fn rollback_start(start: &mut RemoteAgentStart) {
        let AgentTransportInfo::Ssh {
            target,
            ssh_args,
            managed_control_path,
            session,
            ..
        } = &start.transport;
        let connection = SshConnection {
            target: target.clone(),
            ssh_args: ssh_args.clone(),
            managed_control_path: managed_control_path.clone(),
            session: session.clone(),
        };
        let tab_id = start.agent.tab_id.clone();
        let bridge = start.bridge.take();
        let cleanup_guard = PendingPeerCleanupGuard::new();
        thread::spawn(move || {
            let _cleanup_guard = cleanup_guard;
            if let Err(err) = close_remote_tab(&connection, &tab_id) {
                tracing::warn!(%err, tab = %tab_id, "could not roll back remote agent tab");
            }
            if let Some(mut bridge) = bridge {
                bridge.stop(true);
            }
        });
    }

    pub(super) fn api_request(
        target: &str,
        ssh_args: &[String],
        managed_control_path: Option<&str>,
        session: Option<&str>,
        request: &crate::api::schema::Request,
    ) -> io::Result<serde_json::Value> {
        let value = serde_json::to_value(request).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to encode peer API request: {err}"),
            )
        })?;
        remote_api_request(
            &SshConnection {
                target: target.to_string(),
                ssh_args: ssh_args.to_vec(),
                managed_control_path: managed_control_path.map(str::to_string),
                session: session.map(str::to_string),
            },
            value,
        )
    }

    pub(super) fn delegation_status(
        target: &str,
        ssh_args: &[String],
        managed_control_path: Option<&str>,
        session: Option<&str>,
        delegation: &crate::api::schema::TerminalDelegationClaim,
    ) -> io::Result<crate::api::schema::TerminalDelegationStatus> {
        let response = api_request(
            target,
            ssh_args,
            managed_control_path,
            session,
            &crate::api::schema::Request {
                id: "remote-terminal:delegate:status".into(),
                method: crate::api::schema::Method::TerminalDelegateStatus(
                    crate::api::schema::TerminalDelegationTarget {
                        delegation_id: delegation.delegation_id.clone(),
                        epoch: delegation.epoch,
                    },
                ),
            },
        )?;
        if let Some(error) = response.get("error") {
            if error["code"].as_str() == Some("terminal_delegation_not_found") {
                return Ok(crate::api::schema::TerminalDelegationStatus::Failed);
            }
            return Err(io::Error::other(
                error["message"]
                    .as_str()
                    .unwrap_or("remote Herdr could not read terminal delegation status")
                    .to_string(),
            ));
        }
        let info: crate::api::schema::TerminalDelegationInfo =
            serde_json::from_value(response["result"]["delegation"].clone()).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("remote Herdr returned invalid delegation status: {err}"),
                )
            })?;
        Ok(info.status)
    }

    pub(super) fn abandon_delegation(
        target: &str,
        ssh_args: &[String],
        managed_control_path: Option<&str>,
        session: Option<&str>,
        delegation: &crate::api::schema::TerminalDelegationClaim,
    ) -> io::Result<()> {
        let response = api_request(
            target,
            ssh_args,
            managed_control_path,
            session,
            &crate::api::schema::Request {
                id: "remote-terminal:delegate:abandon".into(),
                method: crate::api::schema::Method::TerminalDelegateTerminate(
                    crate::api::schema::TerminalDelegationTarget {
                        delegation_id: delegation.delegation_id.clone(),
                        epoch: delegation.epoch,
                    },
                ),
            },
        )?;
        if let Some(error) = response.get("error") {
            if error["code"].as_str() == Some("terminal_delegation_not_found") {
                return Ok(());
            }
            return Err(io::Error::other(
                error["message"]
                    .as_str()
                    .unwrap_or("remote Herdr could not abandon terminal delegation")
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn prepare_remote_terminal(
        connection: &SshConnection,
        params: crate::api::schema::TerminalDelegateCreateParams,
    ) -> io::Result<crate::api::schema::TerminalDelegationInfo> {
        let request = crate::api::schema::Request {
            id: "remote-terminal:delegate:create".into(),
            method: crate::api::schema::Method::TerminalDelegateCreate(params),
        };
        let value = serde_json::to_value(request).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to encode remote terminal delegation request: {err}"),
            )
        })?;
        let response = remote_api_request(connection, value)?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(format!(
                "remote Herdr could not prepare an exclusive terminal: {message}"
            )));
        }
        serde_json::from_value(response["result"]["delegation"].clone()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("remote Herdr returned an invalid terminal delegation: {err}"),
            )
        })
    }

    fn prepare_remote_terminal_claim(
        connection: &SshConnection,
        params: crate::api::schema::TerminalDelegateClaimParams,
    ) -> io::Result<crate::api::schema::TerminalDelegationInfo> {
        let request = crate::api::schema::Request {
            id: "remote-terminal:delegate:claim".into(),
            method: crate::api::schema::Method::TerminalDelegateClaim(params),
        };
        let value = serde_json::to_value(request).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("failed to encode remote terminal delegation request: {err}"),
            )
        })?;
        let response = remote_api_request(connection, value)?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(format!(
                "remote Herdr could not delegate the terminal: {message}"
            )));
        }
        serde_json::from_value(response["result"]["delegation"].clone()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("remote Herdr returned an invalid terminal delegation: {err}"),
            )
        })
    }

    pub(super) fn connect_shell(
        params: &crate::api::schema::PeerConnectSshParams,
    ) -> io::Result<RemoteShellConnect> {
        let connection = SshConnection {
            target: params.target.clone(),
            ssh_args: params.ssh_args.clone(),
            managed_control_path: params.managed_control_path.clone(),
            session: Some(explicit_remote_session(params.session.as_deref())?),
        };
        validate_ssh_target(&connection.target)?;
        validate_managed_control_connection(&connection)?;
        validate_effective_ssh_config(&connection)?;
        ensure_remote_herdr(&connection)?;
        verify_remote_peer_capability(&connection)?;
        let (peer, mut bridge) = start_reverse_peer_bridge(&connection)?;
        if let Err(err) = bridge.registration().register() {
            let err = unmanaged_bridge_failure(&bridge.child, err);
            bridge.stop(true);
            return Err(err);
        }
        drain_unmanaged_bridge_stderr(&bridge.child);
        if let Err(err) = bridge.start_supervisor() {
            bridge.stop(true);
            return Err(err);
        }
        let owner = params.owner.clone().unwrap_or_else(|| {
            let peer_id = local_peer_id();
            crate::api::schema::TerminalPresentationOwner {
                peer_id: peer_id.clone(),
                pane_id: params
                    .owner_pane_id
                    .clone()
                    .unwrap_or_else(|| "current".into()),
                route: vec![peer_id],
            }
        });
        let delegation = match prepare_remote_terminal(
            &connection,
            crate::api::schema::TerminalDelegateCreateParams {
                cwd: None,
                label: params.label.clone(),
                env: std::collections::HashMap::new(),
                owner,
            },
        ) {
            Ok(delegation) => delegation,
            Err(err) => {
                bridge.stop(true);
                return Err(err);
            }
        };
        Ok(RemoteShellConnect {
            peer,
            bridge,
            attach: AgentAttachInfo::Ssh {
                target: connection.target,
                ssh_args: connection.ssh_args,
                managed_control_path: connection.managed_control_path,
                session: connection.session,
                terminal_id: delegation.terminal_id.clone(),
                delegation: Some(crate::api::schema::TerminalDelegationClaim {
                    delegation_id: delegation.delegation_id.clone(),
                    epoch: delegation.epoch,
                }),
            },
            delegation,
        })
    }

    pub(super) fn peer_id_for_connect_params(
        params: &crate::api::schema::PeerConnectSshParams,
    ) -> String {
        peer_id_for_ssh(&SshConnection {
            target: params.target.clone(),
            ssh_args: params.ssh_args.clone(),
            managed_control_path: params.managed_control_path.clone(),
            session: params.session.clone(),
        })
    }

    fn ssh_connection_for_peer(peer: &PeerInfo) -> io::Result<SshConnection> {
        let crate::api::schema::PeerTransportInfo::Ssh {
            target,
            ssh_args,
            managed_control_path,
            session,
        } = &peer.transport
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("peer {} is not an SSH peer", peer.id),
            ));
        };
        Ok(SshConnection {
            target: target.clone(),
            ssh_args: ssh_args.clone(),
            managed_control_path: managed_control_path.clone(),
            session: session.clone(),
        })
    }

    pub(super) fn handoff_delegated_terminal(
        peer: &PeerInfo,
        delegation: &crate::api::schema::TerminalDelegationInfo,
    ) -> io::Result<()> {
        let connection = ssh_connection_for_peer(peer)?;
        let response = remote_api_request(
            &connection,
            serde_json::json!({
                "id": "remote-terminal:delegate:handoff",
                "method": "terminal.delegate.handoff",
                "params": { "pane_id": delegation.pane_id },
            }),
        )?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(format!(
                "remote Herdr could not hand off the terminal: {message}"
            )));
        }
        Ok(())
    }

    pub(super) fn reacquire(
        record: &crate::remote_resume::ResumeRecord,
        managed_control_path: Option<String>,
    ) -> io::Result<RemoteAgentStart> {
        let connection = SshConnection {
            target: record.ssh.target.clone(),
            ssh_args: record.ssh.ssh_args.clone(),
            managed_control_path,
            session: Some(explicit_remote_session(record.ssh.session.as_deref())?),
        };
        validate_ssh_target(&connection.target)?;
        validate_managed_control_connection(&connection)?;
        validate_effective_ssh_config(&connection)?;
        ensure_remote_herdr(&connection)?;
        verify_remote_peer_capability(&connection)?;
        let (peer, mut bridge) = start_reverse_peer_bridge(&connection)?;
        if let Err(err) = bridge.registration().register() {
            let err = unmanaged_bridge_failure(&bridge.child, err);
            bridge.stop(true);
            return Err(err);
        }
        drain_unmanaged_bridge_stderr(&bridge.child);
        if let Err(err) = bridge.start_supervisor() {
            bridge.stop(true);
            return Err(err);
        }
        // Confirm the handed-off pane still exists before claiming it; a pane
        // that exited on its host must not leave a dangling delegation.
        let agent = match remote_agent_get(&connection, &record.remote_pane_id) {
            Ok(agent) => agent,
            Err(err) => {
                bridge.stop(true);
                return Err(err);
            }
        };
        let owner_peer_id = local_peer_id();
        let delegation = match prepare_remote_terminal_claim(
            &connection,
            crate::api::schema::TerminalDelegateClaimParams {
                target: record.remote_terminal_id.clone(),
                owner: crate::api::schema::TerminalPresentationOwner {
                    peer_id: owner_peer_id.clone(),
                    pane_id: "remote-resume".into(),
                    route: vec![owner_peer_id],
                },
                takeover: false,
                // A failed re-acquire must leave the pane alive on its host.
                terminate_on_expire: false,
            },
        ) {
            Ok(delegation) => delegation,
            Err(err) => {
                bridge.stop(true);
                return Err(err);
            }
        };
        let delegation_claim = crate::api::schema::TerminalDelegationClaim {
            delegation_id: delegation.delegation_id.clone(),
            epoch: delegation.epoch,
        };
        let attach_argv = super::ssh_attach_argv(
            &connection.target,
            &connection.ssh_args,
            connection.managed_control_path.as_deref(),
            connection.session.as_deref(),
            &record.remote_terminal_id,
            false,
            Some(&delegation_claim),
        );
        let transport = AgentTransportInfo::Ssh {
            target: connection.target.clone(),
            ssh_args: connection.ssh_args.clone(),
            managed_control_path: connection.managed_control_path.clone(),
            session: connection.session.clone(),
            remote_terminal_id: record.remote_terminal_id.clone(),
            remote_pane_id: record.remote_pane_id.clone(),
            remote_agent: agent.agent.clone(),
            remote_cwd: agent.cwd.clone(),
        };
        Ok(RemoteAgentStart {
            agent,
            delegation,
            attach_argv,
            transport,
            peer: Some(peer),
            bridge: Some(bridge),
        })
    }

    pub(super) fn rollback_reacquire(start: &mut RemoteAgentStart) {
        if let Some(mut bridge) = start.bridge.take() {
            let connection = bridge.connection.clone();
            let terminate = crate::api::schema::Request {
                id: "remote-terminal:delegate:abandon".into(),
                method: crate::api::schema::Method::TerminalDelegateTerminate(
                    crate::api::schema::TerminalDelegationTarget {
                        delegation_id: start.delegation.delegation_id.clone(),
                        epoch: start.delegation.epoch,
                    },
                ),
            };
            let value = serde_json::to_value(terminate);
            if let Ok(value) = value {
                let _ = remote_api_request(&connection, value);
            }
            bridge.stop(true);
        }
    }

    pub(super) fn spawn_mirror(
        transport: AgentTransportInfo,
        local_pane_id: PaneId,
        event_tx: tokio::sync::mpsc::Sender<AppEvent>,
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) {
        thread::spawn(move || {
            let mut retry = Duration::from_secs(1);
            while !cancelled.load(std::sync::atomic::Ordering::Acquire) && !event_tx.is_closed() {
                if let Err(err) = run_mirror(
                    transport.clone(),
                    local_pane_id,
                    event_tx.clone(),
                    &cancelled,
                ) {
                    if cancelled.load(std::sync::atomic::Ordering::Acquire) || event_tx.is_closed()
                    {
                        break;
                    }
                    tracing::warn!(err = %err, retry_seconds = retry.as_secs(), "remote SSH agent mirror disconnected; retrying");
                }
                let deadline = std::time::Instant::now() + retry;
                while std::time::Instant::now() < deadline {
                    if cancelled.load(std::sync::atomic::Ordering::Acquire) || event_tx.is_closed()
                    {
                        return;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
                retry = (retry * 2).min(Duration::from_secs(10));
            }
        });
    }

    fn run_mirror(
        transport: AgentTransportInfo,
        local_pane_id: PaneId,
        event_tx: tokio::sync::mpsc::Sender<AppEvent>,
        cancelled: &std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> io::Result<()> {
        let AgentTransportInfo::Ssh {
            target,
            ssh_args,
            managed_control_path,
            session,
            remote_pane_id,
            remote_agent,
            ..
        } = transport;
        let connection = SshConnection {
            target,
            ssh_args,
            managed_control_path,
            session,
        };
        if let Ok(info) = remote_agent_get(&connection, &remote_pane_id) {
            send_info_events(&event_tx, local_pane_id, &info, remote_agent.as_deref());
        }

        let request = serde_json::json!({
            "id": "remote-agent:subscribe",
            "method": "events.subscribe",
            "params": {
                "subscriptions": [
                    {
                        "type": "pane.agent_status_changed",
                        "pane_id": remote_pane_id
                    }
                ]
            }
        });
        let mut child = ssh_api_bridge_command(&connection)?
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| io::Error::new(err.kind(), format!("failed to start ssh: {err}")))?;
        {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "ssh api bridge stdin missing")
            })?;
            writeln!(stdin, "{request}")?;
        }
        let stdout = child.stdout.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::BrokenPipe, "ssh api bridge stdout missing")
        })?;
        if let Some(stderr) = child.stderr.take() {
            thread::spawn(move || {
                let _ = io::copy(&mut BufReader::new(stderr), &mut io::sink());
            });
        }
        let (line_tx, line_rx) = std::sync::mpsc::sync_channel(MIRROR_EVENT_QUEUE_CAPACITY);
        let reader_cancelled = std::sync::Arc::clone(cancelled);
        let stdout_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                match read_bounded_line(&mut reader, MAX_MIRROR_EVENT_LINE_BYTES) {
                    Ok(Some(line)) => {
                        if !send_mirror_line(&line_tx, Ok(line), &reader_cancelled) {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let _ = send_mirror_line(&line_tx, Err(err), &reader_cancelled);
                        break;
                    }
                }
            }
        });
        let mut last_info_refresh = None;
        loop {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) || event_tx.is_closed() {
                let _ = child.kill();
                let _ = child.wait();
                drop(line_rx);
                let _ = stdout_thread.join();
                return Ok(());
            }
            match line_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(Ok(line)) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    if is_subscription_started(&line) {
                        if should_refresh_mirror_info(last_info_refresh) {
                            if let Ok(info) = remote_agent_get(&connection, &remote_pane_id) {
                                send_info_events(
                                    &event_tx,
                                    local_pane_id,
                                    &info,
                                    remote_agent.as_deref(),
                                );
                            }
                            last_info_refresh = Some(std::time::Instant::now());
                        }
                        continue;
                    }
                    if let Some(event) = parse_status_event(&line) {
                        send_status_event(&event_tx, local_pane_id, event, remote_agent.as_deref());
                        if should_refresh_mirror_info(last_info_refresh) {
                            if let Ok(info) = remote_agent_get(&connection, &remote_pane_id) {
                                send_info_events(
                                    &event_tx,
                                    local_pane_id,
                                    &info,
                                    remote_agent.as_deref(),
                                );
                            }
                            last_info_refresh = Some(std::time::Instant::now());
                        }
                    }
                }
                Ok(Err(err)) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    drop(line_rx);
                    let _ = stdout_thread.join();
                    return Err(err);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if child.try_wait()?.is_some() {
                        break;
                    }
                    if should_refresh_mirror_info(last_info_refresh) {
                        if let Ok(info) = remote_agent_get(&connection, &remote_pane_id) {
                            send_info_events(
                                &event_tx,
                                local_pane_id,
                                &info,
                                remote_agent.as_deref(),
                            );
                        }
                        last_info_refresh = Some(std::time::Instant::now());
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        let status = child.wait()?;
        drop(line_rx);
        let _ = stdout_thread.join();
        if status.success() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "remote api bridge exited with {status}"
            )))
        }
    }

    pub(super) fn send_mirror_line(
        sender: &std::sync::mpsc::SyncSender<io::Result<String>>,
        mut value: io::Result<String>,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> bool {
        loop {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                return false;
            }
            match sender.try_send(value) {
                Ok(()) => return true,
                Err(std::sync::mpsc::TrySendError::Full(returned)) => {
                    value = returned;
                    thread::sleep(Duration::from_millis(10));
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return false,
            }
        }
    }

    fn should_refresh_mirror_info(last_refresh: Option<std::time::Instant>) -> bool {
        last_refresh.is_none_or(|last| last.elapsed() >= MIRROR_INFO_REFRESH_INTERVAL)
    }

    pub(super) fn is_subscription_started(line: &str) -> bool {
        serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|value| value["result"]["type"].as_str().map(str::to_owned))
            .as_deref()
            == Some("subscription_started")
    }

    pub(super) fn read_bounded_line(
        reader: &mut impl BufRead,
        max_bytes: usize,
    ) -> io::Result<Option<String>> {
        let mut line = Vec::new();
        loop {
            let available = reader.fill_buf()?;
            if available.is_empty() {
                if line.is_empty() {
                    return Ok(None);
                }
                break;
            }
            let take = available
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(available.len(), |index| index + 1);
            if line.len().saturating_add(take) > max_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "remote mirror event line is too large",
                ));
            }
            line.extend_from_slice(&available[..take]);
            reader.consume(take);
            if line.last() == Some(&b'\n') {
                break;
            }
        }
        String::from_utf8(line).map(Some).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("remote mirror event is not UTF-8: {err}"),
            )
        })
    }

    fn send_info_events(
        event_tx: &tokio::sync::mpsc::Sender<AppEvent>,
        local_pane_id: PaneId,
        info: &AgentInfo,
        fallback_agent: Option<&str>,
    ) {
        let _ = event_tx.blocking_send(AppEvent::RemoteAgentInfoMirrored {
            pane_id: local_pane_id,
            remote_cwd: info.foreground_cwd.clone().or_else(|| info.cwd.clone()),
        });
        for event in super::events_from_agent_info(local_pane_id, info, fallback_agent) {
            let _ = event_tx.blocking_send(event);
        }
    }

    fn send_status_event(
        event_tx: &tokio::sync::mpsc::Sender<AppEvent>,
        local_pane_id: PaneId,
        event: RemoteStatusEvent,
        fallback_agent: Option<&str>,
    ) {
        let agent_label = super::normalize_remote_agent_label([
            event.agent.as_deref(),
            event.display_agent.as_deref(),
            fallback_agent,
        ]);
        let title = super::normalize_remote_presentation_text(event.title.as_deref(), 80);
        let display_agent =
            super::normalize_remote_presentation_text(event.display_agent.as_deref(), 80);
        let custom_status =
            super::normalize_remote_presentation_text(event.custom_status.as_deref(), 32);
        let state_labels = super::normalize_remote_state_labels(&event.state_labels);
        let _ = event_tx.blocking_send(AppEvent::HookStateReported {
            pane_id: local_pane_id,
            source: super::REMOTE_MIRROR_STATE_SOURCE.to_string(),
            agent_label: agent_label.clone(),
            state: super::status_to_state(event.agent_status),
            message: None,
            custom_status: custom_status.clone(),
            seq: None,
            session_ref: None,
        });
        let _ = event_tx.blocking_send(AppEvent::HookMetadataReported {
            pane_id: local_pane_id,
            source: super::REMOTE_MIRROR_METADATA_SOURCE.to_string(),
            agent_label: Some(agent_label),
            applies_to_source: Some(super::REMOTE_MIRROR_STATE_SOURCE.to_string()),
            title,
            display_agent,
            custom_status,
            state_labels,
            clear_title: false,
            clear_display_agent: false,
            clear_custom_status: false,
            clear_state_labels: false,
            seq: None,
            ttl: None,
        });
    }

    fn validate_ssh_target(target: &str) -> io::Result<()> {
        if target.is_empty() || target.starts_with('-') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "--ssh target must be non-empty and must not start with '-'",
            ));
        }
        Ok(())
    }

    pub(super) fn explicit_remote_session(session: Option<&str>) -> io::Result<String> {
        let session = session
            .filter(|session| !session.is_empty())
            .unwrap_or(crate::session::DEFAULT_SESSION_NAME);
        crate::session::validate_name(session)
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
        Ok(session.to_string())
    }

    fn validate_effective_ssh_config(connection: &SshConnection) -> io::Result<()> {
        let args = validated_interactive_ssh_args(connection)?;
        match crate::ssh_integration::preflight_interactive_ssh_args(&args) {
            Ok(true) => Ok(()),
            Ok(false) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Herdr cannot federate this SSH target because its effective SSH configuration changes interactive session semantics (for example RemoteCommand, LocalCommand, forwarding, StdinNull, or SessionType); remove or override that option for this host",
            )),
            Err(err) => Err(io::Error::new(
                err.kind(),
                format!("could not validate effective SSH configuration for Herdr federation: {err}"),
            )),
        }
    }

    pub(super) fn validated_interactive_ssh_args(
        connection: &SshConnection,
    ) -> io::Result<Vec<String>> {
        let mut args = connection.ssh_args.clone();
        args.push(connection.target.clone());
        let parsed = crate::ssh_integration::parse_interactive_ssh_args(&args).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "SSH federation accepts only destination connection options; remote commands, forwarding, user-managed control sockets, and additional destinations are not allowed",
            )
        })?;
        if parsed.target != connection.target || parsed.ssh_args != connection.ssh_args {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SSH federation arguments did not resolve to the declared destination and connection options",
            ));
        }
        Ok(args)
    }

    fn validate_managed_control_connection(connection: &SshConnection) -> io::Result<()> {
        if let Some(control_path) = connection.managed_control_path.as_deref() {
            crate::ssh_integration::validate_managed_control_path(std::path::Path::new(
                control_path,
            ))?;
        }
        Ok(())
    }

    impl PeerBridgeRuntime {
        fn start_supervisor(&self) -> io::Result<()> {
            let registration = self.registration();
            let child = std::sync::Arc::clone(&self.child);
            let cancelled = std::sync::Arc::clone(&self.supervisor_cancelled);
            let healthy = std::sync::Arc::clone(&self.healthy);
            let spawn = thread::Builder::new()
                .name("herdr-peer-supervisor".into())
                .spawn(move || {
                    while !cancelled.load(std::sync::atomic::Ordering::Acquire) {
                        let deadline = std::time::Instant::now() + Duration::from_secs(5);
                        while std::time::Instant::now() < deadline {
                            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                                return;
                            }
                            thread::sleep(Duration::from_millis(100));
                        }

                        let process_alive = bridge_transport_is_alive(&registration, &child);
                        let reverse_healthy = process_alive
                            && verify_reverse_peer_health(
                                &registration.connection,
                                &registration.remote_peer_id,
                            )
                            .is_ok();
                        if reverse_healthy {
                            healthy.store(true, std::sync::atomic::Ordering::Release);
                            continue;
                        }
                        healthy.store(false, std::sync::atomic::Ordering::Release);
                        if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                            return;
                        }
                        match repair_reverse_peer_bridge(&registration, &child, &cancelled) {
                            Ok(()) => {
                                healthy.store(true, std::sync::atomic::Ordering::Release);
                            }
                            Err(err) => {
                                tracing::debug!(%err, peer = %registration.remote_peer_id, "could not repair reverse peer bridge");
                            }
                        }
                    }
                });
            spawn.map(|_| ()).map_err(|err| {
                self.healthy
                    .store(false, std::sync::atomic::Ordering::Release);
                io::Error::new(
                    err.kind(),
                    format!("could not start reverse peer bridge supervisor: {err}"),
                )
            })
        }

        pub(super) fn shutdown(&mut self, unregister_remote: bool) {
            if self.stopped {
                return;
            }
            self.stopped = true;
            self.healthy
                .store(false, std::sync::atomic::Ordering::Release);
            self.supervisor_cancelled
                .store(true, std::sync::atomic::Ordering::Release);

            let mut child = self
                .child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(child) = child.as_mut() {
                // Revoke an unmanaged reverse forward before any remote cleanup can block.
                kill_ssh_process_group(child);
                let _ = child.kill();
            }
            let cancel_child =
                start_managed_reverse_forward_cancel(&self.connection, &self.remote_api_socket);
            let registration = self.registration();
            let connection = self.connection.clone();
            let remote_api_socket = self.remote_api_socket.clone();
            let cleanup_guard = PendingPeerCleanupGuard::new();
            thread::spawn(move || {
                let _cleanup_guard = cleanup_guard;
                if let Some(cancel_child) = cancel_child {
                    let _ = wait_with_timeout(cancel_child, Duration::from_secs(5));
                }
                if let Some(child) = child {
                    terminate_ssh_process(child);
                }

                if unregister_remote {
                    if let Err(err) = registration.unregister_if_current() {
                        tracing::debug!(%err, peer = %registration.remote_peer_id, "could not unregister remote peer");
                    }
                }
                if let Err(err) = cleanup_remote_bridge_socket(&connection, &remote_api_socket) {
                    tracing::debug!(%err, socket = %remote_api_socket, "could not clean remote peer socket");
                }
                release_and_close_managed_control_path(&connection);
            });
        }
    }

    fn bridge_transport_is_alive(
        registration: &PeerBridgeRegistration,
        child: &std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
    ) -> bool {
        if let Some(control_path) = registration.connection.managed_control_path.as_deref() {
            return crate::ssh_integration::managed_control_connection_is_alive(
                &registration.connection.target,
                control_path,
            );
        }
        let mut child = child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        child
            .as_mut()
            .is_some_and(|child| child.try_wait().is_ok_and(|status| status.is_none()))
    }

    fn repair_reverse_peer_bridge(
        registration: &PeerBridgeRegistration,
        child: &std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
        cancelled: &std::sync::atomic::AtomicBool,
    ) -> io::Result<()> {
        let ensure_active = || {
            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "peer bridge repair was cancelled",
                ))
            } else {
                Ok(())
            }
        };
        ensure_active()?;
        if registration.connection.managed_control_path.is_some() {
            if let Some(cancel_child) = start_managed_reverse_forward_cancel(
                &registration.connection,
                &registration.remote_api_socket,
            ) {
                let _ = wait_with_timeout(cancel_child, Duration::from_secs(5));
            }
            cleanup_remote_bridge_socket(
                &registration.connection,
                &registration.remote_api_socket,
            )?;
            ensure_active()?;
            start_managed_reverse_forward(
                &registration.connection,
                &registration.remote_api_socket,
            )?;
        } else {
            let previous = child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(previous) = previous {
                terminate_ssh_process(previous);
            }
            cleanup_remote_bridge_socket(
                &registration.connection,
                &registration.remote_api_socket,
            )?;
            ensure_active()?;
            let replacement = spawn_unmanaged_reverse_forward(
                &registration.connection,
                &registration.remote_api_socket,
            )?;
            let mut child = child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if cancelled.load(std::sync::atomic::Ordering::Acquire) {
                terminate_ssh_process(replacement);
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "peer bridge repair was cancelled",
                ));
            }
            *child = Some(replacement);
        }
        ensure_active()?;
        match registration.register() {
            Ok(()) => {
                drain_unmanaged_bridge_stderr(child);
                Ok(())
            }
            Err(err) => Err(unmanaged_bridge_failure(child, err)),
        }
    }

    fn start_managed_reverse_forward_cancel(
        connection: &SshConnection,
        remote_api_socket: &str,
    ) -> Option<std::process::Child> {
        let control_path = connection.managed_control_path.as_deref()?;
        let mut command = managed_control_command(control_path).ok()?;
        let forward = format!(
            "{}:{}",
            remote_api_socket,
            crate::api::peer_socket_path().display()
        );
        command
            .arg("-O")
            .arg("cancel")
            .arg("-R")
            .arg(forward)
            .arg(&connection.target)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()
    }

    fn close_managed_control_connection(connection: &SshConnection) {
        let Some(control_path) = connection.managed_control_path.as_deref() else {
            return;
        };
        let validated = crate::ssh_integration::validate_managed_control_path(
            std::path::Path::new(control_path),
        );
        if let Ok(mut command) = managed_control_command(control_path) {
            command
                .arg("-O")
                .arg("exit")
                .arg(&connection.target)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let _ = command_output_with_timeout(command, Duration::from_secs(5));
        }
        if let Ok(validated) = validated {
            if let Err(err) = crate::ssh_integration::cleanup_managed_control_path(validated) {
                tracing::debug!(%err, "could not clean managed SSH control path");
            }
        }
    }

    fn reverse_forward_spec(remote_api_socket: &str) -> String {
        format!(
            "{}:{}",
            remote_api_socket,
            crate::api::peer_socket_path().display()
        )
    }

    fn start_managed_reverse_forward(
        connection: &SshConnection,
        remote_api_socket: &str,
    ) -> io::Result<()> {
        let control_path = connection.managed_control_path.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed SSH control path is missing",
            )
        })?;
        let mut command = managed_control_command(control_path)?;
        command
            .arg("-O")
            .arg("forward")
            .arg("-R")
            .arg(reverse_forward_spec(remote_api_socket))
            .arg(&connection.target);
        let output = command_output_with_timeout(command, SSH_API_TIMEOUT)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(remote_forwarding_failed(output))
        }
    }

    fn spawn_unmanaged_reverse_forward(
        connection: &SshConnection,
        remote_api_socket: &str,
    ) -> io::Result<std::process::Child> {
        let mut command = ssh_program_command()?;
        apply_noninteractive_ssh_options(&mut command);
        command
            .arg("-N")
            .arg("-o")
            .arg("ExitOnForwardFailure=yes")
            .arg("-o")
            .arg("StreamLocalBindUnlink=yes")
            .arg("-o")
            .arg("StreamLocalBindMask=0177")
            .arg("-R")
            .arg(reverse_forward_spec(remote_api_socket))
            .args(&connection.ssh_args)
            .arg(&connection.target)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|err| {
            io::Error::new(err.kind(), format!("failed to start ssh bridge: {err}"))
        })?;
        thread::sleep(Duration::from_millis(250));
        if child.try_wait()?.is_some() {
            let output = kill_and_collect_ssh_process(child, SSH_OUTPUT_DRAIN_TIMEOUT)?;
            return Err(remote_forwarding_failed(output));
        }
        Ok(child)
    }

    fn drain_unmanaged_bridge_stderr(
        child: &std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
    ) {
        let stderr = child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
            .and_then(|child| child.stderr.take());
        if let Some(stderr) = stderr {
            thread::spawn(move || {
                let _ = io::copy(&mut BufReader::new(stderr), &mut io::sink());
            });
        }
    }

    fn unmanaged_bridge_failure(
        child: &std::sync::Arc<std::sync::Mutex<Option<std::process::Child>>>,
        fallback: io::Error,
    ) -> io::Error {
        let deadline = std::time::Instant::now() + UNMANAGED_FORWARD_FAILURE_GRACE;
        let exited = loop {
            let status = child
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_mut()
                .map(std::process::Child::try_wait);
            match status {
                Some(Ok(Some(_))) => break true,
                Some(Ok(None)) if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(25));
                }
                _ => break false,
            }
        };
        if !exited {
            return fallback;
        }
        let child = child
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match child
            .and_then(|child| kill_and_collect_ssh_process(child, SSH_OUTPUT_DRAIN_TIMEOUT).ok())
        {
            Some(output) => remote_forwarding_failed(output),
            None => fallback,
        }
    }

    fn start_reverse_peer_bridge(
        connection: &SshConnection,
    ) -> io::Result<(PeerInfo, PeerBridgeRuntime)> {
        retain_managed_control_path(connection);
        let result = start_reverse_peer_bridge_with_reserved_control_path(connection);
        if result.is_err() {
            release_and_close_managed_control_path(connection);
        }
        result
    }

    fn start_reverse_peer_bridge_with_reserved_control_path(
        connection: &SshConnection,
    ) -> io::Result<(PeerInfo, PeerBridgeRuntime)> {
        let peer_id = peer_id_for_ssh(connection);
        let remote_peer_id = local_peer_id();
        let registration_lock = peer_registration_lock(&remote_peer_id);
        let suffix = peer_bridge_suffix(connection);
        let remote_api_socket = format!("/tmp/herdr-peer-{suffix}-api.sock");
        cleanup_remote_bridge_socket(connection, &remote_api_socket)?;

        let child = if let Some(control_path) = connection.managed_control_path.as_deref() {
            let _ = control_path;
            start_managed_reverse_forward(connection, &remote_api_socket)?;
            None
        } else {
            Some(spawn_unmanaged_reverse_forward(
                connection,
                &remote_api_socket,
            )?)
        };

        let peer = PeerInfo {
            id: peer_id,
            label: connection.target.to_string(),
            status: PeerStatus::Connected,
            transport: PeerTransportInfo::Ssh {
                target: connection.target.to_string(),
                ssh_args: connection.ssh_args.clone(),
                managed_control_path: connection.managed_control_path.clone(),
                session: connection.session.clone(),
            },
        };
        let bridge = PeerBridgeRuntime {
            connection_id: suffix,
            child: std::sync::Arc::new(std::sync::Mutex::new(child)),
            remote_api_socket,
            connection: connection.clone(),
            remote_peer_id,
            stopped: false,
            supervisor_cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            healthy: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            registration_lock,
            #[cfg(test)]
            test_noop_registration: false,
        };
        Ok((peer, bridge))
    }

    pub(super) fn retain_managed_control_path(connection: &SshConnection) {
        let Some(control_path) = connection.managed_control_path.as_deref() else {
            return;
        };
        let lease = {
            let mut leases = managed_control_path_leases()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            leases
                .entry(control_path.to_string())
                .or_insert_with(|| std::sync::Arc::new(std::sync::Mutex::new(0)))
                .clone()
        };
        let mut count = lease
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *count += 1;
    }

    fn release_and_close_managed_control_path(connection: &SshConnection) {
        release_managed_control_path_with(connection, close_managed_control_connection);
    }

    pub(super) fn release_managed_control_path_with(
        connection: &SshConnection,
        on_last_release: impl FnOnce(&SshConnection),
    ) -> bool {
        let Some(control_path) = connection.managed_control_path.as_deref() else {
            return false;
        };
        let lease = {
            let leases = managed_control_path_leases()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            leases.get(control_path).cloned()
        };
        let Some(lease) = lease else {
            tracing::debug!(control_path, "managed SSH control path had no active lease");
            return false;
        };
        let mut count = lease
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *count > 1 {
            *count -= 1;
            return false;
        }
        *count = 0;

        // Keep this path's lease locked while closing. Retainers for this path wait,
        // while unrelated control paths remain independent.
        on_last_release(connection);

        let mut leases = managed_control_path_leases()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let is_current = leases
            .get(control_path)
            .is_some_and(|current| std::sync::Arc::ptr_eq(current, &lease));
        if is_current && std::sync::Arc::strong_count(&lease) == 2 {
            leases.remove(control_path);
        }
        drop(leases);
        drop(count);
        true
    }

    fn managed_control_path_leases() -> &'static std::sync::Mutex<
        std::collections::HashMap<String, std::sync::Arc<std::sync::Mutex<usize>>>,
    > {
        use std::sync::{Arc, Mutex, OnceLock};

        static LEASES: OnceLock<Mutex<std::collections::HashMap<String, Arc<Mutex<usize>>>>> =
            OnceLock::new();
        LEASES.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
    }

    fn peer_registration_lock(peer_id: &str) -> std::sync::Arc<std::sync::Mutex<()>> {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex, OnceLock, Weak};

        static LOCKS: OnceLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();
        let mut locks = LOCKS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(lock) = locks.get(peer_id).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(peer_id.to_string(), Arc::downgrade(&lock));
        lock
    }

    fn remote_forwarding_failed(output: std::process::Output) -> io::Error {
        let source = command_failed("remote peer bridge failed", output);
        io::Error::other(format!(
            "{source}. Herdr federation requires SSH remote Unix-socket forwarding; check the SSH server and authorized-key forwarding restrictions"
        ))
    }

    pub(super) fn peer_info_for_bridge(bridge: &PeerBridgeRuntime) -> PeerInfo {
        PeerInfo {
            id: peer_id_for_ssh(&bridge.connection),
            label: bridge.connection.target.clone(),
            status: PeerStatus::Connected,
            transport: PeerTransportInfo::Ssh {
                target: bridge.connection.target.clone(),
                ssh_args: bridge.connection.ssh_args.clone(),
                managed_control_path: bridge.connection.managed_control_path.clone(),
                session: bridge.connection.session.clone(),
            },
        }
    }

    pub(super) fn register_local_peer_on_remote(
        connection: &SshConnection,
        remote_peer_id: &str,
        remote_api_socket: &str,
    ) -> io::Result<()> {
        let request = Request {
            id: "remote-agent:peer-register".into(),
            method: crate::api::schema::Method::PeerRegister(PeerRegisterParams {
                peer: PeerInfo {
                    id: remote_peer_id.to_string(),
                    label: local_peer_label(),
                    status: PeerStatus::Connected,
                    transport: PeerTransportInfo::ApiSocket {
                        api_socket: remote_api_socket.to_string(),
                    },
                },
            }),
        };
        let response = api_request(
            &connection.target,
            &connection.ssh_args,
            connection.managed_control_path.as_deref(),
            connection.session.as_deref(),
            &request,
        )?;
        if response.get("error").is_some() {
            return Err(io::Error::other(
                response["error"]["message"]
                    .as_str()
                    .unwrap_or("remote peer registration failed")
                    .to_string(),
            ));
        }
        verify_reverse_peer_health(connection, remote_peer_id)
    }

    pub(super) fn unregister_local_peer_on_remote_if_current(
        connection: &SshConnection,
        remote_peer_id: &str,
        remote_api_socket: &str,
    ) -> io::Result<()> {
        let peers = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:peer-list-before-unregister",
                "method": "peer.list",
                "params": {},
            }),
        )?;
        if !peer_registration_targets_socket(&peers, remote_peer_id, remote_api_socket)? {
            return Ok(());
        }
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:peer-unregister",
                "method": "peer.unregister",
                "params": { "peer_id": remote_peer_id },
            }),
        )?;
        if let Some(message) = response["error"]["message"].as_str() {
            if response["error"]["code"].as_str() == Some("peer_not_found") {
                return Ok(());
            }
            return Err(io::Error::other(message.to_string()));
        }
        Ok(())
    }

    pub(super) fn peer_registration_targets_socket(
        response: &serde_json::Value,
        peer_id: &str,
        expected_socket: &str,
    ) -> io::Result<bool> {
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(message.to_string()));
        }
        let success: crate::api::schema::SuccessResponse = serde_json::from_value(response.clone())
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("remote peer.list returned an invalid response: {err}"),
                )
            })?;
        let crate::api::schema::ResponseResult::PeerList { peers } = success.result else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "remote peer.list returned the wrong response type",
            ));
        };
        Ok(peers.into_iter().any(|peer| {
            peer.id == peer_id
                && matches!(
                    peer.transport,
                    PeerTransportInfo::ApiSocket { api_socket }
                        if api_socket == expected_socket
                )
        }))
    }

    fn cleanup_remote_bridge_socket(
        connection: &SshConnection,
        remote_api_socket: &str,
    ) -> io::Result<()> {
        let command = format!("rm -f {}", shell_quote(remote_api_socket));
        let mut ssh = ssh_command(connection)?;
        ssh.arg(command);
        let output = command_output_with_timeout(ssh, SSH_API_TIMEOUT)?;
        if output.status.success() {
            Ok(())
        } else {
            Err(command_failed(
                "failed to clean remote peer sockets",
                output,
            ))
        }
    }

    pub(super) fn peer_id_for_ssh(connection: &SshConnection) -> String {
        let session = connection
            .session
            .as_deref()
            .filter(|session| {
                !session.is_empty() && *session != crate::session::DEFAULT_SESSION_NAME
            })
            .unwrap_or("");
        let sanitized: String = connection
            .target
            .chars()
            .map(|ch| {
                if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                    ch
                } else {
                    '-'
                }
            })
            .collect();
        let sanitized = sanitized.trim_matches('-');
        let base = if sanitized.is_empty() {
            "remote"
        } else {
            sanitized
        };
        let base = base.chars().take(40).collect::<String>();
        let target_is_lossless = base == connection.target && connection.target.len() <= 40;
        if target_is_lossless && connection.ssh_args.is_empty() && session.is_empty() {
            base
        } else {
            format!(
                "{base}-{}",
                short_hash(&format!(
                    "{}\0{}\0{}",
                    connection.target,
                    connection.ssh_args.join("\0"),
                    session
                ))
            )
        }
    }

    fn peer_bridge_suffix(connection: &SshConnection) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT_BRIDGE: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT_BRIDGE.fetch_add(1, Ordering::Relaxed);
        short_hash(&format!(
            "{}\0{}\0{}\0{}\0{}\0{}\0{}",
            local_peer_id(),
            crate::api::socket_path().display(),
            connection.target,
            connection.ssh_args.join("\0"),
            connection.session.as_deref().unwrap_or(""),
            std::process::id(),
            nonce,
        ))
    }

    fn short_hash(value: &str) -> String {
        use sha2::{Digest as _, Sha256};
        let digest = Sha256::digest(value.as_bytes());
        digest[..8]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    pub(super) fn local_peer_id() -> String {
        let machine = std::fs::read_to_string("/etc/machine-id")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| "unknown-host".to_string());
        format!(
            "origin-{}",
            short_hash(&format!(
                "{}\0{}\0{}",
                machine,
                std::env::var("HOME").unwrap_or_default(),
                crate::api::socket_path().display()
            ))
        )
    }

    fn local_peer_label() -> String {
        let host = std::env::var("HOSTNAME")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "origin".to_string());
        match crate::session::active_name() {
            Some(session) => format!("{host}:{session}"),
            None => host,
        }
    }

    fn ensure_remote_herdr(connection: &SshConnection) -> io::Result<()> {
        let mut command = ssh_command(connection)?;
        command.arg(remote_server_ensure_command(connection.session.as_deref()));
        let output = command_output_with_timeout(command, Duration::from_secs(30))?;
        if output.status.success() {
            return Ok(());
        }

        let details = output_details(&output);
        if output.status.code() == Some(127) {
            return Err(io::Error::other(format!(
                "remote Herdr is not installed on {}. Install Herdr on that machine (run `./install-local.sh` from a source checkout, or use a supported package), then retry.{}",
                connection.target,
                detail_suffix(&details)
            )));
        }
        Err(io::Error::other(format!(
            "could not start the remote Herdr server on {} ({}). This can be an SSH authentication, network, or incompatible-Herdr error.{}",
            connection.target,
            output.status,
            detail_suffix(&details)
        )))
    }

    fn verify_remote_peer_capability(connection: &SshConnection) -> io::Result<()> {
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:capabilities",
                "method": "ping",
                "params": {},
            }),
        )
        .map_err(|err| {
            io::Error::other(format!(
                "remote Herdr on {} does not support peer federation; install the same current Herdr build on both machines: {err}",
                connection.target
            ))
        })?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(format!(
                "remote Herdr peer capability check failed: {message}"
            )));
        }
        let remote_protocol = response["result"]["protocol"].as_u64();
        if remote_protocol != Some(u64::from(crate::protocol::PROTOCOL_VERSION)) {
            return Err(io::Error::other(format!(
                "remote Herdr protocol {} is incompatible with local protocol {}; update Herdr on both machines",
                remote_protocol
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                crate::protocol::PROTOCOL_VERSION
            )));
        }
        if response["result"]["capabilities"]["peer_federation"].as_bool() != Some(true) {
            return Err(io::Error::other(format!(
                "remote Herdr on {} does not advertise peer federation; install the same current Herdr build on both machines",
                connection.target
            )));
        }
        if response["result"]["capabilities"]["remote_presentation"].as_bool() != Some(true) {
            return Err(io::Error::other(format!(
                "remote Herdr on {} does not support exclusive remote panes; install the same current Herdr build on both machines",
                connection.target
            )));
        }
        Ok(())
    }

    fn verify_reverse_peer_health(
        connection: &SshConnection,
        remote_peer_id: &str,
    ) -> io::Result<()> {
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:peer-health",
                "method": "peer.health",
                "params": { "peer_id": remote_peer_id },
            }),
        )?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(format!(
                "reverse peer bridge health check failed: {message}"
            )));
        }
        if response["result"]["peer"]["status"].as_str() != Some("connected") {
            return Err(io::Error::other(
                "reverse peer bridge was created but did not become reachable",
            ));
        }
        Ok(())
    }

    fn install_remote_integration(connection: &SshConnection, agent_label: &str) -> io::Result<()> {
        let Some(integration_target) = integration_target_for_label(agent_label) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("agent {agent_label} does not have a Herdr integration installer"),
            ));
        };
        let integration = crate::integration::integration_target_label(integration_target);
        let mut command = ssh_command(connection)?;
        command.arg(remote_integration_install_command(
            connection.session.as_deref(),
            integration,
        ));
        let output = command_output_with_timeout(command, Duration::from_secs(30))?;
        if output.status.success() {
            return Ok(());
        }

        Err(command_failed(
            &format!(
                "remote integration install for {integration} failed on {}",
                connection.target
            ),
            output,
        ))
    }

    fn start_remote_agent(
        connection: &SshConnection,
        params: &AgentStartParams,
    ) -> io::Result<AgentInfo> {
        let (tab_id, root_pane_id) = create_remote_agent_tab(connection, params)?;
        let mut remote_params = params.clone();
        remote_params.peer = None;
        remote_params.transport = None;
        remote_params.workspace_id = None;
        remote_params.tab_id = Some(tab_id.clone());
        remote_params.split = Some(crate::api::schema::SplitDirection::Right);
        remote_params.focus = false;
        let response = match remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:start",
                "method": "agent.start",
                "params": remote_params,
            }),
        ) {
            Ok(response) => response,
            Err(err) => {
                let _ = close_remote_tab(connection, &tab_id);
                return Err(err);
            }
        };
        if response.get("error").is_some() {
            let err = io::Error::other(
                response["error"]["message"]
                    .as_str()
                    .unwrap_or("remote agent start failed")
                    .to_string(),
            );
            let _ = close_remote_tab(connection, &tab_id);
            return Err(err);
        }
        let agent: AgentInfo = serde_json::from_value(response["result"]["agent"].clone())
            .map_err(|err| {
                let _ = close_remote_tab(connection, &tab_id);
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("remote agent.start returned invalid agent info: {err}"),
                )
            })?;
        if agent.tab_id != tab_id {
            let _ = close_remote_tab(connection, &tab_id);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "remote agent.start placed the agent outside its dedicated tab",
            ));
        }
        if let Err(err) = close_remote_pane(connection, &root_pane_id) {
            tracing::debug!(%err, pane = %root_pane_id, "could not remove remote agent tab bootstrap shell");
        }
        Ok(agent)
    }

    fn create_remote_agent_tab(
        connection: &SshConnection,
        params: &AgentStartParams,
    ) -> io::Result<(String, String)> {
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:tab-create",
                "method": "tab.create",
                "params": {
                    "cwd": params.cwd,
                    "focus": false,
                    "label": params.name,
                    "env": {}
                }
            }),
        )?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(message.to_string()));
        }
        let tab_id = response["result"]["tab"]["tab_id"]
            .as_str()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "remote tab.create response did not include tab_id",
                )
            })?;
        let pane_id = response["result"]["root_pane"]["pane_id"]
            .as_str()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "remote tab.create response did not include root pane_id",
                )
            })?;
        Ok((tab_id.to_string(), pane_id.to_string()))
    }

    fn close_remote_tab(connection: &SshConnection, tab_id: &str) -> io::Result<()> {
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:rollback",
                "method": "tab.close",
                "params": { "tab_id": tab_id },
            }),
        )?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(message.to_string()));
        }
        Ok(())
    }

    fn close_remote_pane(connection: &SshConnection, pane_id: &str) -> io::Result<()> {
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:bootstrap-close",
                "method": "pane.close",
                "params": { "pane_id": pane_id },
            }),
        )?;
        if let Some(message) = response["error"]["message"].as_str() {
            return Err(io::Error::other(message.to_string()));
        }
        Ok(())
    }

    fn remote_agent_get(connection: &SshConnection, pane_id: &str) -> io::Result<AgentInfo> {
        let response = remote_api_request(
            connection,
            serde_json::json!({
                "id": "remote-agent:get",
                "method": "peer.agent.get",
                "params": { "target": pane_id },
            }),
        )?;
        if response.get("error").is_some() {
            return Err(io::Error::other(
                response["error"]["message"]
                    .as_str()
                    .unwrap_or("remote agent get failed")
                    .to_string(),
            ));
        }
        serde_json::from_value(response["result"]["agent"].clone()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("remote agent.get returned invalid agent info: {err}"),
            )
        })
    }

    fn remote_api_request(
        connection: &SshConnection,
        request: serde_json::Value,
    ) -> io::Result<serde_json::Value> {
        let expected_id = request
            .get("id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        let mut child = ssh_api_bridge_command(connection)?
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| io::Error::new(err.kind(), format!("failed to start ssh: {err}")))?;
        {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "ssh api bridge stdin missing")
            })?;
            writeln!(stdin, "{request}")?;
        }
        let output = wait_with_timeout(child, SSH_API_TIMEOUT)?;
        if !output.status.success() {
            return Err(command_failed("remote api request failed", output));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        stdout
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|value| {
                expected_id.as_deref().is_none_or(|id| {
                    value.get("id").and_then(serde_json::Value::as_str) == Some(id)
                })
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "remote api returned no matching JSON response{}",
                        if stdout.trim().is_empty() {
                            String::new()
                        } else {
                            format!(": {}", stdout.trim())
                        }
                    ),
                )
            })
    }

    fn wait_with_timeout(
        mut child: std::process::Child,
        timeout: Duration,
    ) -> io::Result<std::process::Output> {
        use std::os::unix::process::ExitStatusExt as _;

        let stdout = child.stdout.take().map(spawn_bounded_output_reader);
        let stderr = child.stderr.take().map(spawn_bounded_output_reader);
        let start = std::time::Instant::now();
        let mut timed_out = false;
        let status = 'wait: loop {
            if let Some(status) = child.try_wait()? {
                break Some(status);
            }
            if start.elapsed() >= timeout {
                timed_out = true;
                kill_ssh_process_group(&mut child);
                let _ = child.kill();
                let kill_deadline = std::time::Instant::now() + SSH_PROCESS_KILL_GRACE;
                loop {
                    match child.try_wait() {
                        Ok(Some(status)) => break 'wait Some(status),
                        Ok(None) if std::time::Instant::now() < kill_deadline => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Ok(None) => break 'wait None,
                        Err(err) => return Err(err),
                    }
                }
            }
            thread::sleep(Duration::from_millis(50));
        };
        let status = match status {
            Some(status) => status,
            None => {
                let _ = thread::Builder::new()
                    .name("herdr-ssh-reaper".into())
                    .spawn(move || {
                        kill_ssh_process_group(&mut child);
                        let _ = child.kill();
                        let _ = child.wait();
                    });
                std::process::ExitStatus::from_raw(libc::SIGKILL)
            }
        };
        let output = std::process::Output {
            status,
            stdout: stdout
                .and_then(|output| output.recv_timeout(SSH_OUTPUT_DRAIN_TIMEOUT).ok())
                .unwrap_or_default(),
            stderr: stderr
                .and_then(|output| output.recv_timeout(SSH_OUTPUT_DRAIN_TIMEOUT).ok())
                .unwrap_or_default(),
        };
        if timed_out {
            return Err(command_failed("ssh command timed out", output));
        }
        Ok(output)
    }

    pub(super) fn kill_and_collect_ssh_process(
        mut child: std::process::Child,
        timeout: Duration,
    ) -> io::Result<std::process::Output> {
        kill_ssh_process_group(&mut child);
        let _ = child.kill();
        wait_with_timeout(child, timeout)
    }

    fn terminate_ssh_process(child: std::process::Child) {
        let _ = kill_and_collect_ssh_process(child, SSH_PROCESS_KILL_GRACE);
    }

    fn spawn_bounded_output_reader(
        stream: impl std::io::Read + Send + 'static,
    ) -> std::sync::mpsc::Receiver<Vec<u8>> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        thread::spawn(move || {
            let _ = sender.send(read_bounded(stream));
        });
        receiver
    }

    fn kill_ssh_process_group(child: &mut std::process::Child) {
        if let Ok(process_group) = i32::try_from(child.id()) {
            // SAFETY: the SSH child is started as its own process-group leader.
            unsafe {
                libc::kill(-process_group, libc::SIGKILL);
            }
        }
    }

    fn read_bounded(mut stream: impl std::io::Read) -> Vec<u8> {
        let mut captured = Vec::new();
        let mut buffer = [0_u8; 8192];
        let mut truncated = false;
        while let Ok(read) = stream.read(&mut buffer) {
            if read == 0 {
                break;
            }
            let remaining = MAX_SSH_OUTPUT_BYTES.saturating_sub(captured.len());
            if remaining > 0 {
                captured.extend_from_slice(&buffer[..read.min(remaining)]);
            }
            truncated |= read > remaining;
        }
        if truncated {
            const MARKER: &[u8] = b"\n[remote SSH output truncated]\n";
            let keep = MAX_SSH_OUTPUT_BYTES.saturating_sub(MARKER.len());
            captured.truncate(keep);
            captured.extend_from_slice(MARKER);
        }
        captured
    }

    pub(super) fn command_output_with_timeout(
        mut command: Command,
        timeout: Duration,
    ) -> io::Result<std::process::Output> {
        use std::os::unix::process::CommandExt as _;

        command.process_group(0);
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        let child = command
            .spawn()
            .map_err(|err| io::Error::new(err.kind(), format!("failed to start ssh: {err}")))?;
        wait_with_timeout(child, timeout)
    }

    fn ssh_api_bridge_command(connection: &SshConnection) -> io::Result<Command> {
        let mut command = ssh_command(connection)?;
        command.arg(remote_api_bridge_command(connection.session.as_deref()));
        Ok(command)
    }

    fn ssh_program_command() -> io::Result<Command> {
        use std::os::unix::process::CommandExt as _;

        let mut command = Command::new(crate::ssh_integration::real_ssh_program_for_exec()?);
        command.process_group(0);
        command.env(crate::ssh_integration::SSH_BYPASS_ENV_VAR, "1");
        Ok(command)
    }

    fn apply_noninteractive_ssh_options(command: &mut Command) {
        command
            .arg("-T")
            .arg("-o")
            .arg("RemoteCommand=none")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-o")
            .arg("ConnectTimeout=10")
            .arg("-o")
            .arg("ConnectionAttempts=1")
            .arg("-o")
            .arg("ServerAliveInterval=15")
            .arg("-o")
            .arg("ServerAliveCountMax=2");
    }

    pub(super) fn ssh_command(connection: &SshConnection) -> io::Result<Command> {
        let mut command = ssh_program_command()?;
        apply_noninteractive_ssh_options(&mut command);
        command.args(&connection.ssh_args);
        if let Some(control_path) = connection.managed_control_path.as_deref() {
            crate::ssh_integration::validate_managed_control_path(std::path::Path::new(
                control_path,
            ))?;
            command
                .arg("-o")
                .arg("ControlMaster=auto")
                .arg("-S")
                .arg(control_path);
        }
        command.arg(&connection.target);
        Ok(command)
    }

    fn managed_control_command(control_path: &str) -> io::Result<Command> {
        use std::os::unix::process::CommandExt as _;

        crate::ssh_integration::validate_managed_control_path(std::path::Path::new(control_path))?;
        let mut command = Command::new(crate::ssh_integration::real_ssh_program_for_exec()?);
        command.process_group(0);
        command
            .env(crate::ssh_integration::SSH_BYPASS_ENV_VAR, "1")
            .arg("-o")
            .arg("BatchMode=yes")
            .arg("-S")
            .arg(control_path);
        Ok(command)
    }

    fn remote_api_bridge_command(session: Option<&str>) -> String {
        let mut command = remote_herdr_exec_prefix();
        if let Some(session) = session.filter(|session| !session.is_empty()) {
            command.push_str(" --session ");
            command.push_str(&shell_quote(session));
        }
        command.push_str(" api bridge --shell-context");
        command
    }

    fn remote_server_ensure_command(session: Option<&str>) -> String {
        let mut command = remote_herdr_exec_prefix();
        if let Some(session) = session.filter(|session| !session.is_empty()) {
            command.push_str(" --session ");
            command.push_str(&shell_quote(session));
        }
        command.push_str(" server ensure --json");
        command
    }

    fn remote_integration_install_command(session: Option<&str>, integration: &str) -> String {
        let mut command = remote_herdr_exec_prefix();
        if let Some(session) = session.filter(|session| !session.is_empty()) {
            command.push_str(" --session ");
            command.push_str(&shell_quote(session));
        }
        command.push_str(" integration install ");
        command.push_str(&shell_quote(integration));
        command
    }

    fn command_failed(context: &str, output: std::process::Output) -> io::Error {
        let details = output_details(&output);
        io::Error::other(if details.is_empty() {
            format!("{context}: exited with {}", output.status)
        } else {
            format!("{context}: exited with {}\n{details}", output.status)
        })
    }

    fn output_details(output: &std::process::Output) -> String {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        [stderr.trim(), stdout.trim()]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn detail_suffix(details: &str) -> String {
        if details.is_empty() {
            String::new()
        } else {
            format!("\n\nremote output:\n{details}")
        }
    }

    #[derive(Debug, Deserialize)]
    struct RemoteStatusEnvelope {
        event: String,
        data: RemoteStatusData,
    }

    #[derive(Debug, Deserialize)]
    struct RemoteStatusData {
        agent_status: crate::api::schema::AgentStatus,
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        display_agent: Option<String>,
        #[serde(default)]
        custom_status: Option<String>,
        #[serde(default)]
        state_labels: std::collections::HashMap<String, String>,
    }

    #[derive(Debug, Deserialize)]
    struct RemoteStatusEvent {
        agent_status: crate::api::schema::AgentStatus,
        #[serde(default)]
        agent: Option<String>,
        #[serde(default)]
        title: Option<String>,
        #[serde(default)]
        display_agent: Option<String>,
        #[serde(default)]
        custom_status: Option<String>,
        #[serde(default)]
        state_labels: std::collections::HashMap<String, String>,
    }

    fn parse_status_event(line: &str) -> Option<RemoteStatusEvent> {
        let envelope: RemoteStatusEnvelope = serde_json::from_str(line).ok()?;
        if envelope.event != "pane.agent_status_changed" {
            return None;
        }
        Some(RemoteStatusEvent {
            agent_status: envelope.data.agent_status,
            agent: envelope.data.agent,
            title: envelope.data.title,
            display_agent: envelope.data.display_agent,
            custom_status: envelope.data.custom_status,
            state_labels: envelope.data.state_labels,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn mirror_event_reader_bounds_each_line() {
        let mut valid = std::io::Cursor::new(b"status\nnext".to_vec());
        assert_eq!(
            unix::read_bounded_line(&mut valid, 8).unwrap().as_deref(),
            Some("status\n")
        );
        assert_eq!(
            unix::read_bounded_line(&mut valid, 8).unwrap().as_deref(),
            Some("next")
        );

        let mut oversized = std::io::Cursor::new(b"123456789\n".to_vec());
        assert_eq!(
            unix::read_bounded_line(&mut oversized, 8)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[cfg(unix)]
    #[test]
    fn mirror_subscription_ack_requires_the_typed_result() {
        assert!(unix::is_subscription_started(
            r#"{"id":"sub","result":{"type":"subscription_started"}}"#
        ));
        assert!(!unix::is_subscription_started(
            r#"{"event":"subscription_started in attacker text"}"#
        ));
    }

    #[cfg(unix)]
    #[test]
    fn mirror_queue_send_stops_when_cancelled_even_if_full() {
        let (sender, _receiver) = std::sync::mpsc::sync_channel(1);
        sender.try_send(Ok("first".to_string())).unwrap();
        let cancelled = std::sync::atomic::AtomicBool::new(true);

        assert!(!unix::send_mirror_line(
            &sender,
            Ok("second".to_string()),
            &cancelled,
        ));
    }

    #[cfg(unix)]
    #[test]
    fn ssh_output_timeout_kills_the_process_group_and_returns_boundedly() {
        let mut command = std::process::Command::new("sh");
        command.arg("-c").arg("sleep 30 & wait");
        let started = std::time::Instant::now();

        let error =
            unix::command_output_with_timeout(command, std::time::Duration::from_millis(20))
                .unwrap_err();

        assert!(error.to_string().contains("timed out"));
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn unmanaged_ssh_cleanup_kills_the_group_and_drains_output_boundedly() {
        use std::os::unix::process::CommandExt as _;

        let mut command = std::process::Command::new("sh");
        command
            .process_group(0)
            .arg("-c")
            .arg("echo forwarding-denied >&2; sleep 30 & wait")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        let child = command.spawn().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        let started = std::time::Instant::now();

        let output =
            unix::kill_and_collect_ssh_process(child, std::time::Duration::from_millis(100))
                .unwrap();

        assert!(String::from_utf8_lossy(&output.stderr).contains("forwarding-denied"));
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn shared_managed_control_path_closes_only_after_its_last_bridge() {
        let connection = SshConnection {
            target: "workbox".into(),
            ssh_args: Vec::new(),
            managed_control_path: Some(format!(
                "/tmp/herdr-test-shared-control-{}",
                std::process::id()
            )),
            session: Some(crate::session::DEFAULT_SESSION_NAME.into()),
        };

        unix::retain_managed_control_path(&connection);
        unix::retain_managed_control_path(&connection);
        assert!(!unix::release_managed_control_path_with(
            &connection,
            |_| panic!("shared control path closed before its final release"),
        ));
        let closed = std::sync::atomic::AtomicBool::new(false);
        assert!(unix::release_managed_control_path_with(&connection, |_| {
            closed.store(true, std::sync::atomic::Ordering::Release)
        },));
        assert!(closed.load(std::sync::atomic::Ordering::Acquire));
    }

    #[cfg(unix)]
    #[test]
    fn final_managed_control_close_serializes_with_a_new_reservation() {
        let connection = SshConnection {
            target: "workbox".into(),
            ssh_args: Vec::new(),
            managed_control_path: Some(format!(
                "/tmp/herdr-test-control-close-race-{}",
                std::process::id()
            )),
            session: Some(crate::session::DEFAULT_SESSION_NAME.into()),
        };
        unix::retain_managed_control_path(&connection);

        let (close_started_tx, close_started_rx) = std::sync::mpsc::channel();
        let (finish_close_tx, finish_close_rx) = std::sync::mpsc::channel();
        let release_connection = connection.clone();
        let releaser = std::thread::spawn(move || {
            unix::release_managed_control_path_with(&release_connection, |_| {
                close_started_tx.send(()).unwrap();
                finish_close_rx.recv().unwrap();
            })
        });
        close_started_rx.recv().unwrap();

        let unrelated_connection = SshConnection {
            target: "other-workbox".into(),
            ssh_args: Vec::new(),
            managed_control_path: Some(format!(
                "/tmp/herdr-test-unrelated-control-{}",
                std::process::id()
            )),
            session: Some(crate::session::DEFAULT_SESSION_NAME.into()),
        };
        let (unrelated_tx, unrelated_rx) = std::sync::mpsc::channel();
        let unrelated_retain_connection = unrelated_connection.clone();
        let unrelated_retainer = std::thread::spawn(move || {
            unix::retain_managed_control_path(&unrelated_retain_connection);
            unrelated_tx.send(()).unwrap();
        });
        unrelated_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        unrelated_retainer.join().unwrap();

        let (reserved_tx, reserved_rx) = std::sync::mpsc::channel();
        let retain_connection = connection.clone();
        let retainer = std::thread::spawn(move || {
            unix::retain_managed_control_path(&retain_connection);
            reserved_tx.send(()).unwrap();
        });
        assert_eq!(
            reserved_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
        );

        finish_close_tx.send(()).unwrap();
        assert!(releaser.join().unwrap());
        reserved_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        retainer.join().unwrap();
        assert!(unix::release_managed_control_path_with(&connection, |_| {},));
        assert!(unix::release_managed_control_path_with(
            &unrelated_connection,
            |_| {},
        ));
    }

    #[cfg(unix)]
    #[test]
    fn conditional_peer_unregister_matches_only_its_forwarded_socket() {
        let response = serde_json::to_value(crate::api::schema::SuccessResponse {
            id: "peer-list".into(),
            result: crate::api::schema::ResponseResult::PeerList {
                peers: vec![PeerInfo {
                    id: "origin-123".into(),
                    label: "origin".into(),
                    status: crate::api::schema::PeerStatus::Connected,
                    transport: crate::api::schema::PeerTransportInfo::ApiSocket {
                        api_socket: "/tmp/current.sock".into(),
                    },
                }],
            },
        })
        .unwrap();

        assert!(unix::peer_registration_targets_socket(
            &response,
            "origin-123",
            "/tmp/current.sock",
        )
        .unwrap());
        assert!(!unix::peer_registration_targets_socket(
            &response,
            "origin-123",
            "/tmp/replaced.sock",
        )
        .unwrap());
        assert!(!unix::peer_registration_targets_socket(
            &response,
            "origin-other",
            "/tmp/current.sock",
        )
        .unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn default_session_keeps_the_canonical_ssh_peer_id() {
        let connection = SshConnection {
            target: "workbox".into(),
            ssh_args: Vec::new(),
            managed_control_path: None,
            session: Some(crate::session::DEFAULT_SESSION_NAME.into()),
        };

        assert_eq!(unix::peer_id_for_ssh(&connection), "workbox");
    }

    #[cfg(unix)]
    #[test]
    fn remote_session_defaults_and_uses_standard_name_validation() {
        assert_eq!(
            unix::explicit_remote_session(None).unwrap(),
            crate::session::DEFAULT_SESSION_NAME
        );
        assert_eq!(
            unix::explicit_remote_session(Some("-agents")).unwrap(),
            "-agents"
        );
        assert!(unix::explicit_remote_session(Some("work session")).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn ssh_federation_args_are_limited_to_one_destination() {
        let valid = SshConnection {
            target: "workbox".into(),
            ssh_args: vec!["-p".into(), "2222".into(), "-A".into()],
            managed_control_path: None,
            session: Some(crate::session::DEFAULT_SESSION_NAME.into()),
        };
        assert_eq!(
            unix::validated_interactive_ssh_args(&valid).unwrap(),
            ["-p", "2222", "-A", "workbox"]
        );

        for ssh_args in [
            vec!["other-host".into()],
            vec!["-S".into(), "/tmp/user-control".into()],
            vec!["-L".into(), "8080:localhost:80".into()],
        ] {
            let invalid = SshConnection {
                ssh_args,
                ..valid.clone()
            };
            assert!(unix::validated_interactive_ssh_args(&invalid).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn managed_helper_commands_replay_the_validated_session_options() {
        use std::os::unix::fs::PermissionsExt as _;
        use std::os::unix::net::UnixListener;

        let control_dir = std::env::temp_dir().join(format!(
            "herdr-ssh-control-options-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir(&control_dir).expect("control directory");
        std::fs::set_permissions(&control_dir, std::fs::Permissions::from_mode(0o700))
            .expect("private control directory");
        let control_path = control_dir.join("c");
        let listener = UnixListener::bind(&control_path).expect("control socket");
        let connection = SshConnection {
            target: "workbox".into(),
            ssh_args: vec!["-F".into(), "/tmp/ssh-config".into(), "-A".into()],
            managed_control_path: Some(control_path.display().to_string()),
            session: Some(crate::session::DEFAULT_SESSION_NAME.into()),
        };

        let command = unix::ssh_command(&connection).expect("SSH command");
        let args = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();

        assert!(args
            .windows(2)
            .any(|pair| pair == ["-F", "/tmp/ssh-config"]));
        assert!(args.iter().any(|arg| arg == "-A"));
        assert!(args
            .windows(2)
            .any(|pair| { pair == ["-S", connection.managed_control_path.as_deref().unwrap()] }));
        assert_eq!(args.last().map(String::as_str), Some("workbox"));

        drop(listener);
        std::fs::remove_file(control_path).expect("remove control socket");
        std::fs::remove_dir(control_dir).expect("remove control directory");
    }

    #[test]
    fn mirrored_agent_snapshots_are_normalized_and_unsequenced() {
        let info: AgentInfo = serde_json::from_value(serde_json::json!({
            "terminal_id": "term_remote",
            "agent": "  custom\u{0000}agent-with-a-name-that-is-deliberately-longer-than-eighty-characters-abcdefghijklmnopqrstuvwxyz  ",
            "agent_status": "working",
            "title": "  title\u{0007}text  ",
            "display_agent": "  display\nagent  ",
            "custom_status": "  custom\tstatus-that-is-longer-than-thirty-two-characters  ",
            "state_labels": {
                "WORKING": "  building\u{0000}now  ",
                "attacker-state": "must be dropped"
            },
            "agent_session": {
                "source": "attacker:rotating-source",
                "agent": "custom",
                "kind": "id",
                "value": "session-1"
            },
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "pane_id": "w1:p1",
            "focused": false,
            "revision": 0
        }))
        .unwrap();

        let events = events_from_agent_info(PaneId::from_raw(7), &info, None);
        assert_eq!(events.len(), 2);
        let AppEvent::HookStateReported {
            source,
            agent_label,
            custom_status,
            seq,
            session_ref,
            ..
        } = &events[0]
        else {
            panic!("unexpected mirror state event: {:?}", events[0]);
        };
        assert_eq!(source, REMOTE_MIRROR_STATE_SOURCE);
        assert!(agent_label.chars().count() <= 80);
        assert!(!agent_label.chars().any(char::is_control));
        assert!(custom_status.as_ref().unwrap().chars().count() <= 32);
        assert_eq!(*seq, None);
        assert!(session_ref.is_none());

        let AppEvent::HookMetadataReported {
            source,
            applies_to_source,
            title,
            display_agent,
            state_labels,
            seq,
            ..
        } = &events[1]
        else {
            panic!("unexpected mirror metadata event: {:?}", events[1]);
        };
        assert_eq!(source, REMOTE_MIRROR_METADATA_SOURCE);
        assert_eq!(
            applies_to_source.as_deref(),
            Some(REMOTE_MIRROR_STATE_SOURCE)
        );
        assert_eq!(title.as_deref(), Some("titletext"));
        assert_eq!(display_agent.as_deref(), Some("displayagent"));
        assert_eq!(state_labels.len(), 1);
        assert_eq!(
            state_labels.get("working").map(String::as_str),
            Some("buildingnow")
        );
        assert_eq!(*seq, None);
        for event in events {
            assert!(!matches!(event, AppEvent::AgentSessionReported { .. }));
        }
    }

    #[test]
    fn ssh_attach_argv_quotes_session_and_terminal() {
        let argv = ssh_attach_argv(
            "devbox",
            &["-p".into(), "2222".into()],
            None,
            Some("my session"),
            "term_abc",
            true,
            Some(&crate::api::schema::TerminalDelegationClaim {
                delegation_id: "delegation one".into(),
                epoch: 7,
            }),
        );

        assert!(argv[0].ends_with("ssh"));
        assert_eq!(argv[1], "-tt");
        assert_eq!(argv[2], "-p");
        assert_eq!(argv[3], "2222");
        assert_eq!(argv[4], "devbox");
        assert!(argv[5].contains("--session 'my session'"));
        assert!(argv[5].contains("terminal attach term_abc --takeover"));
        assert!(argv[5].contains("--delegation 'delegation one'"));
        assert!(argv[5].contains("--delegation-epoch 7"));
    }

    #[test]
    fn ssh_attach_argv_respects_observer_mode() {
        let argv = ssh_attach_argv("devbox", &[], None, None, "term_abc", false, None);

        assert!(argv[3].contains("terminal attach term_abc"));
        assert!(!argv[3].contains("--takeover"));
    }

    #[test]
    fn remote_herdr_commands_run_through_posix_sh() {
        let command = remote_terminal_shell_command(Some("agents"), Some("remote shell"));

        assert!(command.starts_with("sh -c "));
        assert!(command.contains("$HOME/.local/bin/herdr"));
        assert!(command.contains("--session agents terminal shell"));
        assert!(command.contains("--label 'remote shell'"));
    }

    #[cfg(unix)]
    #[test]
    fn remote_agent_selection_prefers_explicit_agent() {
        assert_eq!(
            validate_remote_agent_selection(Some("open-code"), &["anything".into()], true,)
                .unwrap(),
            (Some("opencode".into()), Some("opencode".into()))
        );
    }

    #[cfg(unix)]
    #[test]
    fn remote_agent_selection_rejects_unknown_explicit_agent() {
        let err =
            validate_remote_agent_selection(Some("unknown-agent"), &["opencode".into()], false)
                .unwrap_err();
        assert!(err.contains("unsupported agent label"));
    }

    #[cfg(unix)]
    #[test]
    fn remote_agent_selection_requires_a_recognized_integration_before_ssh() {
        let err =
            validate_remote_agent_selection(None, &["custom-agent".into()], true).unwrap_err();
        assert!(err.contains("recognizable argv[0]"));

        assert_eq!(
            validate_remote_agent_selection(None, &["custom-agent".into()], false).unwrap(),
            (None, None),
        );
    }
}
