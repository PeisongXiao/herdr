use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::client::{ApiClient, ApiClientError, ConnectionTarget};
use crate::api::schema::{
    AgentAttachInfo, AgentInfo, AgentTarget, EmptyParams, ErrorBody, Method, PeerInfo,
    PeerTransportInfo, Request,
};

use super::{
    terminal_targets::{TerminalTarget, TerminalTargetError},
    App,
};

const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) enum AgentRoute {
    Local(TerminalTarget),
    Peer { peer: PeerInfo, target: String },
}

pub(crate) fn project_peer_agent_metadata(mut agent: AgentInfo) -> AgentInfo {
    agent.mirror_of_terminal_id = agent.transport.as_ref().map(|transport| match transport {
        crate::api::schema::AgentTransportInfo::Ssh {
            remote_terminal_id, ..
        } => remote_terminal_id.clone(),
    });
    strip_peer_private_metadata(&mut agent);
    agent
}

fn strip_peer_private_metadata(agent: &mut AgentInfo) {
    agent.agent_session = None;
    agent.transport = None;
    agent.attach = None;
    agent.cwd = None;
    agent.foreground_cwd = None;
}

impl App {
    pub(crate) fn collect_agent_infos_with_peers(&self, include_peers: bool) -> Vec<AgentInfo> {
        let mut agents = self.collect_agent_infos();
        if !include_peers {
            return agents;
        }

        let local_terminal_ids: HashSet<String> = agents
            .iter()
            .map(|agent| agent.terminal_id.clone())
            .collect();
        let local_remote_terminal_ids: HashSet<String> = agents
            .iter()
            .filter_map(|agent| match agent.transport.as_ref() {
                Some(crate::api::schema::AgentTransportInfo::Ssh {
                    remote_terminal_id, ..
                }) => Some(remote_terminal_id.clone()),
                _ => None,
            })
            .collect();
        let local_peer_id = crate::remote_agent::local_peer_id();

        let mut peer_ids: Vec<String> = self.peer_agent_cache.keys().cloned().collect();
        peer_ids.sort();
        for peer_id in peer_ids {
            let Some(peer) = self.state.peers.get(&peer_id) else {
                continue;
            };
            let Some(peer_agents) = self.peer_agent_cache.get(&peer_id) else {
                continue;
            };
            for mut agent in peer_agents.clone() {
                if agent.presentation.as_ref().is_some_and(|presentation| {
                    presentation
                        .route
                        .iter()
                        .any(|peer_id| peer_id == &local_peer_id)
                }) {
                    continue;
                }
                if peer_agent_duplicates_local_mirror(
                    &agent,
                    &local_terminal_ids,
                    &local_remote_terminal_ids,
                ) {
                    continue;
                }
                self.annotate_peer_agent(peer, &mut agent);
                agents.push(agent);
            }
        }
        agents
    }

    pub(crate) fn resolve_agent_route(&self, target: &str) -> Result<AgentRoute, ErrorBody> {
        match self.resolve_terminal_target(target) {
            Ok(resolved) => return Ok(AgentRoute::Local(resolved)),
            Err(err @ TerminalTargetError::Ambiguous { .. }) => {
                return Err(self.agent_target_error_body(err));
            }
            Err(TerminalTargetError::NotFound { .. }) => {}
        }

        if let Some((peer_id, peer_target)) = target.split_once("::") {
            let Some(peer) = self.state.peers.get(peer_id).cloned() else {
                return Err(ErrorBody {
                    code: "peer_not_found".into(),
                    message: format!("peer {peer_id} not found"),
                });
            };
            if peer_target.is_empty() {
                return Err(ErrorBody {
                    code: "agent_not_found".into(),
                    message: format!("agent target {target} not found"),
                });
            }
            return Ok(AgentRoute::Peer {
                peer,
                target: peer_target.to_string(),
            });
        }

        let mut matches = Vec::new();
        for agent in self.collect_agent_infos_with_peers(true) {
            if agent.peer.is_some() && agent_matches_target(&agent, target) {
                matches.push((agent.peer.clone(), agent));
            }
        }

        match matches.len() {
            0 => Err(ErrorBody {
                code: "agent_not_found".into(),
                message: format!("agent target {target} not found"),
            }),
            1 => {
                let (peer_id, agent) = matches.remove(0);
                let Some(peer_id) = peer_id else {
                    return self
                        .resolve_terminal_target(target)
                        .map(AgentRoute::Local)
                        .map_err(|err| self.agent_target_error_body(err));
                };
                let Some(peer) = self.state.peers.get(&peer_id).cloned() else {
                    return Err(ErrorBody {
                        code: "peer_not_found".into(),
                        message: format!("peer {peer_id} not found"),
                    });
                };
                Ok(AgentRoute::Peer {
                    peer,
                    target: agent.terminal_id,
                })
            }
            _ => Err(ErrorBody {
                code: "agent_target_ambiguous".into(),
                message: format!(
                    "agent target {target} is ambiguous; candidates: {}",
                    matches
                        .into_iter()
                        .map(|(_, agent)| agent_candidate_text(&agent))
                        .collect::<Vec<_>>()
                        .join("; ")
                ),
            }),
        }
    }

    pub(crate) fn peer_request_value(
        &self,
        peer: &PeerInfo,
        request: &Request,
    ) -> Result<serde_json::Value, String> {
        peer_request_value(peer, request)
    }

    pub(crate) fn start_peer_refresh(&mut self, peer: PeerInfo) {
        self.cancel_peer_refresh(&peer.id);
        let generation = self.next_peer_refresh_generation;
        self.next_peer_refresh_generation = self.next_peer_refresh_generation.wrapping_add(1);
        self.peer_refresh_generations
            .insert(peer.id.clone(), generation);
        let cancelled = Arc::new(AtomicBool::new(false));
        self.peer_refresh_cancellations
            .insert(peer.id.clone(), Arc::clone(&cancelled));
        let event_tx = self.event_tx.clone();
        std::thread::spawn(move || {
            while !cancelled.load(Ordering::Acquire) && !event_tx.is_closed() {
                let result = peer_agent_list_value(&peer);
                let observed_at = Instant::now();
                if event_tx
                    .blocking_send(crate::events::AppEvent::PeerAgentsRefreshed {
                        peer_id: peer.id.clone(),
                        generation,
                        observed_at,
                        result,
                    })
                    .is_err()
                {
                    break;
                }
                let deadline = Instant::now() + PEER_REFRESH_INTERVAL;
                while Instant::now() < deadline {
                    if cancelled.load(Ordering::Acquire) || event_tx.is_closed() {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        });
    }

    pub(crate) fn cancel_peer_refresh(&mut self, peer_id: &str) {
        if let Some(cancelled) = self.peer_refresh_cancellations.remove(peer_id) {
            cancelled.store(true, Ordering::Release);
        }
        self.peer_refresh_generations.remove(peer_id);
        self.peer_agent_cache.remove(peer_id);
        self.incoming_peer_deadlines.remove(peer_id);
    }

    pub(crate) fn apply_peer_refresh(
        &mut self,
        peer_id: String,
        generation: u64,
        observed_at: Instant,
        result: Result<Vec<AgentInfo>, String>,
    ) {
        if self.peer_refresh_generations.get(&peer_id) != Some(&generation) {
            return;
        }
        if !self.state.peers.contains_key(&peer_id) {
            return;
        }
        match result {
            Ok(agents) => {
                // Transport is evidence for remote-mirror deduplication. Strip it only
                // when a record is projected for a consumer.
                self.peer_agent_cache.insert(peer_id.clone(), agents);
                self.renew_incoming_peer_lease(&peer_id, observed_at);
                if let Some(peer) = self.state.peers.get_mut(&peer_id) {
                    peer.status = if self
                        .peer_bridges
                        .get(&peer_id)
                        .is_none_or(|bridges| bridges.has_healthy_bridge())
                    {
                        crate::api::schema::PeerStatus::Connected
                    } else {
                        crate::api::schema::PeerStatus::Disconnected
                    };
                }
            }
            Err(err) => {
                tracing::debug!(%err, peer = %peer_id, "peer agent refresh failed");
                self.peer_agent_cache.remove(&peer_id);
                if let Some(peer) = self.state.peers.get_mut(&peer_id) {
                    peer.status = crate::api::schema::PeerStatus::Disconnected;
                }
            }
        }
    }

    pub(crate) fn peer_agent_get(&self, target: String) -> Request {
        Request {
            id: "peer:agent:get".into(),
            method: Method::PeerAgentGet(AgentTarget { target }),
        }
    }
}

pub(super) fn peer_request_value(
    peer: &PeerInfo,
    request: &Request,
) -> Result<serde_json::Value, String> {
    match &peer.transport {
        PeerTransportInfo::ApiSocket { api_socket, .. } => {
            peer_socket_request_value(&peer.id, api_socket, request)
        }
        PeerTransportInfo::Ssh {
            target,
            ssh_args,
            managed_control_path,
            session,
        } => crate::remote_agent::api_request(
            target,
            ssh_args,
            managed_control_path.as_deref(),
            session.as_deref(),
            request,
        )
        .map_err(|err| format!("peer {} unreachable: {err}", peer.id)),
    }
}

impl App {
    pub(crate) fn annotate_peer_agent(&self, peer: &PeerInfo, agent: &mut AgentInfo) {
        strip_peer_private_metadata(agent);
        agent.mirror_of_terminal_id = None;
        agent.peer = Some(peer.id.clone());
        agent.qualified_target = Some(format!(
            "{}::{}",
            peer.id,
            agent
                .name
                .as_deref()
                .or(agent.agent.as_deref())
                .unwrap_or(&agent.terminal_id)
        ));
        agent.attach = match &peer.transport {
            PeerTransportInfo::Ssh {
                target,
                ssh_args,
                managed_control_path,
                session,
            } => Some(AgentAttachInfo::Ssh {
                target: target.clone(),
                ssh_args: ssh_args.clone(),
                managed_control_path: managed_control_path.clone(),
                session: session.clone(),
                terminal_id: agent.terminal_id.clone(),
                delegation: None,
            }),
            PeerTransportInfo::ApiSocket { .. } => None,
        };
    }
}

fn peer_agent_list_value(peer: &PeerInfo) -> Result<Vec<AgentInfo>, String> {
    let response = peer_request_value(
        peer,
        &Request {
            id: "peer:agent:list".into(),
            method: Method::PeerAgentList(EmptyParams::default()),
        },
    )?;
    if response.get("error").is_some() {
        return Err(response["error"]["message"]
            .as_str()
            .unwrap_or("peer.agent.list failed")
            .to_string());
    }
    serde_json::from_value(response["result"]["agents"].clone())
        .map_err(|err| format!("peer.agent.list returned invalid agents: {err}"))
}

fn peer_socket_request_value(
    peer_id: &str,
    api_socket: &str,
    request: &Request,
) -> Result<serde_json::Value, String> {
    let client = ApiClient::for_target(ConnectionTarget::SocketPath(PathBuf::from(api_socket)));
    let deadline = Instant::now() + PEER_REQUEST_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout = remaining.min(Duration::from_secs(2));
        match client.request_value_with_timeout(request, timeout) {
            Ok(value) => return Ok(value),
            Err(err) if is_transient_peer_socket_error(&err) && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(format!("peer {peer_id} unreachable: {err}")),
        }
    }
}

fn is_transient_peer_socket_error(err: &ApiClientError) -> bool {
    matches!(
        err,
        ApiClientError::Io(io_err)
            if matches!(
                io_err.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::ConnectionRefused
                    | std::io::ErrorKind::WouldBlock
                    | std::io::ErrorKind::TimedOut
                    | std::io::ErrorKind::Interrupted
            )
    )
}

fn agent_matches_target(agent: &AgentInfo, target: &str) -> bool {
    agent.terminal_id == target
        || agent.pane_id == target
        || agent.name.as_deref() == Some(target)
        || agent.agent.as_deref() == Some(target)
        || agent.qualified_target.as_deref() == Some(target)
}

fn peer_agent_duplicates_local_mirror(
    agent: &AgentInfo,
    local_terminal_ids: &HashSet<String>,
    local_remote_terminal_ids: &HashSet<String>,
) -> bool {
    if local_remote_terminal_ids.contains(&agent.terminal_id) {
        return true;
    }
    if agent
        .mirror_of_terminal_id
        .as_ref()
        .is_some_and(|terminal_id| local_terminal_ids.contains(terminal_id))
    {
        return true;
    }
    match agent.transport.as_ref() {
        Some(crate::api::schema::AgentTransportInfo::Ssh {
            remote_terminal_id, ..
        }) => local_terminal_ids.contains(remote_terminal_id),
        _ => false,
    }
}

fn agent_candidate_text(agent: &AgentInfo) -> String {
    format!(
        "peer={} terminal_id={} pane_id={} workspace_id={} tab_id={} cwd={} status={:?}",
        agent.peer.as_deref().unwrap_or("local"),
        agent.terminal_id,
        agent.pane_id,
        agent.workspace_id,
        agent.tab_id,
        agent.cwd.as_deref().unwrap_or("unknown"),
        agent.agent_status,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{AgentPresentationInfo, AgentTransportInfo, PeerStatus};
    use crate::workspace::Workspace;

    fn app_with_peer() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.peers.insert(
            "peer".into(),
            PeerInfo {
                id: "peer".into(),
                label: "peer".into(),
                status: PeerStatus::Connected,
                transport: PeerTransportInfo::ApiSocket {
                    api_socket: "/path/that/must/not/be-opened.sock".into(),
                },
            },
        );
        app
    }

    fn add_named_local_agent(app: &mut App, name: &str) -> String {
        let workspace = Workspace::test_new("peer-routing");
        let root = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&root]
            .attached_terminal_id
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_agent_name(name.into());
        terminal_id.to_string()
    }

    fn peer_agent_info(terminal_id: &str) -> AgentInfo {
        serde_json::from_value(serde_json::json!({
            "terminal_id": terminal_id,
            "name": "peer-mirror",
            "agent_status": "working",
            "workspace_id": "peer-workspace",
            "tab_id": "peer-tab",
            "pane_id": "peer-pane",
            "focused": false,
            "revision": 4
        }))
        .unwrap()
    }

    #[test]
    fn peer_refresh_keeps_transport_until_remote_mirror_deduplication() {
        let mut app = app_with_peer();
        let local_terminal_id = add_named_local_agent(&mut app, "local-agent");
        let mut peer_agent = peer_agent_info("peer-mirror-terminal");
        peer_agent.transport = Some(AgentTransportInfo::Ssh {
            target: "remote.example".into(),
            ssh_args: Vec::new(),
            managed_control_path: None,
            session: None,
            remote_terminal_id: local_terminal_id.clone(),
            remote_pane_id: "remote-pane".into(),
            remote_agent: Some("codex".into()),
            remote_cwd: Some("/remote/worktree".into()),
        });
        app.peer_refresh_generations.insert("peer".into(), 7);

        app.apply_peer_refresh("peer".into(), 7, Instant::now(), Ok(vec![peer_agent]));

        assert!(app.peer_agent_cache["peer"][0].transport.is_some());
        let agents = app.collect_agent_infos_with_peers(true);
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].terminal_id, local_terminal_id);
    }

    #[test]
    fn peer_refresh_deduplicates_a_projected_remote_mirror_hint() {
        let mut app = app_with_peer();
        let local_terminal_id = add_named_local_agent(&mut app, "native-agent");
        let mut peer_agent = peer_agent_info("origin-mirror-terminal");
        peer_agent.transport = None;
        peer_agent.mirror_of_terminal_id = Some(local_terminal_id.clone());
        app.peer_refresh_generations.insert("peer".into(), 8);

        app.apply_peer_refresh("peer".into(), 8, Instant::now(), Ok(vec![peer_agent]));

        let agents = app.collect_agent_infos_with_peers(true);
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].terminal_id, local_terminal_id);
    }

    #[test]
    fn projected_mirror_hint_is_not_exposed_after_peer_annotation() {
        let mut app = app_with_peer();
        let mut peer_agent = peer_agent_info("origin-mirror-terminal");
        peer_agent.mirror_of_terminal_id = Some("third-party-terminal".into());
        app.peer_refresh_generations.insert("peer".into(), 9);

        app.apply_peer_refresh("peer".into(), 9, Instant::now(), Ok(vec![peer_agent]));

        let agents = app.collect_agent_infos_with_peers(true);
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].peer.as_deref(), Some("peer"));
        assert!(agents[0].mirror_of_terminal_id.is_none());
    }

    #[test]
    fn peer_projection_is_hidden_from_every_machine_on_its_owner_route() {
        let mut app = app_with_peer();
        let local_peer_id = crate::remote_agent::local_peer_id();
        let mut routed = peer_agent_info("routed-terminal");
        routed.presentation = Some(AgentPresentationInfo {
            origin_peer_id: "host-c".into(),
            owner_peer_id: "owner-a".into(),
            route: vec!["owner-a".into(), local_peer_id, "host-c".into()],
        });
        app.peer_refresh_generations.insert("peer".into(), 10);
        app.apply_peer_refresh("peer".into(), 10, Instant::now(), Ok(vec![routed]));

        assert!(app.collect_agent_infos_with_peers(true).is_empty());

        let mut unrelated = peer_agent_info("unrelated-terminal");
        unrelated.presentation = Some(AgentPresentationInfo {
            origin_peer_id: "host-c".into(),
            owner_peer_id: "owner-a".into(),
            route: vec!["owner-a".into(), "host-c".into()],
        });
        app.apply_peer_refresh("peer".into(), 10, Instant::now(), Ok(vec![unrelated]));

        assert_eq!(app.collect_agent_infos_with_peers(true).len(), 1);
    }

    #[test]
    fn exact_local_name_with_peer_separator_takes_precedence() {
        let mut app = app_with_peer();
        let local_terminal_id = add_named_local_agent(&mut app, "peer::worker");

        let route = app.resolve_agent_route("peer::worker").unwrap();

        let AgentRoute::Local(resolved) = route else {
            panic!("expected exact local agent name to resolve locally");
        };
        assert_eq!(resolved.terminal_id, local_terminal_id);
    }

    #[test]
    fn known_peer_qualification_still_routes_to_peer() {
        let app = app_with_peer();

        let route = app.resolve_agent_route("peer::worker").unwrap();

        let AgentRoute::Peer { peer, target } = route else {
            panic!("expected qualified peer route");
        };
        assert_eq!(peer.id, "peer");
        assert_eq!(target, "worker");
    }
}
