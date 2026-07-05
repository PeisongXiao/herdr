use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::api::client::{ApiClient, ApiClientError};
#[cfg(unix)]
use crate::api::schema::{
    AgentAttachInfo, PeerConnectSshParams, PeerDisconnectSshParams, PeerKeepaliveSshParams,
};
use crate::api::schema::{
    AgentStatus, ClientWindowTitleSetParams, EmptyParams, Method, OutputMatch, PaneAgentState,
    PaneWaitForOutputParams, ReadFormat, ReadSource, Request, SplitDirection, Subscription,
    TabCreateParams,
};

mod agent;
mod api;
mod completion;
mod integration;
mod notification;
mod pane;
mod peer;
mod plugin;
mod server;
mod spec;
mod status;
mod tab;
mod workspace;
mod worktree;

const TERMINAL_SESSION_OBSERVE_USAGE: &str =
    "usage: herdr terminal session observe <target> [--cols N] [--rows N]";
const TERMINAL_SESSION_CONTROL_USAGE: &str =
    "usage: herdr terminal session control <target> [--takeover] [--cols N] [--rows N]";

pub(crate) fn parse_env_assignment(raw: &str) -> Result<(String, String), String> {
    let Some((key, value)) = raw.split_once('=') else {
        return Err("env must use KEY=VALUE".into());
    };
    if key.is_empty() {
        return Err("env key must not be empty".into());
    }
    if key.contains('\0') || value.contains('\0') {
        return Err("env must not contain NUL bytes".into());
    }
    Ok((key.to_string(), value.to_string()))
}

pub enum CommandOutcome {
    Handled(i32),
    NotCli,
}

pub(crate) fn run_ssh_os_command(args: &[std::ffi::OsString]) -> std::io::Result<i32> {
    match ssh_os_args_as_utf8(args) {
        Some(args) => run_ssh_command(&args),
        None => crate::ssh_integration::run_real_ssh_os_args(args),
    }
}

fn ssh_os_args_as_utf8(args: &[std::ffi::OsString]) -> Option<Vec<String>> {
    args.iter()
        .map(|arg| arg.to_str().map(str::to_string))
        .collect()
}

pub fn maybe_run(args: &[String]) -> std::io::Result<CommandOutcome> {
    let Some(command) = args.get(1).map(|arg| arg.as_str()) else {
        return Ok(CommandOutcome::NotCli);
    };

    let exit_code = match command {
        "server" => {
            let Some(exit_code) = server::run_server_command(&args[2..])? else {
                return Ok(CommandOutcome::NotCli);
            };
            exit_code
        }
        "api" => api::run_api_command(&args[2..])?,
        "status" => status::run_status_command(&args[2..])?,
        "completion" | "completions" => completion::run_completion_command(&args[2..])?,
        "config" => run_config_command(&args[2..])?,
        "channel" => run_channel_command(&args[2..])?,
        "workspace" => workspace::run_workspace_command(&args[2..])?,
        "worktree" => worktree::run_worktree_command(&args[2..])?,
        "tab" => tab::run_tab_command(&args[2..])?,
        "notification" => notification::run_notification_command(&args[2..])?,
        "agent" => agent::run_agent_command(&args[2..])?,
        "ssh" => run_ssh_command(&args[2..])?,
        "remote-handoff" => run_remote_handoff_command(&args[2..])?,
        "terminal" => run_terminal_command(&args[2..])?,
        "pane" => pane::run_pane_command(&args[2..])?,
        "peer" => peer::run_peer_command(&args[2..])?,
        "plugin" => plugin::run_plugin_command(&args[2..])?,
        "wait" => run_wait_command(&args[2..])?,
        "integration" => integration::run_integration_command(&args[2..])?,
        "session" => run_session_command(&args[2..])?,
        _ => return Ok(CommandOutcome::NotCli),
    };

    Ok(CommandOutcome::Handled(exit_code))
}

fn run_remote_handoff_command(args: &[String]) -> std::io::Result<i32> {
    if matches!(
        args.first().map(String::as_str),
        Some("help" | "--help" | "-h")
    ) {
        println!("usage: herdr remote-handoff");
        return Ok(0);
    }
    if !args.is_empty() {
        eprintln!("usage: herdr remote-handoff");
        return Ok(2);
    }
    let Some(pane_id) = std::env::var(crate::integration::HERDR_PANE_ID_ENV_VAR)
        .ok()
        .filter(|pane_id| !pane_id.is_empty())
    else {
        eprintln!("remote-handoff must be run in the Herdr pane containing your cursor");
        return Ok(1);
    };
    let response = send_request(&Request {
        id: "cli:remote-handoff".into(),
        method: Method::TerminalDelegateHandoff(
            crate::api::schema::TerminalDelegateHandoffParams { pane_id },
        ),
    })?;
    if let Some(message) = response["error"]["message"].as_str() {
        eprintln!("remote-handoff failed: {message}");
        return Ok(1);
    }
    println!("remote pane handed off to a new workspace on this machine");
    Ok(0)
}

fn run_channel_command(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(|arg| arg.as_str()) {
        Some("set") => channel_set(&args[1..]),
        Some("show") if args.len() == 1 => {
            let config = crate::config::Config::load().config;
            println!("{}", config.update.channel.as_str());
            Ok(0)
        }
        Some("help" | "--help" | "-h") => {
            print_channel_help();
            Ok(0)
        }
        _ => {
            print_channel_help();
            Ok(2)
        }
    }
}

fn channel_set(args: &[String]) -> std::io::Result<i32> {
    let Some(channel) = parse_channel_set_arg(args) else {
        eprintln!("usage: herdr channel set <stable|preview>");
        return Ok(2);
    };

    if let Some(reason) = channel_set_rejection(
        channel,
        crate::update::preview_channel_rejection_for_current_install(),
    ) {
        eprintln!("{reason}.");
        return Ok(1);
    }

    let path = crate::config::config_path();
    let content = if path.exists() {
        std::fs::read_to_string(&path)?
    } else {
        String::new()
    };
    if let Err(err) = content.parse::<toml::Value>() {
        eprintln!(
            "config file at {} is invalid TOML: {err}. Fix it before changing the update channel.",
            path.display()
        );
        return Ok(1);
    }

    let updated = crate::config::upsert_section_value(
        &content,
        "update",
        "channel",
        &format!("\"{channel}\""),
    );
    if let Err(err) = updated.parse::<toml::Value>() {
        eprintln!(
            "changing the update channel would make {} invalid TOML: {err}; leaving config unchanged",
            path.display()
        );
        return Ok(1);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, updated)?;
    println!(
        "Herdr update channel set to {channel} in {}.",
        path.display()
    );

    match channel_set_install_action(
        crate::update::package_manager_channel_update_guidance_for_current_install(),
    ) {
        ChannelSetInstallAction::PrintGuidance(guidance) => {
            println!("{guidance}");
            return Ok(0);
        }
        ChannelSetInstallAction::RunSelfUpdate => {}
    }

    if let Err(err) = crate::update::self_update(crate::update::SelfUpdateOptions::default()) {
        eprintln!("update failed: {err}");
        eprintln!("Run `herdr update` to retry.");
        return Ok(1);
    }

    Ok(0)
}

fn parse_channel_set_arg(args: &[String]) -> Option<&str> {
    let channel = args.first().map(|arg| arg.as_str())?;
    if args.len() == 1 && matches!(channel, "stable" | "preview") {
        Some(channel)
    } else {
        None
    }
}

fn channel_set_rejection(
    channel: &str,
    install_rejection: Option<&'static str>,
) -> Option<&'static str> {
    if cfg!(windows) && channel == "stable" {
        return Some(
            "stable channel is not available on Windows yet; Windows builds are preview-only",
        );
    }

    if channel == "preview" {
        return install_rejection;
    }

    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelSetInstallAction {
    RunSelfUpdate,
    PrintGuidance(&'static str),
}

fn channel_set_install_action(
    package_manager_guidance: Option<&'static str>,
) -> ChannelSetInstallAction {
    match package_manager_guidance {
        Some(guidance) => ChannelSetInstallAction::PrintGuidance(guidance),
        None => ChannelSetInstallAction::RunSelfUpdate,
    }
}

fn print_channel_help() {
    eprintln!("herdr channel commands:");
    eprintln!("  herdr channel show                  print the configured update channel");
    eprintln!("  herdr channel set <stable|preview>  choose the update channel");
}

fn run_config_command(args: &[String]) -> std::io::Result<i32> {
    let Some(subcommand) = args.first().map(|arg| arg.as_str()) else {
        print_config_help();
        return Ok(2);
    };

    match subcommand {
        "reset-keys" => config_reset_keys(&args[1..]),
        "help" | "--help" | "-h" => {
            print_config_help();
            Ok(0)
        }
        _ => {
            print_config_help();
            Ok(2)
        }
    }
}

fn config_reset_keys(args: &[String]) -> std::io::Result<i32> {
    if !args.is_empty() {
        eprintln!("usage: herdr config reset-keys");
        return Ok(2);
    }

    let path = crate::config::config_path();
    if !path.exists() {
        println!(
            "No config file found at {}. Built-in v2 keybindings already apply.",
            path.display()
        );
        return Ok(0);
    }

    let content = std::fs::read_to_string(&path)?;
    let parsed = match content.parse::<toml::Value>() {
        Ok(value) => value,
        Err(err) => {
            eprintln!(
                "config file at {} is invalid TOML: {err}. Fix it manually or move it aside to use defaults.",
                path.display()
            );
            return Ok(1);
        }
    };
    let Some(table) = parsed.as_table() else {
        eprintln!(
            "config file at {} is invalid TOML: top-level config must be a table.",
            path.display()
        );
        return Ok(1);
    };

    if !table.contains_key("keys") {
        println!(
            "No [keys] config found in {}. Built-in v2 keybindings already apply.",
            path.display()
        );
        return Ok(0);
    }

    let (updated, removed) = crate::config::remove_keybinding_config_sections(&content);
    if !removed {
        eprintln!(
            "could not safely remove keybinding config from {} without rewriting comments; edit the file manually or remove the top-level keys setting.",
            path.display()
        );
        return Ok(1);
    }
    if let Err(err) = updated.parse::<toml::Value>() {
        eprintln!(
            "removing keybinding config would make {} invalid TOML: {err}; leaving config unchanged",
            path.display()
        );
        return Ok(1);
    }

    let backup_path = key_config_backup_path(&path);
    std::fs::copy(&path, &backup_path)?;
    std::fs::write(&path, updated)?;

    println!("Created backup: {}", backup_path.display());
    println!(
        "Removed [keys], [keys.indexed], and [[keys.command]] from {}.",
        path.display()
    );
    println!("Built-in v2 keybindings will apply after Herdr restarts or reloads config.");
    println!("If a Herdr server is running, run `herdr server reload-config` to apply this now.");
    println!(
        "To restore: cp {} {}",
        backup_path.display(),
        path.display()
    );
    Ok(0)
}

fn key_config_backup_path(path: &std::path::Path) -> std::path::PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("config.toml");
    path.with_file_name(format!("{file_name}.bak-keybind-v2-{timestamp}"))
}

fn run_terminal_command(args: &[String]) -> std::io::Result<i32> {
    let Some(subcommand) = args.first().map(|arg| arg.as_str()) else {
        print_terminal_help();
        return Ok(2);
    };

    match subcommand {
        "attach" => terminal_attach(&args[1..]),
        "shell" => terminal_shell(&args[1..]),
        "session" => terminal_session(&args[1..]),
        "title" => terminal_title(&args[1..]),
        "help" | "--help" | "-h" => {
            print_terminal_help();
            Ok(0)
        }
        _ => {
            print_terminal_help();
            Ok(2)
        }
    }
}

fn terminal_shell(args: &[String]) -> std::io::Result<i32> {
    let label = match args {
        [] => None,
        [flag, value] if flag == "--label" && !value.is_empty() => Some(value.clone()),
        _ => {
            eprintln!("usage: herdr terminal shell [--label LABEL]");
            return Ok(2);
        }
    };
    let cwd = std::env::current_dir()
        .ok()
        .and_then(|path| path.into_os_string().into_string().ok());
    let env = std::env::vars()
        .filter(|(key, _)| !key.starts_with("HERDR_"))
        .collect();
    let response = send_request(&Request {
        id: "cli:terminal:shell".into(),
        method: Method::TabCreate(TabCreateParams {
            workspace_id: None,
            cwd,
            focus: false,
            label,
            env,
        }),
    })?;
    if response.get("error").is_some() {
        eprintln!("{}", serde_json::to_string(&response).unwrap());
        return Ok(1);
    }
    let Some(terminal_id) = response["result"]["root_pane"]["terminal_id"].as_str() else {
        eprintln!("terminal shell failed: tab.create response did not include a terminal id");
        return Ok(1);
    };
    crate::client::run_terminal_attach(terminal_id.to_string(), false)?;
    Ok(0)
}

fn run_wait_command(args: &[String]) -> std::io::Result<i32> {
    let Some(subcommand) = args.first().map(|arg| arg.as_str()) else {
        print_wait_help();
        return Ok(2);
    };

    match subcommand {
        "output" => wait_output(&args[1..]),
        "agent-status" => wait_agent_status(&args[1..]),
        "help" | "--help" | "-h" => {
            print_wait_help();
            Ok(0)
        }
        _ => {
            print_wait_help();
            Ok(2)
        }
    }
}

fn run_session_command(args: &[String]) -> std::io::Result<i32> {
    let Some(subcommand) = args.first().map(|arg| arg.as_str()) else {
        print_session_help();
        return Ok(2);
    };

    match subcommand {
        "list" => session_list(&args[1..]),
        "attach" => session_attach_help(&args[1..]),
        "stop" => session_stop(&args[1..]),
        "delete" => session_delete(&args[1..]),
        "help" | "--help" | "-h" => {
            print_session_help();
            Ok(0)
        }
        _ => {
            print_session_help();
            Ok(2)
        }
    }
}

fn session_attach_help(args: &[String]) -> std::io::Result<i32> {
    if matches!(
        args.first().map(String::as_str),
        Some("help" | "--help" | "-h")
    ) {
        eprintln!("usage: herdr session attach <name>");
        return Ok(0);
    }
    eprintln!("usage: herdr session attach <name>");
    Ok(2)
}

fn session_list(args: &[String]) -> std::io::Result<i32> {
    let json = match parse_session_json_only(args, "usage: herdr session list [--json]") {
        Ok(json) => json,
        Err(code) => return Ok(code),
    };

    let sessions = crate::session::list_sessions()?;
    if json {
        _print_json(&serde_json::json!({
            "sessions": sessions,
        }));
    } else {
        print_session_table(&sessions);
    }
    Ok(0)
}

fn session_stop(args: &[String]) -> std::io::Result<i32> {
    let (name, json) =
        match parse_session_name_and_json(args, "usage: herdr session stop <name> [--json]") {
            Ok(parsed) => parsed,
            Err(code) => return Ok(code),
        };

    let target = match crate::session::parse_target_name(&name) {
        Ok(target) => target,
        Err(message) => {
            print_session_error("invalid_session_name", &message);
            return Ok(1);
        }
    };
    match crate::session::stop_session(target.as_deref()) {
        Ok(session) => {
            if json {
                _print_json(&serde_json::json!({
                    "stopped": true,
                    "session": session,
                }));
            } else {
                println!("stopped session {}", session.name);
            }
            Ok(0)
        }
        Err(message) => {
            print_session_error("session_stop_failed", &message);
            Ok(1)
        }
    }
}

fn session_delete(args: &[String]) -> std::io::Result<i32> {
    let (name, json) =
        match parse_session_name_and_json(args, "usage: herdr session delete <name> [--json]") {
            Ok(parsed) => parsed,
            Err(code) => return Ok(code),
        };

    match crate::session::delete_session(&name) {
        Ok(session) => {
            if json {
                _print_json(&serde_json::json!({
                    "deleted": true,
                    "session": session,
                }));
            } else {
                println!("deleted session {}", session.name);
            }
            Ok(0)
        }
        Err(message) => {
            print_session_error("session_delete_failed", &message);
            Ok(1)
        }
    }
}

#[cfg(not(unix))]
fn run_ssh_command(args: &[String]) -> std::io::Result<i32> {
    crate::ssh_integration::run_real_ssh_args(args)
}

#[cfg(unix)]
fn run_ssh_command(args: &[String]) -> std::io::Result<i32> {
    if !crate::ssh_integration::should_integrate_invocation() {
        return crate::ssh_integration::run_real_ssh_args(args);
    }
    let Some(parsed) = crate::ssh_integration::parse_interactive_ssh_args(args) else {
        return crate::ssh_integration::run_real_ssh_args(args);
    };
    if !crate::ssh_integration::preflight_interactive_ssh_args(args).unwrap_or(false) {
        return crate::ssh_integration::run_real_ssh_args(args);
    }

    let mut managed = match crate::ssh_integration::prepare_managed_connection(
        &parsed.target,
        &parsed.ssh_args,
    ) {
        Ok(managed) => managed,
        Err(err) => {
            eprintln!("herdr ssh setup failed, falling back to ssh: {err}");
            return crate::ssh_integration::run_real_ssh_args(args);
        }
    };
    let managed_control_path = managed
        .as_ref()
        .map(crate::ssh_integration::ManagedSshConnection::control_path);

    let owner_pane_id = std::env::var(crate::integration::HERDR_PANE_ID_ENV_VAR).ok();
    let response = match send_request(&Request {
        id: "cli:ssh:connect".into(),
        method: Method::PeerConnectSsh(shim_peer_connect_params(
            &parsed,
            managed_control_path.clone(),
            owner_pane_id.clone(),
        )),
    }) {
        Ok(response) => response,
        Err(err) => {
            eprintln!("herdr ssh integration unavailable, falling back to ssh: {err}");
            drop(managed.take());
            return crate::ssh_integration::run_real_ssh_args(args);
        }
    };
    if response.get("error").is_some() {
        let message = response["error"]["message"]
            .as_str()
            .unwrap_or("ssh integration failed");
        eprintln!("herdr ssh integration failed, falling back to ssh: {message}");
        drop(managed.take());
        return crate::ssh_integration::run_real_ssh_args(args);
    }
    let Some(peer_id) = response["result"]["peer"]["id"]
        .as_str()
        .map(str::to_string)
    else {
        eprintln!("herdr ssh integration returned no peer id, falling back to ssh");
        drop(managed.take());
        return crate::ssh_integration::run_real_ssh_args(args);
    };
    let Some(connection_id) = response["result"]["connection_id"]
        .as_str()
        .map(str::to_string)
    else {
        eprintln!("herdr ssh integration returned no connection id, falling back to ssh");
        drop(managed.take());
        return crate::ssh_integration::run_real_ssh_args(args);
    };
    let attach =
        match serde_json::from_value::<AgentAttachInfo>(response["result"]["attach"].clone()) {
            Ok(attach) => attach,
            Err(err) => {
                let _ = disconnect_ssh_peer(&peer_id, &connection_id, None);
                eprintln!(
                "herdr ssh integration returned invalid attach info, falling back to ssh: {err}"
            );
                drop(managed.take());
                return crate::ssh_integration::run_real_ssh_args(args);
            }
        };
    let Some(argv) = crate::remote_agent::attach_argv_for_agent_attach(&attach, false) else {
        let _ = disconnect_ssh_peer(&peer_id, &connection_id, None);
        eprintln!(
            "herdr ssh integration returned an unsupported attach transport, falling back to ssh"
        );
        drop(managed.take());
        return crate::ssh_integration::run_real_ssh_args(args);
    };
    if let Some(managed) = managed.take() {
        let _ = managed.transfer();
    }
    let keepalive = SshPeerKeepalive::start(
        peer_id.clone(),
        connection_id.clone(),
        parsed.target,
        managed_control_path,
    );
    let result = crate::ssh_integration::run_ssh_argv(&argv);
    let presentation_activated = crate::remote_agent::delegation_was_activated(&attach)
        .ok()
        .flatten()
        .unwrap_or_else(|| result.as_ref().is_ok_and(|code| *code == 0));
    if !presentation_activated {
        let _ = crate::remote_agent::abandon_unactivated_delegation(&attach);
    }
    drop(keepalive);
    let activated_delegation = if presentation_activated {
        match &attach {
            AgentAttachInfo::Ssh { delegation, .. } => delegation.clone(),
            AgentAttachInfo::SshShell { .. } => None,
        }
    } else {
        None
    };
    if let Err(err) = disconnect_ssh_peer(&peer_id, &connection_id, activated_delegation) {
        eprintln!("warning: could not release Herdr SSH peer connection: {err}");
    }
    if presentation_activated {
        if let Some(owner_pane_id) = owner_pane_id {
            let _ = send_request(&Request {
                id: "cli:ssh:close-owner-pane".into(),
                method: Method::PaneClose(crate::api::schema::PaneTarget {
                    pane_id: owner_pane_id,
                }),
            });
        }
    }
    result
}

#[cfg(unix)]
fn shim_peer_connect_params(
    parsed: &crate::ssh_integration::ParsedSshInvocation,
    managed_control_path: Option<String>,
    owner_pane_id: Option<String>,
) -> PeerConnectSshParams {
    PeerConnectSshParams {
        target: parsed.target.clone(),
        ssh_args: parsed.ssh_args.clone(),
        managed_control_path,
        session: Some(explicit_remote_ssh_session(None)),
        label: Some(parsed.target.clone()),
        owner_pane_id,
        owner: None,
    }
}

fn explicit_remote_ssh_session(session: Option<String>) -> String {
    session.unwrap_or_else(|| crate::session::DEFAULT_SESSION_NAME.to_string())
}

#[cfg(unix)]
struct SshPeerKeepalive {
    stop_tx: std::sync::mpsc::SyncSender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}

#[cfg(unix)]
impl SshPeerKeepalive {
    fn start(
        peer_id: String,
        connection_id: String,
        target: String,
        managed_control_path: Option<String>,
    ) -> Self {
        let (stop_tx, stop_rx) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || loop {
            match stop_rx.recv_timeout(std::time::Duration::from_secs(3)) {
                Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            }
            if managed_control_path.as_deref().is_some_and(|control_path| {
                !crate::ssh_integration::managed_control_connection_is_alive(&target, control_path)
            }) {
                break;
            }
            let response = send_ssh_control_request(&Request {
                id: "cli:ssh:keepalive".into(),
                method: Method::PeerKeepaliveSsh(PeerKeepaliveSshParams {
                    peer_id: peer_id.clone(),
                    connection_id: connection_id.clone(),
                }),
            });
            if ssh_keepalive_should_stop(&response) {
                break;
            }
        });
        Self {
            stop_tx,
            thread: Some(thread),
        }
    }
}

#[cfg(unix)]
fn ssh_keepalive_should_stop(response: &std::io::Result<serde_json::Value>) -> bool {
    response
        .as_ref()
        .is_ok_and(|value| value.get("error").is_some())
}

#[cfg(unix)]
impl Drop for SshPeerKeepalive {
    fn drop(&mut self) {
        let _ = self.stop_tx.try_send(());
        if let Some(thread) = self.thread.take() {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
            while !thread.is_finished() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            if thread.is_finished() {
                let _ = thread.join();
            }
        }
    }
}

#[cfg(unix)]
fn disconnect_ssh_peer(
    peer_id: &str,
    connection_id: &str,
    activated_delegation: Option<crate::api::schema::TerminalDelegationClaim>,
) -> std::io::Result<()> {
    let response = send_ssh_control_request(&Request {
        id: "cli:ssh:disconnect".into(),
        method: Method::PeerDisconnectSsh(PeerDisconnectSshParams {
            peer_id: peer_id.to_string(),
            connection_id: connection_id.to_string(),
            activated_delegation,
        }),
    })?;
    if let Some(message) = response["error"]["message"].as_str() {
        return Err(std::io::Error::other(message.to_string()));
    }
    Ok(())
}

#[cfg(unix)]
fn send_ssh_control_request(request: &Request) -> std::io::Result<serde_json::Value> {
    ApiClient::local()
        .request_value_with_timeout(request, std::time::Duration::from_secs(2))
        .map_err(api_client_error_to_io)
}

fn terminal_attach(args: &[String]) -> std::io::Result<i32> {
    let Some(terminal_id) = args
        .first()
        .filter(|value| !value.starts_with('-'))
        .cloned()
    else {
        eprintln!("usage: herdr terminal attach <terminal_id> [--takeover]");
        return Ok(2);
    };
    let mut takeover = false;
    let mut delegation_id = None;
    let mut delegation_epoch = None;
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--takeover" => {
                takeover = true;
                index += 1;
            }
            "--delegation" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --delegation");
                    return Ok(2);
                };
                delegation_id = Some(value.clone());
                index += 2;
            }
            "--delegation-epoch" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --delegation-epoch");
                    return Ok(2);
                };
                delegation_epoch = match value.parse::<u64>() {
                    Ok(value) => Some(value),
                    Err(_) => {
                        eprintln!("invalid value for --delegation-epoch: {value}");
                        return Ok(2);
                    }
                };
                index += 2;
            }
            _ => {
                eprintln!("usage: herdr terminal attach <terminal_id> [--takeover]");
                return Ok(2);
            }
        }
    }
    let delegation = match (delegation_id, delegation_epoch) {
        (None, None) => None,
        (Some(delegation_id), Some(epoch)) => Some(crate::api::schema::TerminalDelegationClaim {
            delegation_id,
            epoch,
        }),
        _ => {
            eprintln!("--delegation and --delegation-epoch must be used together");
            return Ok(2);
        }
    };
    #[cfg(unix)]
    crate::client::run_terminal_attach_with_delegation(terminal_id, takeover, delegation)?;
    #[cfg(windows)]
    {
        if delegation.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "remote terminal delegation is not supported on Windows yet",
            ));
        }
        crate::client::run_terminal_attach(terminal_id, takeover)?;
    }
    Ok(0)
}

fn terminal_session(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(|arg| arg.as_str()) {
        Some("control") => terminal_session_control(&args[1..]),
        Some("observe") => terminal_session_observe(&args[1..]),
        Some("help" | "--help" | "-h") => {
            eprintln!("{TERMINAL_SESSION_CONTROL_USAGE}");
            eprintln!("{TERMINAL_SESSION_OBSERVE_USAGE}");
            Ok(0)
        }
        _ => {
            eprintln!("{TERMINAL_SESSION_CONTROL_USAGE}");
            eprintln!("{TERMINAL_SESSION_OBSERVE_USAGE}");
            Ok(2)
        }
    }
}

fn terminal_session_control(args: &[String]) -> std::io::Result<i32> {
    let options = match parse_terminal_session_options(
        args,
        TERMINAL_SESSION_CONTROL_USAGE,
        "control",
        true,
    )? {
        Ok(options) => options,
        Err(code) => return Ok(code),
    };

    crate::client::run_terminal_session_control(
        options.target,
        options.takeover,
        options.cols,
        options.rows,
    )?;
    Ok(0)
}

fn terminal_session_observe(args: &[String]) -> std::io::Result<i32> {
    let options = match parse_terminal_session_options(
        args,
        TERMINAL_SESSION_OBSERVE_USAGE,
        "observe",
        false,
    )? {
        Ok(options) => options,
        Err(code) => return Ok(code),
    };

    crate::client::run_terminal_session_observe(options.target, options.cols, options.rows)?;
    Ok(0)
}

struct TerminalSessionOptions {
    target: String,
    cols: u16,
    rows: u16,
    takeover: bool,
}

fn parse_terminal_session_options(
    args: &[String],
    usage: &str,
    command: &str,
    allow_takeover: bool,
) -> std::io::Result<Result<TerminalSessionOptions, i32>> {
    if matches!(
        args.first().map(|arg| arg.as_str()),
        Some("help" | "--help" | "-h")
    ) {
        eprintln!("{usage}");
        return Ok(Err(0));
    }
    let Some(target) = args.first() else {
        eprintln!("{usage}");
        return Ok(Err(2));
    };

    let mut cols = 120;
    let mut rows = 40;
    let mut takeover = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--takeover" if allow_takeover => {
                takeover = true;
                i += 1;
            }
            "--cols" => {
                let Some(value) = args.get(i + 1) else {
                    eprintln!("{usage}");
                    return Ok(Err(2));
                };
                cols = parse_terminal_dimension(value, "--cols")?;
                i += 2;
            }
            "--rows" => {
                let Some(value) = args.get(i + 1) else {
                    eprintln!("{usage}");
                    return Ok(Err(2));
                };
                rows = parse_terminal_dimension(value, "--rows")?;
                i += 2;
            }
            "help" | "--help" | "-h" => {
                eprintln!("{usage}");
                return Ok(Err(0));
            }
            other => {
                eprintln!("unknown terminal session {command} option: {other}");
                eprintln!("{usage}");
                return Ok(Err(2));
            }
        }
    }

    Ok(Ok(TerminalSessionOptions {
        target: target.clone(),
        cols,
        rows,
        takeover,
    }))
}

fn parse_terminal_dimension(raw: &str, flag: &str) -> std::io::Result<u16> {
    let parsed = raw.parse::<u16>().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{flag} must be an integer between 1 and {}", u16::MAX),
        )
    })?;
    if parsed == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{flag} must be greater than 0"),
        ));
    }
    Ok(parsed)
}

fn terminal_title(args: &[String]) -> std::io::Result<i32> {
    match args.first().map(|arg| arg.as_str()) {
        Some("set") => {
            if args.len() != 2 {
                eprintln!("usage: herdr terminal title set <title>");
                return Ok(2);
            }
            print_response(&send_request(&Request {
                id: "cli:terminal:title:set".into(),
                method: Method::ClientWindowTitleSet(ClientWindowTitleSetParams {
                    title: args[1].clone(),
                }),
            })?)
        }
        Some("clear") => {
            if args.len() != 1 {
                eprintln!("usage: herdr terminal title clear");
                return Ok(2);
            }
            print_response(&send_request(&Request {
                id: "cli:terminal:title:clear".into(),
                method: Method::ClientWindowTitleClear(EmptyParams::default()),
            })?)
        }
        Some("help" | "--help" | "-h") => {
            eprintln!("usage: herdr terminal title set <title>");
            eprintln!("       herdr terminal title clear");
            Ok(0)
        }
        _ => {
            eprintln!("usage: herdr terminal title set <title>");
            eprintln!("       herdr terminal title clear");
            Ok(2)
        }
    }
}

pub(super) fn parse_attach_target(args: &[String], usage: &str) -> Result<(String, bool), i32> {
    let Some(target) = args.first() else {
        eprintln!("{usage}");
        return Err(2);
    };
    let mut takeover = false;
    for arg in &args[1..] {
        match arg.as_str() {
            "--takeover" => takeover = true,
            "help" | "--help" | "-h" => {
                eprintln!("{usage}");
                return Err(0);
            }
            other => {
                eprintln!("unknown option: {other}");
                return Err(2);
            }
        }
    }
    Ok((target.clone(), takeover))
}

fn wait_output(args: &[String]) -> std::io::Result<i32> {
    let Some(raw_pane_id) = args.first() else {
        eprintln!("usage: herdr wait output <pane_id> --match <text> [--source visible|recent|recent-unwrapped] [--lines N] [--timeout MS] [--regex]");
        return Ok(2);
    };

    let pane_id = normalize_pane_id(raw_pane_id);
    let mut source = ReadSource::Recent;
    let mut lines = None;
    let mut timeout_ms = None;
    let mut strip_ansi = true;
    let mut regex = false;
    let mut match_value = None;

    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--match" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --match");
                    return Ok(2);
                };
                match_value = Some(value.clone());
                index += 2;
            }
            "--source" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --source");
                    return Ok(2);
                };
                source = parse_read_source(value)?;
                index += 2;
            }
            "--lines" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --lines");
                    return Ok(2);
                };
                lines = Some(parse_u32_flag("--lines", value)?);
                index += 2;
            }
            "--timeout" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --timeout");
                    return Ok(2);
                };
                timeout_ms = Some(parse_u64_flag("--timeout", value)?);
                index += 2;
            }
            "--regex" => {
                regex = true;
                index += 1;
            }
            "--raw" => {
                strip_ansi = false;
                index += 1;
            }
            other => {
                eprintln!("unknown option: {other}");
                return Ok(2);
            }
        }
    }

    let Some(match_value) = match_value else {
        eprintln!("missing required --match");
        return Ok(2);
    };

    let matcher = if regex {
        OutputMatch::Regex { value: match_value }
    } else {
        OutputMatch::Substring { value: match_value }
    };

    let response = send_request(&Request {
        id: "cli:wait:output".into(),
        method: Method::PaneWaitForOutput(PaneWaitForOutputParams {
            pane_id,
            source,
            lines,
            r#match: matcher,
            timeout_ms,
            strip_ansi,
        }),
    })?;

    if response.get("error").is_some() {
        eprintln!("{}", serde_json::to_string(&response).unwrap());
        return Ok(1);
    }

    println!("{}", serde_json::to_string(&response).unwrap());
    Ok(0)
}

fn wait_agent_status(args: &[String]) -> std::io::Result<i32> {
    let Some(raw_pane_id) = args.first() else {
        eprintln!("usage: herdr wait agent-status <pane_id> --status <idle|working|blocked|done|unknown> [--timeout MS]");
        return Ok(2);
    };

    let pane_id = normalize_pane_id(raw_pane_id);
    let mut timeout_ms = None;
    let mut desired_status = None;

    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--status" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --status");
                    return Ok(2);
                };
                desired_status = Some(parse_agent_status(value)?);
                index += 2;
            }
            "--timeout" => {
                let Some(value) = args.get(index + 1) else {
                    eprintln!("missing value for --timeout");
                    return Ok(2);
                };
                timeout_ms = Some(parse_u64_flag("--timeout", value)?);
                index += 2;
            }
            other => {
                eprintln!("unknown option: {other}");
                return Ok(2);
            }
        }
    }

    let Some(agent_status) = desired_status else {
        eprintln!("missing required --status");
        return Ok(2);
    };

    wait_for_agent_change(
        Request {
            id: "cli:wait:agent-status".into(),
            method: Method::EventsSubscribe(crate::api::schema::EventsSubscribeParams {
                subscriptions: vec![Subscription::PaneAgentStatusChanged {
                    pane_id,
                    agent_status: Some(agent_status),
                }],
            }),
        },
        timeout_ms,
        "timed out waiting for agent status change",
    )
}

pub(super) fn wait_for_agent_change(
    request: Request,
    timeout_ms: Option<u64>,
    timeout_message: &str,
) -> std::io::Result<i32> {
    let read_timeout = timeout_ms.map(Duration::from_millis);
    let (ack, mut stream) = ApiClient::local()
        .subscribe_value(&request, read_timeout)
        .map_err(api_client_error_to_io)?;
    if let Err(err) = crate::api::client::parse_response_value(ack) {
        if let ApiClientError::ErrorResponse(response) = err {
            eprintln!("{}", serde_json::to_string(&response).unwrap());
            return Ok(1);
        }
        return Err(api_client_error_to_io(err));
    }

    match stream.next_event() {
        Ok(None) => {
            eprintln!("subscription closed before event arrived");
            Ok(1)
        }
        Ok(Some(event_value)) => {
            println!("{}", serde_json::to_string(&event_value).unwrap());
            Ok(0)
        }
        Err(ApiClientError::Io(err)) if api_timeout_error(&err) => {
            eprintln!("{timeout_message}");
            Ok(1)
        }
        Err(err) => Err(api_client_error_to_io(err)),
    }
}

pub(super) fn print_response(response: &serde_json::Value) -> std::io::Result<i32> {
    if response.get("error").is_some() {
        eprintln!("{}", serde_json::to_string(response).unwrap());
        return Ok(1);
    }

    println!("{}", serde_json::to_string(response).unwrap());
    Ok(0)
}

pub(super) fn send_ok_request(method: Method) -> std::io::Result<i32> {
    let response = send_request(&Request {
        id: "cli:request".into(),
        method,
    })?;

    if response.get("error").is_some() {
        eprintln!("{}", serde_json::to_string(&response).unwrap());
        return Ok(1);
    }

    Ok(0)
}

pub(super) fn send_request(request: &Request) -> std::io::Result<serde_json::Value> {
    ApiClient::local()
        .request_value(request)
        .map_err(api_client_error_to_io)
}

fn api_timeout_error(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

fn api_client_error_to_io(err: ApiClientError) -> std::io::Error {
    match err {
        ApiClientError::Io(err) => err,
        err => std::io::Error::other(err),
    }
}

pub(super) fn normalize_workspace_id(value: &str) -> String {
    value.to_string()
}

pub(super) fn normalize_tab_id(value: &str) -> String {
    value.to_string()
}

pub(super) fn normalize_pane_id(value: &str) -> String {
    value.to_string()
}

pub(super) fn parse_split_direction(value: &str) -> std::io::Result<SplitDirection> {
    match value {
        "right" => Ok(SplitDirection::Right),
        "down" => Ok(SplitDirection::Down),
        _ => Err(std::io::Error::other(format!(
            "invalid split direction: {value}"
        ))),
    }
}

pub(super) fn parse_read_source(value: &str) -> std::io::Result<ReadSource> {
    match value {
        "visible" => Ok(ReadSource::Visible),
        "recent" => Ok(ReadSource::Recent),
        "recent-unwrapped" | "recent_unwrapped" => Ok(ReadSource::RecentUnwrapped),
        "detection" => Ok(ReadSource::Detection),
        _ => Err(std::io::Error::other(format!(
            "invalid read source: {value}"
        ))),
    }
}

pub(super) fn parse_read_format(value: &str) -> std::io::Result<ReadFormat> {
    match value {
        "text" => Ok(ReadFormat::Text),
        "ansi" => Ok(ReadFormat::Ansi),
        _ => Err(std::io::Error::other(format!(
            "invalid read format: {value}"
        ))),
    }
}

fn parse_agent_status(value: &str) -> std::io::Result<AgentStatus> {
    match value {
        "idle" => Ok(AgentStatus::Idle),
        "working" => Ok(AgentStatus::Working),
        "blocked" => Ok(AgentStatus::Blocked),
        "done" => Ok(AgentStatus::Done),
        "unknown" => Ok(AgentStatus::Unknown),
        _ => Err(std::io::Error::other(format!(
            "invalid agent status: {value} (expected idle, working, blocked, done, or unknown)"
        ))),
    }
}

pub(super) fn parse_pane_agent_state(value: &str) -> std::io::Result<PaneAgentState> {
    match value {
        "idle" => Ok(PaneAgentState::Idle),
        "working" => Ok(PaneAgentState::Working),
        "blocked" => Ok(PaneAgentState::Blocked),
        "unknown" => Ok(PaneAgentState::Unknown),
        _ => Err(std::io::Error::other(format!(
            "invalid pane agent state: {value} (expected idle, working, blocked, or unknown)"
        ))),
    }
}

pub(super) fn parse_u32_flag(flag: &str, value: &str) -> std::io::Result<u32> {
    value
        .parse::<u32>()
        .map_err(|_| std::io::Error::other(format!("invalid value for {flag}: {value}")))
}

pub(super) fn parse_u64_flag(flag: &str, value: &str) -> std::io::Result<u64> {
    value
        .parse::<u64>()
        .map_err(|_| std::io::Error::other(format!("invalid value for {flag}: {value}")))
}

fn parse_session_json_only(args: &[String], usage: &str) -> Result<bool, i32> {
    match args {
        [] => Ok(false),
        [flag] if flag == "--json" => Ok(true),
        _ => {
            eprintln!("{usage}");
            Err(2)
        }
    }
}

fn parse_session_name_and_json(args: &[String], usage: &str) -> Result<(String, bool), i32> {
    let mut name = None;
    let mut json = false;
    for arg in args {
        if arg == "--json" {
            json = true;
        } else if name.is_none() {
            name = Some(arg.clone());
        } else {
            eprintln!("{usage}");
            return Err(2);
        }
    }

    let Some(name) = name else {
        eprintln!("{usage}");
        return Err(2);
    };
    Ok((name, json))
}

fn print_session_table(sessions: &[crate::session::SessionInfo]) {
    println!("{:<20} {:<8} {:<48} socket", "name", "status", "directory");
    for session in sessions {
        println!(
            "{:<20} {:<8} {:<48} {}",
            session.name,
            if session.running {
                "running"
            } else {
                "stopped"
            },
            session.session_dir,
            session.socket_path
        );
    }
}

fn print_session_error(code: &str, message: &str) {
    eprintln!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "error": {
                "code": code,
                "message": message,
            }
        }))
        .unwrap()
    );
}

fn print_config_help() {
    eprintln!("herdr config commands:");
    eprintln!("  herdr config reset-keys  back up config.toml and remove custom keybindings");
}

fn print_terminal_help() {
    eprintln!("herdr terminal commands:");
    eprintln!("  herdr terminal attach <terminal_id> [--takeover]");
    eprintln!("  herdr terminal shell [--label LABEL]");
    eprintln!("  herdr terminal session control <target> [--takeover] [--cols N] [--rows N]");
    eprintln!("  herdr terminal session observe <target> [--cols N] [--rows N]");
    eprintln!("  herdr terminal title set <title>");
    eprintln!("  herdr terminal title clear");
    eprintln!("  detach from direct attach with ctrl+b q; send literal ctrl+b with ctrl+b ctrl+b");
}

fn print_wait_help() {
    eprintln!("herdr wait commands:");
    eprintln!("  herdr wait output <pane_id> --match <text> [--source visible|recent|recent-unwrapped] [--lines N] [--timeout MS] [--regex] [--raw]");
    eprintln!(
        "  herdr wait agent-status <pane_id> --status <idle|working|blocked|done|unknown> [--timeout MS]"
    );
}

fn print_session_help() {
    eprintln!("herdr session commands:");
    eprintln!("  herdr session list [--json]");
    eprintln!("  herdr session attach <name>");
    eprintln!("  herdr session stop <name> [--json]");
    eprintln!("  herdr session delete <name> [--json]");
    eprintln!("  use 'default' as <name> to target the default session for stop");
}

fn _print_json<T: Serialize>(value: &T) {
    println!("{}", serde_json::to_string(value).unwrap());
}

#[cfg(test)]
mod tests {
    #[test]
    fn parses_channel_set_argument() {
        assert_eq!(
            super::parse_channel_set_arg(&["preview".to_string()]),
            Some("preview")
        );
        assert_eq!(
            super::parse_channel_set_arg(&["stable".to_string()]),
            Some("stable")
        );
        assert_eq!(super::parse_channel_set_arg(&["nightly".to_string()]), None);
        assert_eq!(
            super::parse_channel_set_arg(&["preview".to_string(), "stable".to_string()]),
            None
        );
    }

    #[test]
    fn channel_set_rejects_package_managed_preview_before_config_write() {
        assert_eq!(
            super::channel_set_rejection("preview", Some("no preview")),
            Some("no preview")
        );
        assert_eq!(
            super::channel_set_rejection("stable", Some("no preview")),
            if cfg!(windows) {
                Some(
                    "stable channel is not available on Windows yet; Windows builds are preview-only",
                )
            } else {
                None
            }
        );
        assert_eq!(super::channel_set_rejection("preview", None), None);
    }

    #[test]
    fn channel_set_rejects_stable_only_on_windows() {
        assert_eq!(
            super::channel_set_rejection("stable", None),
            if cfg!(windows) {
                Some(
                    "stable channel is not available on Windows yet; Windows builds are preview-only",
                )
            } else {
                None
            }
        );
    }

    #[test]
    fn channel_set_skips_self_update_for_package_manager_guidance() {
        assert_eq!(
            super::channel_set_install_action(Some("use package manager")),
            super::ChannelSetInstallAction::PrintGuidance("use package manager")
        );
        assert_eq!(
            super::channel_set_install_action(None),
            super::ChannelSetInstallAction::RunSelfUpdate
        );
    }

    #[test]
    fn parse_env_assignment_accepts_empty_values() {
        assert_eq!(
            super::parse_env_assignment("HERDR_ROLE=").unwrap(),
            ("HERDR_ROLE".to_string(), String::new())
        );
    }

    #[test]
    fn parse_env_assignment_requires_key_value_separator() {
        assert_eq!(
            super::parse_env_assignment("HERDR_ROLE").unwrap_err(),
            "env must use KEY=VALUE"
        );
    }

    #[cfg(unix)]
    #[test]
    fn shim_ssh_uses_the_remote_default_session() {
        let parsed = crate::ssh_integration::ParsedSshInvocation {
            target: "workbox".to_string(),
            ssh_args: vec!["-p".to_string(), "2222".to_string()],
        };

        let params = super::shim_peer_connect_params(&parsed, None, None);

        assert_eq!(params.target, "workbox");
        assert_eq!(params.ssh_args, ["-p", "2222"]);
        assert_eq!(
            params.session.as_deref(),
            Some(crate::session::DEFAULT_SESSION_NAME)
        );
    }

    #[cfg(unix)]
    #[test]
    fn ssh_keepalive_retries_io_errors_but_stops_on_server_rejection() {
        assert!(!super::ssh_keepalive_should_stop(&Ok(serde_json::json!({
            "result": {}
        }))));
        assert!(!super::ssh_keepalive_should_stop(&Err(
            std::io::Error::new(std::io::ErrorKind::TimedOut, "temporary timeout")
        )));
        assert!(super::ssh_keepalive_should_stop(&Ok(serde_json::json!({
            "error": {"message": "connection is gone"}
        }))));
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_ssh_arguments_bypass_managed_interception() {
        use std::os::unix::ffi::OsStringExt as _;

        let args = vec![
            std::ffi::OsString::from("workbox"),
            std::ffi::OsString::from_vec(vec![b'-', b'F', 0xff]),
        ];

        assert!(super::ssh_os_args_as_utf8(&args).is_none());
    }
}
