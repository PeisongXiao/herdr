//! Internal app events delivered via channel.
//!
//! Background tasks (PTY child watchers, future hook listeners, etc.) send
//! events to the main loop through this channel. No polling needed.

use std::time::Instant;

use crate::detect::{Agent, AgentState};
use crate::layout::PaneId;
use crate::workspace::{GitStatusCacheEntry, WorkspaceGitStatus};

#[derive(Debug)]
pub struct ApiWorktreeAddRequest {
    pub id: String,
    pub operation_id: u64,
    pub checkout_key: std::path::PathBuf,
    pub source_workspace_id: Option<String>,
    pub source_existing_membership: Option<crate::workspace::WorktreeSpaceMembership>,
    pub source_checkout_path: std::path::PathBuf,
    pub source_repo_root: std::path::PathBuf,
    pub repo_key: String,
    pub repo_name: String,
    pub label: Option<String>,
    pub focus: bool,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

#[derive(Debug)]
pub struct WorktreeAddResult {
    pub path: std::path::PathBuf,
    pub api_request: Option<ApiWorktreeAddRequest>,
    pub result: Result<(), String>,
}

#[derive(Debug)]
pub struct ApiWorktreeRemoveRequest {
    pub id: String,
    pub operation_id: u64,
    pub checkout_key: std::path::PathBuf,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

#[derive(Debug)]
pub struct WorktreeRemoveResult {
    pub workspace_id: String,
    pub path: std::path::PathBuf,
    pub workspace: Option<Box<crate::api::schema::WorkspaceInfo>>,
    pub worktree: Option<Box<crate::api::schema::WorktreeInfo>>,
    pub forced: bool,
    pub api_request: Option<ApiWorktreeRemoveRequest>,
    pub result: Result<(), String>,
}

#[derive(Debug)]
pub struct PeerConnectSshResult {
    pub id: String,
    pub peer_id: String,
    pub generation: u64,
    pub owner_terminal_id: Option<crate::terminal::TerminalId>,
    pub result: Result<crate::remote_agent::RemoteShellConnect, String>,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

#[derive(Debug)]
pub struct RemoteAgentStartResult {
    pub id: String,
    pub params: crate::api::schema::AgentStartParams,
    pub initial_name: String,
    pub result: Result<crate::remote_agent::RemoteAgentStart, String>,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

/// One re-acquire attempt per handed-off pane, completed outside the app
/// loop. Carries the resume record so the app can prune it on success or
/// annotate it with the failure.
#[cfg(unix)]
#[derive(Debug)]
pub struct RemoteReacquireResult {
    pub record: crate::remote_resume::ResumeRecord,
    pub generation: u64,
    pub request_token: Option<u64>,
    pub result: Result<crate::remote_agent::RemoteAgentStart, RemoteReacquireFailure>,
}

#[cfg(unix)]
#[derive(Debug)]
pub enum RemoteReacquireFailure {
    Cancelled,
    TimedOut { message: String },
    Retryable { message: String },
    Ended { message: String },
}

/// A batch of re-acquire attempts (one peer) completed outside the app loop.
/// `respond_to` is set when a `remote.resume` API request is waiting for the
/// per-record outcomes.
#[cfg(unix)]
#[derive(Debug)]
pub struct RemoteReacquireBatch {
    pub peer_id: String,
    pub results: Vec<RemoteReacquireResult>,
    /// Kept for shutdown compatibility with legacy one-batch resume callers.
    /// New requests aggregate by `request_token` in `App` instead.
    pub respond_to: Option<(String, std::sync::mpsc::Sender<String>)>,
}

#[cfg(unix)]
#[derive(Debug)]
pub struct RemoteParkedTerminateResult {
    pub remote_terminal_id: String,
    pub result: Result<(), String>,
}

#[cfg(unix)]
#[derive(Debug)]
pub struct RemoteOrphanInventoryResult {
    pub peer_id: String,
    pub result: Result<Vec<crate::api::schema::TerminalParkedInfo>, String>,
}

#[cfg(unix)]
#[derive(Debug)]
pub struct RemoteOrphanResolveResult {
    pub entry: crate::app::state::OrphanReviewEntry,
    pub result: Result<RemoteOrphanResolveOutcome, String>,
}

#[cfg(unix)]
#[derive(Debug)]
pub enum RemoteOrphanResolveOutcome {
    Resolved,
    Promoted(Box<crate::remote_agent::RemoteAgentStart>),
}

#[derive(Debug)]
pub struct PeerAgentRequestResult {
    pub id: String,
    pub peer: crate::api::schema::PeerInfo,
    pub request: crate::api::schema::Request,
    pub owner_terminal_id: Option<crate::terminal::TerminalId>,
    pub result: Result<serde_json::Value, String>,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

#[derive(Debug)]
pub struct PeerHealthRequestResult {
    pub id: String,
    pub peer: crate::api::schema::PeerInfo,
    pub generation: u64,
    pub result: Result<serde_json::Value, String>,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

/// An event from a background task to the main loop.
#[derive(Debug)]
pub enum AppEvent {
    /// A pane's child process exited.
    PaneDied { pane_id: PaneId },
    /// Fallback detector state changed in a pane.
    StateChanged {
        pane_id: PaneId,
        agent: Option<Agent>,
        state: AgentState,
        visible_blocker: bool,
        visible_working: bool,
        process_exited: bool,
        observed_at: Instant,
    },
    /// Hook-authoritative agent state was reported for a pane.
    HookStateReported {
        pane_id: PaneId,
        source: String,
        agent_label: String,
        state: AgentState,
        message: Option<String>,
        custom_status: Option<String>,
        seq: Option<u64>,
        session_ref: Option<crate::agent_resume::AgentSessionRef>,
    },
    /// Agent session identity was reported without state authority.
    AgentSessionReported {
        pane_id: PaneId,
        source: String,
        agent_label: String,
        seq: Option<u64>,
        session_ref: Option<crate::agent_resume::AgentSessionRef>,
        session_start_source: Option<String>,
    },
    /// Display-only agent metadata was reported for a pane.
    HookMetadataReported {
        pane_id: PaneId,
        source: String,
        agent_label: Option<String>,
        applies_to_source: Option<String>,
        title: Option<String>,
        display_agent: Option<String>,
        custom_status: Option<String>,
        state_labels: std::collections::HashMap<String, String>,
        clear_title: bool,
        clear_display_agent: bool,
        clear_custom_status: bool,
        clear_state_labels: bool,
        seq: Option<u64>,
        ttl: Option<std::time::Duration>,
    },
    /// Hook authority was explicitly cleared for a pane.
    HookAuthorityCleared {
        pane_id: PaneId,
        source: Option<String>,
        seq: Option<u64>,
    },
    /// The current detected agent gracefully released this pane back to the shell.
    HookAgentReleased {
        pane_id: PaneId,
        source: String,
        agent_label: String,
        known_agent: Option<Agent>,
        seq: Option<u64>,
    },
    /// A background peer refresh completed without blocking the app loop.
    PeerAgentsRefreshed {
        peer_id: String,
        generation: u64,
        observed_at: Instant,
        result: Result<Vec<crate::api::schema::AgentInfo>, String>,
    },
    /// SSH peer setup completed outside the app loop.
    PeerConnectSshFinished(Box<PeerConnectSshResult>),
    /// Remote agent setup completed outside the app loop.
    RemoteAgentStartFinished(Box<RemoteAgentStartResult>),
    /// Remote pane re-acquire attempts completed outside the app loop.
    #[cfg(unix)]
    RemoteReacquireFinished(Box<RemoteReacquireBatch>),
    /// Best-effort cleanup requested by closing a restore reservation.
    #[cfg(unix)]
    RemoteParkedTerminateFinished(RemoteParkedTerminateResult),
    #[cfg(unix)]
    RemoteOrphanInventoryFinished(RemoteOrphanInventoryResult),
    #[cfg(unix)]
    RemoteOrphanResolveFinished(RemoteOrphanResolveResult),
    /// A peer-routed agent operation completed outside the app loop.
    PeerAgentRequestFinished(Box<PeerAgentRequestResult>),
    /// A peer health operation completed outside the app loop.
    PeerHealthRequestFinished(Box<PeerHealthRequestResult>),
    /// A prepared owner-side presentation was observed as active or expired remotely.
    RemotePresentationActivationObserved {
        delegation_id: String,
        activated: bool,
    },
    /// A remote mirror refreshed presentation metadata that belongs on the local transport.
    #[cfg(unix)]
    RemoteAgentInfoMirrored {
        pane_id: PaneId,
        remote_cwd: Option<String>,
    },
    /// A pane child emitted a valid OSC 52 clipboard write. The main loop
    /// re-emits it through herdr's own clipboard writer.
    ClipboardWrite { content: Vec<u8> },
    /// Prefix-mode ASCII input-source request, emitted on entering/leaving the ASCII input
    /// realm. The foreground process applies the host-local TIS switch (`active = true`) /
    /// restore (`active = false`): the client in server mode (via server forwarding), the
    /// app itself in monolithic mode.
    PrefixInputSource { active: bool },
    /// A pane child reported its shell current directory through terminal
    /// metadata such as OSC 7.
    TerminalCwdReported {
        pane_id: PaneId,
        cwd: std::path::PathBuf,
    },
    /// Background git status refresh completed for workspaces.
    GitStatusRefreshed {
        results: Vec<WorkspaceGitStatus>,
        cache_updates: Vec<(std::path::PathBuf, GitStatusCacheEntry)>,
    },
    /// A plugin action or event command finished.
    PluginCommandFinished {
        log_id: String,
        finished_unix_ms: u64,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
        error: Option<String>,
    },
    /// Background `git worktree add` completed.
    WorktreeAddFinished(Box<WorktreeAddResult>),
    /// Background `git worktree remove` completed.
    WorktreeRemoveFinished(Box<WorktreeRemoveResult>),
}
