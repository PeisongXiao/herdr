use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::agents::{AgentAttachInfo, AgentInfo};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalPresentationOwner {
    pub peer_id: String,
    pub pane_id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub route: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegationClaim {
    pub delegation_id: String,
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegationInfo {
    pub delegation_id: String,
    pub epoch: u64,
    pub terminal_id: String,
    pub pane_id: String,
    pub origin_peer_id: String,
    pub owner: TerminalPresentationOwner,
    pub status: TerminalDelegationStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalDelegationStatus {
    Pending,
    Active,
    Parked,
    TakenOver,
    HandedOff,
    Promoted,
    Terminated,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegateCreateParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub env: HashMap<String, String>,
    pub owner: TerminalPresentationOwner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegateClaimParams {
    pub target: String,
    pub owner: TerminalPresentationOwner,
    #[serde(default)]
    pub takeover: bool,
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub terminate_on_expire: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegationTarget {
    pub delegation_id: String,
    pub epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegateHandoffParams {
    pub pane_id: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalDelegateParkParams {
    pub target: TerminalDelegationTarget,
    pub park_id: String,
    pub origin_id: String,
    pub resume_token: String,
    pub discovery_token: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedTarget {
    pub park_id: String,
    pub origin_id: String,
    pub resume_token: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedResumeParams {
    #[serde(flatten)]
    pub target: TerminalParkedTarget,
    pub owner: TerminalPresentationOwner,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedListParams {
    pub origin_id: String,
    pub discovery_token: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalParkedResolveAction {
    Retain,
    Terminate,
    Promote,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedResolveParams {
    pub park_id: String,
    pub origin_id: String,
    pub discovery_token: String,
    pub action: TerminalParkedResolveAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<TerminalPresentationOwner>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedAdminListParams {
    pub admin_token: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedAdminResolveParams {
    pub park_id: String,
    pub admin_token: String,
    pub action: TerminalParkedResolveAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedInfo {
    pub park_id: String,
    pub terminal_id: String,
    pub pane_id: String,
    pub origin_id: String,
    pub status: TerminalDelegationStatus,
    #[serde(default, skip_serializing_if = "super::is_false")]
    pub resuming: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalParkedResumePrepared {
    pub delegation: TerminalDelegationInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentInfo>,
}

impl std::fmt::Debug for TerminalDelegateParkParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalDelegateParkParams")
            .field("target", &self.target)
            .field("park_id", &self.park_id)
            .field("origin_id", &self.origin_id)
            .field("resume_token", &"[redacted]")
            .field("discovery_token", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for TerminalParkedTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalParkedTarget")
            .field("park_id", &self.park_id)
            .field("origin_id", &self.origin_id)
            .field("resume_token", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for TerminalParkedResumeParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalParkedResumeParams")
            .field("target", &self.target)
            .field("owner", &self.owner)
            .finish()
    }
}

impl std::fmt::Debug for TerminalParkedListParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalParkedListParams")
            .field("origin_id", &self.origin_id)
            .field("discovery_token", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for TerminalParkedResolveParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalParkedResolveParams")
            .field("park_id", &self.park_id)
            .field("origin_id", &self.origin_id)
            .field("discovery_token", &"[redacted]")
            .field("action", &self.action)
            .field("owner", &self.owner)
            .finish()
    }
}

impl std::fmt::Debug for TerminalParkedAdminListParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalParkedAdminListParams")
            .field("admin_token", &"[redacted]")
            .finish()
    }
}

impl std::fmt::Debug for TerminalParkedAdminResolveParams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TerminalParkedAdminResolveParams")
            .field("park_id", &self.park_id)
            .field("admin_token", &"[redacted]")
            .field("action", &self.action)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentAttachPrepareParams {
    pub target: String,
    pub owner_pane_id: String,
    #[serde(default)]
    pub takeover: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentAttachPrepared {
    pub attach: AgentAttachInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation: Option<TerminalDelegationInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PeerRegisterParams {
    pub peer: PeerInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PeerConnectSshParams {
    pub target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ssh_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_control_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_pane_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<TerminalPresentationOwner>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PeerTarget {
    pub peer_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PeerDisconnectSshParams {
    pub peer_id: String,
    pub connection_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activated_delegation: Option<TerminalDelegationClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PeerKeepaliveSshParams {
    pub peer_id: String,
    pub connection_id: String,
}

/// Retry re-acquiring handed-off remote panes. Sent by `herdr remote-resume`
/// after the CLI has authenticated a managed SSH control connection for the
/// peer; the server then runs the re-acquire for each stored resume record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RemoteResumeParams {
    /// Limit the retry to one peer. Without it every pending record resumes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer_id: Option<String>,
    /// Interactive managed control path for the peer, when the CLI prepared
    /// one. Automatic re-acquire passes none and relies on BatchMode auth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub managed_control_path: Option<String>,
}

/// Per-record outcome of a `remote.resume` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RemoteResumeOutcome {
    pub remote_terminal_id: String,
    pub peer_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PeerInfo {
    pub id: String,
    pub label: String,
    pub status: PeerStatus,
    pub transport: PeerTransportInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus {
    Connected,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PeerTransportInfo {
    Ssh {
        target: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        ssh_args: Vec<String>,
        #[serde(skip)]
        #[schemars(skip)]
        managed_control_path: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session: Option<String>,
    },
    ApiSocket {
        api_socket: String,
    },
}
