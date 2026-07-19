//! Private MCP bridge for desktop coding agents such as Codex Desktop.
//!
//! This module deliberately stays on the existing local API boundary. It never
//! starts, stops, discovers, subscribes to, or otherwise manages a Herdr server.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rmcp::model::{CallToolResult, JsonObject};
use rmcp::{tool, tool_router, ErrorData, ServiceExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Notify;

use crate::api::client::{ApiClient, ApiClientError, ConnectionTarget};
use crate::api::schema::{
    self, AgentInfo, AgentListParams, AgentRenameParams, AgentStartParams, EmptyParams, Method,
    PaneProcessInfoParams, PaneReadParams, PaneReadResult, PaneSendInputParams, PaneTarget,
    PingParams, ReadFormat, ReadSource, Request, ResponseResult, SplitDirection,
};
use crate::desktop_queue::{
    ChannelHandle, CliIdentity, CliState, DrainResult, QueueErrorKind, QueueManager,
    QUEUE_SCHEMA_VERSION,
};

const BRIDGE_VERSION: &str = env!("CARGO_PKG_VERSION");
const REQUIRED_PROTOCOL: u32 = 17;
const _: () = assert!(crate::protocol::PROTOCOL_VERSION == REQUIRED_PROTOCOL);
const SNAPSHOT_MAX_BYTES: usize = 2 * 1024 * 1024;
const PANE_READ_MAX_BYTES: usize = 256 * 1024;
const PANE_READ_DEFAULT_BYTES: usize = 64 * 1024;
const QUEUE_DRAIN_MAX_BYTES: usize = 1024 * 1024;
const MAX_ARGV: usize = 64;
const MAX_ARGV_BYTES: usize = 32 * 1024;
const MAX_NAME_BYTES: usize = 64;
const MAX_ENV_ENTRIES: usize = 64;
const MAX_ENV_BYTES: usize = 32 * 1024;
const MAX_ENV_KEY_BYTES: usize = 256;
const MAX_ENV_VALUE_BYTES: usize = 8 * 1024;
const MAX_INPUT_BYTES: usize = 64 * 1024;
const MAX_KEYS: usize = 32;
const DEFAULT_READ_LINES: u32 = 200;
const DEFAULT_DRAIN_LIMIT: usize = 20;
const MAX_DRAIN_LIMIT: usize = 100;
const CONTROL_ARGUMENT_MAX_BYTES: usize = 1024 * 1024;
const CONTROL_RESULT_MAX_BYTES: usize = SNAPSHOT_MAX_BYTES;
const DEFAULT_WAIT_MS: u64 = 60_000;
const MAX_WAIT_MS: u64 = 600_000;
const WAIT_ENVELOPE: Duration = Duration::from_secs(5);
const PRESENCE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

const HEALTH_TIMEOUT: Duration = Duration::from_secs(1);
const API_TIMEOUT: Duration = Duration::from_secs(5);
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(10);

const RESTRICTED_TOOL_NAMES: [&str; 10] = [
    "herdr_ack_messages",
    "herdr_drain_messages",
    "herdr_health",
    "herdr_interrupt_cli",
    "herdr_launch_cli",
    "herdr_pane_process_info",
    "herdr_read_pane",
    "herdr_send_input",
    "herdr_snapshot",
    "herdr_stop_cli",
];

const FULL_CONTROL_TOOL_NAMES: [&str; 11] = [
    "herdr_agent_control",
    "herdr_destructive_action",
    "herdr_inspect",
    "herdr_layout_control",
    "herdr_pane_control",
    "herdr_peer_control",
    "herdr_recovery_control",
    "herdr_tab_control",
    "herdr_wait",
    "herdr_workspace_control",
    "herdr_worktree_control",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccessMode {
    Restricted,
    FullControl,
}

impl AccessMode {
    fn wire_name(self) -> &'static str {
        match self {
            Self::Restricted => "restricted",
            Self::FullControl => "full_control",
        }
    }

    fn schema_value(self) -> schema::ControlClientAccessMode {
        match self {
            Self::Restricted => schema::ControlClientAccessMode::Restricted,
            Self::FullControl => schema::ControlClientAccessMode::FullControl,
        }
    }

    fn allows_tool(self, name: &str) -> bool {
        match self {
            Self::Restricted => RESTRICTED_TOOL_NAMES.contains(&name),
            Self::FullControl => {
                RESTRICTED_TOOL_NAMES.contains(&name) || FULL_CONTROL_TOOL_NAMES.contains(&name)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct BridgeError {
    code: String,
    message: String,
    retryable: bool,
    details: Option<Value>,
}

impl BridgeError {
    fn new(code: impl Into<String>, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
            details: None,
        }
    }

    fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    fn result(self) -> CallToolResult {
        let mut error = json!({
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        });
        if let Some(details) = self.details {
            error["details"] = details;
        }
        structured_result(json!({ "ok": false, "error": error }), true)
    }

    fn is_confirmed_absent(&self) -> bool {
        self.code == "cli_not_running"
            && self
                .details
                .as_ref()
                .and_then(|details| details.get("confirmed_absent"))
                .and_then(Value::as_bool)
                == Some(true)
    }
}

fn success(data: Value) -> CallToolResult {
    structured_result(json!({ "ok": true, "data": data }), false)
}

fn structured_result(value: Value, is_error: bool) -> CallToolResult {
    if is_error {
        CallToolResult::structured_error(value)
    } else {
        CallToolResult::structured(value)
    }
}

fn serialized_tool_result_len(result: &CallToolResult) -> Result<usize, BridgeError> {
    serde_json::to_vec(result)
        .map(|encoded| encoded.len())
        .map_err(|err| {
            BridgeError::new(
                "internal_error",
                format!("failed to encode bridge result: {err}"),
                false,
            )
        })
}

fn invalid_params(message: impl Into<String>) -> ErrorData {
    ErrorData::invalid_params(message.into(), None)
}

fn parse_params<T: serde::de::DeserializeOwned>(arguments: JsonObject) -> Result<T, ErrorData> {
    serde_json::from_value(Value::Object(arguments))
        .map_err(|err| invalid_params(format!("invalid parameters: {err}")))
}

fn parse_control_params<T: serde::de::DeserializeOwned>(
    arguments: JsonObject,
) -> Result<T, ErrorData> {
    let encoded_len = serde_json::to_vec(&arguments)
        .map_err(|err| invalid_params(format!("invalid parameters: {err}")))?
        .len();
    if encoded_len > CONTROL_ARGUMENT_MAX_BYTES {
        return Err(invalid_params(format!(
            "parameters must be at most {CONTROL_ARGUMENT_MAX_BYTES} bytes"
        )));
    }
    parse_params(arguments)
}

fn tool_input_schema<T: schemars::JsonSchema + Any>() -> Arc<JsonObject> {
    match rmcp::handler::server::common::schema_for_input::<T>() {
        Ok(schema) => schema,
        Err(err) => {
            eprintln!(
                "herdr mcp: failed to generate input schema for {}: {err}",
                std::any::type_name::<T>()
            );
            rmcp::handler::server::common::schema_for_empty_input()
        }
    }
}

fn join_error(err: tokio::task::JoinError) -> BridgeError {
    BridgeError::new(
        "internal_error",
        format!("bridge worker failed: {err}"),
        true,
    )
}

fn queue_error(err: crate::desktop_queue::QueueError) -> BridgeError {
    let kind = err.kind();
    let (code, retryable) = match kind {
        QueueErrorKind::PermissionDenied => ("queue_permission_denied", false),
        QueueErrorKind::Full => ("queue_full", true),
        QueueErrorKind::Corrupt => ("queue_corrupt", false),
        QueueErrorKind::CliNotFound => ("cli_not_found", false),
        QueueErrorKind::LeaseNotFound => ("lease_not_found", false),
        QueueErrorKind::Closed => ("cli_not_running", false),
        QueueErrorKind::LeaseExpired => ("lease_expired", false),
        QueueErrorKind::Io | QueueErrorKind::NotFound => ("queue_unavailable", true),
        QueueErrorKind::Invalid => ("queue_unavailable", false),
    };
    BridgeError::new(code, err.to_string(), retryable)
        .with_details(json!({ "kind": format!("{kind:?}") }))
}

fn api_error(err: ApiClientError) -> BridgeError {
    match err {
        ApiClientError::Io(err) => match err.kind() {
            io::ErrorKind::NotFound
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected => BridgeError::new(
                "server_unavailable",
                "the selected Herdr session is not running",
                true,
            ),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => BridgeError::new(
                "server_timeout",
                "the selected Herdr session did not respond in time",
                true,
            ),
            kind => BridgeError::new("server_unavailable", err.to_string(), true)
                .with_details(json!({ "io_kind": format!("{kind:?}") })),
        },
        ApiClientError::ErrorResponse(response) => {
            BridgeError::new(response.error.code, response.error.message, false)
        }
        ApiClientError::Json(err) => BridgeError::new(
            "server_protocol_error",
            format!("invalid response from Herdr: {err}"),
            false,
        ),
        ApiClientError::EmptyResponse => BridgeError::new(
            "server_protocol_error",
            "Herdr returned an empty response",
            false,
        ),
        ApiClientError::UnexpectedResult(result) => BridgeError::new(
            "server_protocol_error",
            format!("Herdr returned an unexpected result: {result}"),
            false,
        ),
    }
}

#[derive(Clone)]
struct ApiAdapter {
    client: ApiClient,
    next_id: Arc<AtomicU64>,
}

impl ApiAdapter {
    fn new(socket_path: PathBuf) -> Self {
        Self {
            client: ApiClient::for_target(ConnectionTarget::SocketPath(socket_path)),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }

    async fn request(
        &self,
        method: Method,
        timeout: Duration,
    ) -> Result<ResponseResult, BridgeError> {
        let request_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = Request {
            id: format!("desktop-mcp:{request_id}"),
            method,
        };
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || {
            let value = client.request_value_with_timeout(&request, timeout)?;
            crate::api::client::parse_response_value(value).map(|response| response.result)
        })
        .await
        .map_err(join_error)?
        .map_err(api_error)
    }

    async fn ping(&self) -> Result<ServerVersion, BridgeError> {
        match self
            .request(Method::Ping(PingParams::default()), HEALTH_TIMEOUT)
            .await?
        {
            ResponseResult::Pong {
                version, protocol, ..
            } => Ok(ServerVersion { version, protocol }),
            result => Err(unexpected_result("ping", result)),
        }
    }

    async fn require_compatible(&self) -> Result<ServerVersion, BridgeError> {
        let status = self.ping().await?;
        if status.protocol != REQUIRED_PROTOCOL {
            return Err(BridgeError::new(
                "server_incompatible",
                format!(
                    "Herdr protocol {} is incompatible; protocol {REQUIRED_PROTOCOL} is required",
                    status.protocol
                ),
                false,
            )
            .with_details(json!({
                "actual_protocol": status.protocol,
                "required_protocol": REQUIRED_PROTOCOL,
                "herdr_version": status.version,
            })));
        }
        Ok(status)
    }
}

#[derive(Debug)]
struct ServerVersion {
    version: String,
    protocol: u32,
}

fn unexpected_result(operation: &str, result: ResponseResult) -> BridgeError {
    BridgeError::new(
        "server_protocol_error",
        format!("unexpected Herdr result for {operation}: {result:?}"),
        false,
    )
}

fn remap_rejection(err: BridgeError, code: &'static str) -> BridgeError {
    if matches!(
        err.code.as_str(),
        "server_unavailable"
            | "server_timeout"
            | "server_protocol_error"
            | "server_incompatible"
            | "internal_error"
    ) {
        return err;
    }
    let mut details = json!({ "upstream_code": err.code });
    if let Some(upstream_details) = err.details {
        details["upstream_details"] = upstream_details;
    }
    BridgeError::new(code, err.message, err.retryable).with_details(details)
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct PaneIdParams {
    pane_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct NoParams {}

#[derive(Debug, Clone, Copy, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpReadSource {
    Visible,
    #[default]
    Recent,
    RecentUnwrapped,
}

impl From<McpReadSource> for ReadSource {
    fn from(value: McpReadSource) -> Self {
        match value {
            McpReadSource::Visible => Self::Visible,
            McpReadSource::Recent => Self::Recent,
            McpReadSource::RecentUnwrapped => Self::RecentUnwrapped,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpReadFormat {
    #[default]
    Text,
    Ansi,
}

impl From<McpReadFormat> for ReadFormat {
    fn from(value: McpReadFormat) -> Self {
        match value {
            McpReadFormat::Text => Self::Text,
            McpReadFormat::Ansi => Self::Ansi,
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadPaneParams {
    pane_id: String,
    #[serde(default)]
    source: McpReadSource,
    #[serde(default = "default_read_lines")]
    lines: u32,
    #[serde(default)]
    format: McpReadFormat,
    #[serde(default = "default_read_max_bytes")]
    max_bytes: usize,
}

fn default_read_lines() -> u32 {
    DEFAULT_READ_LINES
}

fn default_read_max_bytes() -> usize {
    PANE_READ_DEFAULT_BYTES
}

#[derive(Debug, Clone, Copy, Default, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum McpSplit {
    #[default]
    Right,
    Down,
}

impl From<McpSplit> for SplitDirection {
    fn from(value: McpSplit) -> Self {
        match value {
            McpSplit::Right => Self::Right,
            McpSplit::Down => Self::Down,
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct LaunchCliParams {
    name: String,
    argv: Vec<String>,
    cwd: String,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    tab_id: Option<String>,
    #[serde(default)]
    split: McpSplit,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SendInputParams {
    cli_id: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    keys: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct CliIdParams {
    cli_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct DrainMessagesParams {
    cli_id: String,
    #[serde(default = "default_drain_limit")]
    limit: usize,
}

fn default_drain_limit() -> usize {
    DEFAULT_DRAIN_LIMIT
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct AckMessagesParams {
    cli_id: String,
    lease_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum InspectParams {
    WorkspaceList(EmptyParams),
    WorkspaceGet(schema::WorkspaceTarget),
    TabList(schema::TabListParams),
    TabGet(schema::TabTarget),
    PaneList(schema::PaneListParams),
    PaneCurrent(schema::PaneCurrentParams),
    PaneGet(PaneTarget),
    PaneRead(PaneReadParams),
    PaneProcessInfo(PaneProcessInfoParams),
    PaneLayout(schema::PaneLayoutParams),
    PaneNeighbor(schema::PaneNeighborParams),
    PaneEdges(schema::PaneEdgesParams),
    LayoutExport(schema::LayoutExportParams),
    AgentList(AgentListParams),
    AgentGet(schema::AgentTarget),
    AgentRead(schema::AgentReadParams),
    AgentExplain(schema::AgentTarget),
    WorktreeList(schema::WorktreeListParams),
    PeerList(EmptyParams),
    PeerHealth(schema::PeerTarget),
    RemoteAgentList(EmptyParams),
    RemoteAgentGet(schema::AgentTarget),
    RemoteAgentRead(schema::AgentReadParams),
    RemoteAgentExplain(schema::AgentTarget),
    RecoveryList(schema::TerminalRecoveryListParams),
    RecoveryStatus(schema::TerminalRecoveryTarget),
}

impl InspectParams {
    fn into_method(self) -> Result<Method, ErrorData> {
        let method = match self {
            Self::WorkspaceList(params) => Method::WorkspaceList(params),
            Self::WorkspaceGet(params) => Method::WorkspaceGet(params),
            Self::TabList(params) => Method::TabList(params),
            Self::TabGet(params) => Method::TabGet(params),
            Self::PaneList(params) => Method::PaneList(params),
            Self::PaneCurrent(params) => Method::PaneCurrent(params),
            Self::PaneGet(params) => Method::PaneGet(params),
            Self::PaneRead(params) => Method::PaneRead(params),
            Self::PaneProcessInfo(params) => Method::PaneProcessInfo(params),
            Self::PaneLayout(params) => Method::PaneLayout(params),
            Self::PaneNeighbor(params) => Method::PaneNeighbor(params),
            Self::PaneEdges(params) => Method::PaneEdges(params),
            Self::LayoutExport(params) => Method::LayoutExport(params),
            Self::AgentList(params) => Method::AgentList(params),
            Self::AgentGet(params) => {
                validate_local_agent_target(&params.target)?;
                Method::AgentGet(params)
            }
            Self::AgentRead(params) => {
                validate_local_agent_target(&params.target)?;
                Method::AgentRead(params)
            }
            Self::AgentExplain(params) => {
                validate_local_agent_target(&params.target)?;
                Method::AgentExplain(params)
            }
            Self::WorktreeList(params) => Method::WorktreeList(params),
            Self::PeerList(params) => Method::PeerList(params),
            Self::PeerHealth(params) => Method::PeerHealth(params),
            Self::RemoteAgentList(_) => Method::AgentList(AgentListParams {
                include_peers: true,
            }),
            Self::RemoteAgentGet(params) => {
                validate_remote_agent_target(&params.target)?;
                Method::AgentGet(params)
            }
            Self::RemoteAgentRead(params) => {
                validate_remote_agent_target(&params.target)?;
                Method::AgentRead(params)
            }
            Self::RemoteAgentExplain(params) => {
                validate_remote_agent_target(&params.target)?;
                Method::AgentExplain(params)
            }
            Self::RecoveryList(params) => Method::TerminalRecoveryList(params),
            Self::RecoveryStatus(params) => Method::TerminalRecoveryStatus(params),
        };
        validate_control_method(&method)?;
        Ok(method)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WaitParams {
    Event(schema::EventsWaitParams),
    PaneOutput(schema::PaneWaitForOutputParams),
}

impl WaitParams {
    fn into_method(self) -> Result<(Method, Duration), ErrorData> {
        let (method, timeout_ms) = match self {
            Self::Event(mut params) => {
                let timeout_ms = normalize_wait_timeout(params.timeout_ms)?;
                params.timeout_ms = Some(timeout_ms);
                (Method::EventsWait(params), timeout_ms)
            }
            Self::PaneOutput(mut params) => {
                let timeout_ms = normalize_wait_timeout(params.timeout_ms)?;
                params.timeout_ms = Some(timeout_ms);
                validate_output_match(&params.r#match)?;
                (Method::PaneWaitForOutput(params), timeout_ms)
            }
        };
        validate_control_method(&method)?;
        Ok((
            method,
            Duration::from_millis(timeout_ms).saturating_add(WAIT_ENVELOPE),
        ))
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WorkspaceControlParams {
    Create(schema::WorkspaceCreateParams),
    Focus(schema::WorkspaceTarget),
    Rename(schema::WorkspaceRenameParams),
    Move(schema::WorkspaceMoveParams),
}

impl WorkspaceControlParams {
    fn into_method(self) -> Method {
        match self {
            Self::Create(params) => Method::WorkspaceCreate(params),
            Self::Focus(params) => Method::WorkspaceFocus(params),
            Self::Rename(params) => Method::WorkspaceRename(params),
            Self::Move(params) => Method::WorkspaceMove(params),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum TabControlParams {
    Create(schema::TabCreateParams),
    Focus(schema::TabTarget),
    Rename(schema::TabRenameParams),
    Move(schema::TabMoveParams),
}

impl TabControlParams {
    fn into_method(self) -> Method {
        match self {
            Self::Create(params) => Method::TabCreate(params),
            Self::Focus(params) => Method::TabFocus(params),
            Self::Rename(params) => Method::TabRename(params),
            Self::Move(params) => Method::TabMove(params),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum PaneControlParams {
    Split(schema::PaneSplitParams),
    Swap(schema::PaneSwapParams),
    Move(schema::PaneMoveParams),
    Zoom(schema::PaneZoomParams),
    Focus(PaneTarget),
    FocusDirection(schema::PaneFocusDirectionParams),
    Resize(schema::PaneResizeParams),
    Rename(schema::PaneRenameParams),
    SendInput(PaneSendInputParams),
}

impl PaneControlParams {
    fn into_method(self) -> Result<Method, ErrorData> {
        let method = match self {
            Self::Split(params) => Method::PaneSplit(params),
            Self::Swap(params) => Method::PaneSwap(params),
            Self::Move(params) => Method::PaneMove(params),
            Self::Zoom(params) => Method::PaneZoom(params),
            Self::Focus(params) => Method::PaneFocus(params),
            Self::FocusDirection(params) => Method::PaneFocusDirection(params),
            Self::Resize(params) => Method::PaneResize(params),
            Self::Rename(params) => Method::PaneRename(params),
            Self::SendInput(params) => Method::PaneSendInput(params),
        };
        validate_control_method(&method)?;
        Ok(method)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum LayoutControlParams {
    Apply(schema::LayoutApplyParams),
    SetSplitRatio(schema::LayoutSetSplitRatioParams),
}

impl LayoutControlParams {
    fn into_method(self) -> Method {
        match self {
            Self::Apply(params) => Method::LayoutApply(params),
            Self::SetSplitRatio(params) => Method::LayoutSetSplitRatio(params),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum AgentControlParams {
    Send(schema::AgentSendParams),
    Rename(AgentRenameParams),
    Focus(schema::AgentTarget),
    Start(Box<AgentStartParams>),
}

impl AgentControlParams {
    fn into_method(self) -> Result<Method, ErrorData> {
        let method = match self {
            Self::Send(params) => {
                validate_local_agent_target(&params.target)?;
                Method::AgentSend(params)
            }
            Self::Rename(params) => {
                validate_local_agent_target(&params.target)?;
                Method::AgentRename(params)
            }
            Self::Focus(params) => {
                validate_local_agent_target(&params.target)?;
                Method::AgentFocus(params)
            }
            Self::Start(params) => {
                validate_agent_start(&params, false)?;
                Method::AgentStart(*params)
            }
        };
        validate_control_method(&method)?;
        Ok(method)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum WorktreeControlParams {
    Create(schema::WorktreeCreateParams),
    Open(schema::WorktreeOpenParams),
}

impl WorktreeControlParams {
    fn into_method(self) -> Method {
        match self {
            Self::Create(params) => Method::WorktreeCreate(params),
            Self::Open(params) => Method::WorktreeOpen(params),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum PeerControlParams {
    ConnectSsh(schema::PeerConnectSshParams),
    DisconnectSsh(schema::PeerDisconnectSshParams),
    SendAgent(schema::AgentSendParams),
    RenameAgent(AgentRenameParams),
    StartAgent(AgentStartParams),
}

impl PeerControlParams {
    fn into_method(self) -> Result<Method, ErrorData> {
        let method = match self {
            Self::ConnectSsh(params) => Method::PeerConnectSsh(params),
            Self::DisconnectSsh(params) => Method::PeerDisconnectSsh(params),
            Self::SendAgent(params) => {
                validate_remote_agent_target(&params.target)?;
                Method::AgentSend(params)
            }
            Self::RenameAgent(params) => {
                validate_remote_agent_target(&params.target)?;
                Method::AgentRename(params)
            }
            Self::StartAgent(params) => {
                validate_agent_start(&params, true)?;
                Method::AgentStart(params)
            }
        };
        validate_control_method(&method)?;
        Ok(method)
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum RecoveryControlParams {
    Retry(schema::TerminalRecoveryTarget),
}

impl RecoveryControlParams {
    fn into_method(self) -> Method {
        match self {
            Self::Retry(params) => Method::TerminalRecoveryRetry(params),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(
    tag = "action",
    content = "params",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum DestructiveActionParams {
    CloseWorkspace(schema::WorkspaceTarget),
    CloseTab(schema::TabTarget),
    ClosePane(PaneTarget),
    RemoveWorktree(schema::WorktreeRemoveParams),
    RemovePeer(schema::PeerTarget),
    DiscardRecovery(schema::TerminalRecoveryTarget),
}

impl DestructiveActionParams {
    fn into_method(self) -> Method {
        match self {
            Self::CloseWorkspace(params) => Method::WorkspaceClose(params),
            Self::CloseTab(params) => Method::TabClose(params),
            Self::ClosePane(params) => Method::PaneClose(params),
            Self::RemoveWorktree(params) => Method::WorktreeRemove(params),
            Self::RemovePeer(params) => Method::PeerUnregister(params),
            Self::DiscardRecovery(params) => Method::TerminalRecoveryDiscard(params),
        }
    }
}

struct PresenceLifecycle {
    client_id: String,
    started: AtomicBool,
    stopping: AtomicBool,
    wake: Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone)]
struct DesktopMcp {
    api: ApiAdapter,
    queue: Arc<QueueManager>,
    session: Arc<str>,
    access_mode: AccessMode,
    presence: Arc<PresenceLifecycle>,
}

impl DesktopMcp {
    fn new(access_mode: AccessMode) -> Result<Self, BridgeError> {
        let session = crate::session::active_name()
            .unwrap_or_else(|| crate::session::DEFAULT_SESSION_NAME.to_string());
        let socket_path = crate::session::active_api_socket_path();
        let queue = QueueManager::for_active_session().map_err(queue_error)?;
        let client_id = random_control_client_id()?;
        Ok(Self {
            api: ApiAdapter::new(socket_path),
            queue: Arc::new(queue),
            session: Arc::from(session),
            access_mode,
            presence: Arc::new(PresenceLifecycle {
                client_id,
                started: AtomicBool::new(false),
                stopping: AtomicBool::new(false),
                wake: Notify::new(),
                task: Mutex::new(None),
            }),
        })
    }

    async fn queue_create_channel(
        &self,
        name: String,
        cwd: PathBuf,
    ) -> Result<Arc<ChannelHandle>, BridgeError> {
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || queue.create_channel(&name, &cwd))
            .await
            .map_err(join_error)?
            .map_err(queue_error)
            .map(Arc::new)
    }

    async fn queue_abort_channel(&self, handle: Arc<ChannelHandle>) {
        let queue = self.queue.clone();
        let _ = tokio::task::spawn_blocking(move || queue.abort_empty_channel(&handle)).await;
    }

    async fn queue_activate_channel(
        &self,
        handle: Arc<ChannelHandle>,
        identity: CliIdentity,
    ) -> Result<CliIdentity, BridgeError> {
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || queue.activate_channel(&handle, identity))
            .await
            .map_err(join_error)?
            .map_err(queue_error)
    }

    async fn queue_activate_reconciled(
        &self,
        cli_id: String,
        launch_marker: String,
        identity: CliIdentity,
    ) -> Result<CliIdentity, BridgeError> {
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || {
            queue.activate_reconciled(&cli_id, &launch_marker, identity)
        })
        .await
        .map_err(join_error)?
        .map_err(queue_error)
    }

    async fn queue_lookup(&self, cli_id: String) -> Result<CliIdentity, BridgeError> {
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || queue.lookup_cli(&cli_id))
            .await
            .map_err(join_error)?
            .map_err(queue_error)
    }

    async fn queue_mark_closed(&self, cli_id: String) -> Result<CliIdentity, BridgeError> {
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || queue.mark_closed(&cli_id))
            .await
            .map_err(join_error)?
            .map_err(queue_error)
    }

    async fn queue_prune_stale_creating(
        &self,
        agents: &[AgentInfo],
    ) -> Result<HashSet<String>, BridgeError> {
        let live_markers = agents
            .iter()
            .filter_map(|agent| agent.name.clone())
            .collect::<HashSet<_>>();
        let queue = self.queue.clone();
        tokio::task::spawn_blocking(move || queue.prune_stale_creating(&live_markers))
            .await
            .map_err(join_error)?
            .map(|cli_ids| cli_ids.into_iter().collect())
            .map_err(queue_error)
    }

    async fn mark_closed_best_effort(&self, cli_id: String) {
        let _ = self.queue_mark_closed(cli_id).await;
    }

    async fn list_local_agents(&self) -> Result<Vec<AgentInfo>, BridgeError> {
        match self
            .api
            .request(
                Method::AgentList(AgentListParams {
                    include_peers: false,
                }),
                API_TIMEOUT,
            )
            .await?
        {
            ResponseResult::AgentList { agents } => Ok(agents),
            result => Err(unexpected_result("agent.list", result)),
        }
    }

    async fn rename_agent_best_effort(&self, pane_id: &str, name: &str) {
        let result = self
            .api
            .request(
                Method::AgentRename(AgentRenameParams {
                    target: pane_id.to_string(),
                    name: Some(name.to_string()),
                }),
                API_TIMEOUT,
            )
            .await;
        match result {
            Ok(ResponseResult::AgentInfo { .. }) => {}
            Ok(result) => tracing::warn!(
                ?result,
                pane_id,
                "Herdr MCP could not restore the requested CLI display name"
            ),
            Err(err) => tracing::warn!(
                code = %err.code,
                message = %err.message,
                pane_id,
                "Herdr MCP could not restore the requested CLI display name"
            ),
        }
    }

    async fn reconcile_creating(&self, creating: CliIdentity) -> Result<CliIdentity, BridgeError> {
        let marker = creating.launch_marker.clone().ok_or_else(|| {
            BridgeError::new(
                "cli_not_running",
                "the CLI launch record cannot be reconciled",
                false,
            )
        })?;
        self.api.require_compatible().await?;
        let agents = self.list_local_agents().await?;
        let mut matches = agents
            .iter()
            .filter(|agent| agent.peer.is_none() && agent.name.as_deref() == Some(&marker))
            .cloned();
        let agent = matches.next();
        if matches.next().is_some() {
            return Err(BridgeError::new(
                "cli_identity_mismatch",
                "multiple panes matched the CLI launch marker",
                false,
            ));
        }
        let pruned = self.queue_prune_stale_creating(&agents).await?;
        let Some(agent) = agent else {
            let confirmed_absent = pruned.contains(&creating.cli_id);
            return Err(BridgeError::new(
                "cli_not_running",
                if confirmed_absent {
                    "the stale CLI launch was confirmed absent"
                } else {
                    "the CLI launch is still pending reconciliation"
                },
                !confirmed_absent,
            )
            .with_details(json!({
                "cli_id": creating.cli_id,
                "outcome_unknown": !confirmed_absent,
                "confirmed_absent": confirmed_absent,
            })));
        };
        let identity = CliIdentity {
            cli_id: creating.cli_id.clone(),
            pane_id: agent.pane_id.clone(),
            terminal_id: agent.terminal_id,
            workspace_id: agent.workspace_id,
            tab_id: agent.tab_id,
            name: creating.name.clone(),
            cwd: creating.cwd,
            state: CliState::Active,
            launch_marker: Some(marker.clone()),
        };
        let identity = self
            .queue_activate_reconciled(creating.cli_id, marker, identity)
            .await?;
        self.rename_agent_best_effort(&agent.pane_id, &creating.name)
            .await;
        Ok(identity)
    }

    async fn active_identity(&self, cli_id: &str) -> Result<CliIdentity, BridgeError> {
        validate_opaque_id("cli_id", cli_id)?;
        let identity = self.queue_lookup(cli_id.to_string()).await?;
        match &identity.state {
            CliState::Active => Ok(identity),
            CliState::Creating => self.reconcile_creating(identity).await,
            CliState::Closed => Err(BridgeError::new(
                "cli_not_running",
                "the registered CLI is no longer running",
                false,
            )),
        }
    }

    async fn preflight_identity(&self, cli_id: &str) -> Result<CliIdentity, BridgeError> {
        let identity = self.active_identity(cli_id).await?;
        self.api.require_compatible().await?;
        let result = self
            .api
            .request(
                Method::PaneGet(PaneTarget {
                    pane_id: identity.pane_id.clone(),
                }),
                API_TIMEOUT,
            )
            .await;
        let pane = match result {
            Ok(ResponseResult::PaneInfo { pane }) => pane,
            Ok(result) => return Err(unexpected_result("pane.get", result)),
            Err(err) if err.code == "pane_not_found" => {
                self.mark_closed_best_effort(identity.cli_id.clone()).await;
                return Err(BridgeError::new(
                    "cli_not_running",
                    "the registered CLI pane no longer exists",
                    false,
                ));
            }
            Err(err) => return Err(err),
        };
        if pane.terminal_id != identity.terminal_id {
            self.mark_closed_best_effort(identity.cli_id.clone()).await;
            return Err(BridgeError::new(
                "cli_identity_mismatch",
                "the pane now belongs to a different terminal",
                false,
            )
            .with_details(json!({
                "cli_id": identity.cli_id,
                "pane_id": identity.pane_id,
            })));
        }
        Ok(identity)
    }
}

fn validate_nonempty(field: &str, value: &str, max_bytes: usize) -> Result<(), ErrorData> {
    if value.trim().is_empty() {
        return Err(invalid_params(format!("{field} must not be empty")));
    }
    if value.len() > max_bytes {
        return Err(invalid_params(format!(
            "{field} must be at most {max_bytes} bytes"
        )));
    }
    if value.contains('\0') {
        return Err(invalid_params(format!("{field} must not contain NUL")));
    }
    Ok(())
}

fn validate_opaque_id(field: &str, value: &str) -> Result<(), BridgeError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(BridgeError::new(
            "cli_not_running",
            format!("the registered {field} is invalid"),
            false,
        ));
    }
    Ok(())
}

fn validate_queue_id(field: &str, value: &str) -> Result<(), ErrorData> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(invalid_params(format!("{field} is invalid")));
    }
    Ok(())
}

fn validate_pane_id(value: &str) -> Result<(), ErrorData> {
    validate_nonempty("pane_id", value, 512)
}

fn validate_launch(params: &LaunchCliParams) -> Result<PathBuf, ErrorData> {
    validate_nonempty("name", &params.name, MAX_NAME_BYTES)?;
    if params.argv.is_empty() || params.argv.len() > MAX_ARGV {
        return Err(invalid_params(format!(
            "argv must contain between 1 and {MAX_ARGV} elements"
        )));
    }
    if params.argv[0].is_empty() {
        return Err(invalid_params("argv[0] must not be empty"));
    }
    if params.argv[0].contains('/') {
        let executable = Path::new(&params.argv[0]);
        if !executable.is_absolute() {
            return Err(invalid_params(
                "argv[0] paths must be absolute; use a bare executable name for PATH lookup",
            ));
        }
        if !executable.is_file() {
            return Err(invalid_params(
                "an absolute argv[0] must name an existing file",
            ));
        }
    }
    if params.argv.iter().any(|arg| arg.contains('\0')) {
        return Err(invalid_params("argv must not contain NUL"));
    }
    let argv_bytes = params
        .argv
        .iter()
        .try_fold(0usize, |total, value| total.checked_add(value.len() + 1))
        .ok_or_else(|| invalid_params("argv is too large"))?;
    if argv_bytes > MAX_ARGV_BYTES {
        return Err(invalid_params(format!(
            "argv must be at most {MAX_ARGV_BYTES} bytes"
        )));
    }

    if params.cwd.contains('\0') {
        return Err(invalid_params("cwd must not contain NUL"));
    }
    let cwd = PathBuf::from(&params.cwd);
    if !cwd.is_absolute() {
        return Err(invalid_params("cwd must be an absolute path"));
    }
    if !cwd.is_dir() {
        return Err(invalid_params("cwd must be an existing directory"));
    }

    if params.env.len() > MAX_ENV_ENTRIES {
        return Err(invalid_params(format!(
            "env must contain at most {MAX_ENV_ENTRIES} entries"
        )));
    }
    let mut env_bytes = 0usize;
    for (key, value) in &params.env {
        if key.is_empty()
            || key.len() > MAX_ENV_KEY_BYTES
            || key.contains('=')
            || key.contains('\0')
        {
            return Err(invalid_params(format!("invalid environment key {key:?}")));
        }
        if key.starts_with("HERDR_") {
            return Err(invalid_params(
                "reserved HERDR_* environment variables cannot be overridden",
            ));
        }
        if value.len() > MAX_ENV_VALUE_BYTES || value.contains('\0') {
            return Err(invalid_params(format!(
                "environment value for {key:?} is invalid or too large"
            )));
        }
        env_bytes = env_bytes
            .checked_add(key.len() + value.len() + 1)
            .ok_or_else(|| invalid_params("env is too large"))?;
    }
    if env_bytes > MAX_ENV_BYTES {
        return Err(invalid_params(format!(
            "env must be at most {MAX_ENV_BYTES} bytes"
        )));
    }

    if let Some(workspace_id) = &params.workspace_id {
        validate_nonempty("workspace_id", workspace_id, 256)?;
    }
    if let Some(tab_id) = &params.tab_id {
        validate_nonempty("tab_id", tab_id, 256)?;
    }
    Ok(cwd)
}

fn validate_send_input(params: &SendInputParams) -> Result<(), ErrorData> {
    validate_queue_id("cli_id", &params.cli_id)?;
    match (&params.text, &params.keys) {
        (Some(text), None) => {
            if text.is_empty() {
                return Err(invalid_params("text must not be empty"));
            }
            if text.len() > MAX_INPUT_BYTES {
                return Err(invalid_params(format!(
                    "text must be at most {MAX_INPUT_BYTES} bytes"
                )));
            }
        }
        (None, Some(keys)) => {
            if keys.is_empty() || keys.len() > MAX_KEYS {
                return Err(invalid_params(format!(
                    "keys must contain between 1 and {MAX_KEYS} tokens"
                )));
            }
            if keys
                .iter()
                .any(|key| key.is_empty() || key.len() > 64 || key.contains('\0'))
            {
                return Err(invalid_params(
                    "each key token must contain 1 to 64 bytes without NUL",
                ));
            }
        }
        _ => {
            return Err(invalid_params("provide exactly one of text or keys"));
        }
    }
    Ok(())
}

fn validate_control_method(method: &Method) -> Result<(), ErrorData> {
    match method {
        Method::PaneRead(params) => {
            validate_pane_id(&params.pane_id)?;
            if params
                .lines
                .is_some_and(|lines| !(1..=1000).contains(&lines))
            {
                return Err(invalid_params("lines must be between 1 and 1000"));
            }
        }
        Method::AgentRead(params) => {
            validate_nonempty("target", &params.target, 512)?;
            if params
                .lines
                .is_some_and(|lines| !(1..=1000).contains(&lines))
            {
                return Err(invalid_params("lines must be between 1 and 1000"));
            }
        }
        Method::PaneWaitForOutput(params) => {
            validate_pane_id(&params.pane_id)?;
            if params
                .lines
                .is_some_and(|lines| !(1..=1000).contains(&lines))
            {
                return Err(invalid_params("lines must be between 1 and 1000"));
            }
        }
        Method::PaneSendInput(params) => validate_direct_pane_input(params)?,
        Method::AgentSend(params) => {
            validate_nonempty("target", &params.target, 512)?;
            validate_nonempty("text", &params.text, MAX_INPUT_BYTES)?;
        }
        _ => {}
    }
    Ok(())
}

fn validate_direct_pane_input(params: &PaneSendInputParams) -> Result<(), ErrorData> {
    validate_pane_id(&params.pane_id)?;
    if params.text.is_empty() && params.keys.is_empty() {
        return Err(invalid_params("provide text, keys, or both"));
    }
    if params.text.len() > MAX_INPUT_BYTES || params.text.contains('\0') {
        return Err(invalid_params(format!(
            "text must be at most {MAX_INPUT_BYTES} bytes without NUL"
        )));
    }
    if params.keys.len() > MAX_KEYS
        || params
            .keys
            .iter()
            .any(|key| key.is_empty() || key.len() > 64 || key.contains('\0'))
    {
        return Err(invalid_params(format!(
            "keys must contain at most {MAX_KEYS} tokens of 1 to 64 bytes without NUL"
        )));
    }
    Ok(())
}

fn validate_local_agent_target(target: &str) -> Result<(), ErrorData> {
    validate_nonempty("target", target, 512)?;
    if target.contains("::") {
        return Err(invalid_params(
            "local agent actions do not accept peer-qualified targets",
        ));
    }
    Ok(())
}

fn validate_remote_agent_target(target: &str) -> Result<(), ErrorData> {
    validate_nonempty("target", target, 512)?;
    let Some((peer, agent)) = target.split_once("::") else {
        return Err(invalid_params(
            "remote agent targets must use the peer::agent form",
        ));
    };
    validate_nonempty("peer", peer, 256)?;
    validate_nonempty("agent", agent, 256)
}

fn validate_output_match(output_match: &schema::OutputMatch) -> Result<(), ErrorData> {
    let value = match output_match {
        schema::OutputMatch::Substring { value } | schema::OutputMatch::Regex { value } => value,
    };
    validate_nonempty("match value", value, 8 * 1024)
}

fn normalize_wait_timeout(timeout_ms: Option<u64>) -> Result<u64, ErrorData> {
    let timeout_ms = timeout_ms.unwrap_or(DEFAULT_WAIT_MS);
    if !(1..=MAX_WAIT_MS).contains(&timeout_ms) {
        return Err(invalid_params(format!(
            "timeout_ms must be between 1 and {MAX_WAIT_MS}"
        )));
    }
    Ok(timeout_ms)
}

fn validate_agent_start(params: &AgentStartParams, remote: bool) -> Result<(), ErrorData> {
    validate_nonempty("name", &params.name, MAX_NAME_BYTES)?;
    if remote {
        let peer = params
            .peer
            .as_deref()
            .ok_or_else(|| invalid_params("remote agent start requires peer"))?;
        validate_nonempty("peer", peer, 256)?;
        if params.focus {
            return Err(invalid_params("remote agent start does not support focus"));
        }
    } else if params.peer.is_some() {
        return Err(invalid_params(
            "local agent start does not accept a peer; use herdr_peer_control",
        ));
    }
    if params.transport.is_some() {
        return Err(invalid_params(
            "MCP agent start does not accept a transport override",
        ));
    }
    if params.argv.is_empty() || params.argv.len() > MAX_ARGV {
        return Err(invalid_params(format!(
            "argv must contain between 1 and {MAX_ARGV} elements"
        )));
    }
    if params.argv[0].is_empty() || params.argv.iter().any(|arg| arg.contains('\0')) {
        return Err(invalid_params(
            "argv entries must be nonempty and contain no NUL",
        ));
    }
    let argv_bytes = params
        .argv
        .iter()
        .try_fold(0usize, |total, value| total.checked_add(value.len() + 1))
        .ok_or_else(|| invalid_params("argv is too large"))?;
    if argv_bytes > MAX_ARGV_BYTES {
        return Err(invalid_params(format!(
            "argv must be at most {MAX_ARGV_BYTES} bytes"
        )));
    }
    if let Some(cwd) = &params.cwd {
        validate_nonempty("cwd", cwd, 4096)?;
        if !remote {
            let path = Path::new(cwd);
            if !path.is_absolute() || !path.is_dir() {
                return Err(invalid_params(
                    "local cwd must be an existing absolute directory",
                ));
            }
        }
    }
    if params.env.len() > MAX_ENV_ENTRIES {
        return Err(invalid_params(format!(
            "env must contain at most {MAX_ENV_ENTRIES} entries"
        )));
    }
    let mut env_bytes = 0usize;
    for (key, value) in &params.env {
        if key.is_empty()
            || key.len() > MAX_ENV_KEY_BYTES
            || key.contains('=')
            || key.contains('\0')
            || key.starts_with("HERDR_")
            || value.len() > MAX_ENV_VALUE_BYTES
            || value.contains('\0')
        {
            return Err(invalid_params(format!(
                "environment entry {key:?} is invalid or reserved"
            )));
        }
        env_bytes = env_bytes
            .checked_add(key.len() + value.len() + 1)
            .ok_or_else(|| invalid_params("env is too large"))?;
    }
    if env_bytes > MAX_ENV_BYTES {
        return Err(invalid_params(format!(
            "env must be at most {MAX_ENV_BYTES} bytes"
        )));
    }
    Ok(())
}

fn random_control_client_id() -> Result<String, BridgeError> {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|err| {
        BridgeError::new(
            "internal_error",
            format!("failed to generate MCP client id: {err}"),
            false,
        )
    })?;
    let mut id = String::with_capacity(4 + random.len() * 2);
    id.push_str("mcp_");
    for byte in random {
        use std::fmt::Write as _;
        write!(&mut id, "{byte:02x}").map_err(|err| {
            BridgeError::new(
                "internal_error",
                format!("failed to encode MCP client id: {err}"),
                false,
            )
        })?;
    }
    Ok(id)
}

fn truncate_utf8_tail(text: &mut String, max_bytes: usize) -> bool {
    if text.len() <= max_bytes {
        return false;
    }
    let mut start = text.len() - max_bytes;
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    *text = text[start..].to_string();
    true
}

fn pane_read_tool_result(read: &PaneReadResult) -> Result<CallToolResult, BridgeError> {
    value_or_internal(read).map(|value| success(json!({ "read": value })))
}

fn bounded_text_read_result(
    read: &mut PaneReadResult,
    max_bytes: usize,
) -> Result<Option<CallToolResult>, BridgeError> {
    let original = read.text.clone();
    let mut boundaries = original
        .char_indices()
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    boundaries.push(original.len());

    let mut low = 0usize;
    let mut high = boundaries.len();
    let mut best = None;
    while low < high {
        let middle = low + (high - low) / 2;
        read.text = original[boundaries[middle]..].to_string();
        let result = pane_read_tool_result(read)?;
        if serialized_tool_result_len(&result)? <= max_bytes {
            best = Some(result);
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    if best.is_none() && low < boundaries.len() {
        read.text = original[boundaries[low]..].to_string();
        let result = pane_read_tool_result(read)?;
        if serialized_tool_result_len(&result)? <= max_bytes {
            best = Some(result);
        }
    }
    Ok(best)
}

fn drain_tool_result(drain: &DrainResult) -> CallToolResult {
    let messages = drain
        .messages
        .iter()
        .map(|message| {
            json!({
                "id": message.message_id,
                "sequence": message.sequence,
                "cli_id": message.cli_id,
                "created_at_unix_ms": message.created_at_unix_ms,
                "kind": message.kind,
                "correlation_id": message.correlation_id,
                "text": message.text,
            })
        })
        .collect::<Vec<_>>();
    success(json!({
        "lease_id": drain.lease_id,
        "lease_expires_at_unix_ms": drain.lease_expires_at_unix_ms,
        "messages": messages,
        "remaining": drain.remaining,
        "quarantined": drain.quarantined,
    }))
}

fn launch_tool_result(identity: &CliIdentity) -> CallToolResult {
    success(json!({
        "cli_id": identity.cli_id,
        "pane_id": identity.pane_id,
        "terminal_id": identity.terminal_id,
        "workspace_id": identity.workspace_id,
        "tab_id": identity.tab_id,
        "name": identity.name,
        "cwd": identity.cwd,
    }))
}

fn value_or_internal<T: serde::Serialize>(value: T) -> Result<Value, BridgeError> {
    serde_json::to_value(value).map_err(|err| {
        BridgeError::new(
            "internal_error",
            format!("failed to encode bridge result: {err}"),
            false,
        )
    })
}

fn is_ok_result(result: &ResponseResult) -> bool {
    matches!(result, ResponseResult::Ok { .. })
}

impl DesktopMcp {
    async fn dispatch_control_method(
        &self,
        method: Method,
        timeout: Duration,
    ) -> Result<CallToolResult, ErrorData> {
        if let Err(err) = self.api.require_compatible().await {
            return Ok(err.result());
        }
        let max_bytes = if matches!(
            &method,
            Method::PaneRead(_) | Method::AgentRead(_) | Method::PaneWaitForOutput(_)
        ) {
            PANE_READ_MAX_BYTES
        } else {
            CONTROL_RESULT_MAX_BYTES
        };
        let result = match self.api.request(method, timeout).await {
            Ok(result) => result,
            Err(err) => return Ok(err.result()),
        };
        let value = match value_or_internal(result) {
            Ok(value) => value,
            Err(err) => return Ok(err.result()),
        };
        let response = success(value);
        let encoded_len = match serialized_tool_result_len(&response) {
            Ok(encoded_len) => encoded_len,
            Err(err) => return Ok(err.result()),
        };
        if encoded_len > max_bytes {
            return Ok(BridgeError::new(
                "result_too_large",
                "the control result exceeds the MCP bridge limit",
                false,
            )
            .with_details(json!({
                "actual_bytes": encoded_len,
                "max_bytes": max_bytes,
            }))
            .result());
        }
        Ok(response)
    }

    fn start_presence_loop(&self) {
        if self
            .presence
            .started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let service = self.clone();
        let task = tokio::spawn(async move {
            service.run_presence_loop().await;
        });
        match self.presence.task.lock() {
            Ok(mut slot) => *slot = Some(task),
            Err(_) => task.abort(),
        }
    }

    async fn run_presence_loop(&self) {
        let mut registered = false;
        while !self.presence.stopping.load(Ordering::SeqCst) {
            let method = if registered {
                Method::ControlClientHeartbeat(schema::ControlClientTarget {
                    client_id: self.presence.client_id.clone(),
                })
            } else {
                Method::ControlClientRegister(schema::ControlClientRegisterParams {
                    client_id: self.presence.client_id.clone(),
                    kind: schema::ControlClientKind::Mcp,
                    access_mode: self.access_mode.schema_value(),
                })
            };
            registered = matches!(
                self.api.request(method, HEALTH_TIMEOUT).await,
                Ok(ResponseResult::ControlClientStatus { .. })
            );

            tokio::select! {
                () = tokio::time::sleep(PRESENCE_HEARTBEAT_INTERVAL) => {}
                () = self.presence.wake.notified() => {}
            }
        }

        let _ = self
            .api
            .request(
                Method::ControlClientUnregister(schema::ControlClientTarget {
                    client_id: self.presence.client_id.clone(),
                }),
                HEALTH_TIMEOUT,
            )
            .await;
    }

    async fn shutdown_presence(&self) {
        self.presence.stopping.store(true, Ordering::SeqCst);
        self.presence.wake.notify_waiters();
        let task = self
            .presence
            .task
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

#[tool_router]
impl DesktopMcp {
    #[tool(
        name = "herdr_health",
        description = "Report bridge and selected Herdr session compatibility without starting Herdr.",
        input_schema = tool_input_schema::<NoParams>(),
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_health(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let _: NoParams = parse_params(arguments)?;
        let base = json!({
            "mcp_version": BRIDGE_VERSION,
            "queue_schema": QUEUE_SCHEMA_VERSION,
            "selected_session": self.session.as_ref(),
            "access_mode": self.access_mode.wire_name(),
            "binary_version": crate::build_info::version(),
            "required_protocol": REQUIRED_PROTOCOL,
        });
        let mut data = base;
        match self.api.ping().await {
            Ok(status) => {
                data["herdr_available"] = json!(true);
                data["herdr_status"] = json!(if status.protocol == REQUIRED_PROTOCOL {
                    "available"
                } else {
                    "incompatible"
                });
                data["herdr_version"] = json!(status.version);
                data["herdr_protocol"] = json!(status.protocol);
            }
            Err(err) => {
                data["herdr_available"] = json!(false);
                data["herdr_status"] = json!("unavailable");
                data["availability_error"] = json!({
                    "code": err.code,
                    "message": err.message,
                    "retryable": err.retryable,
                });
            }
        }
        Ok(success(data))
    }

    #[tool(
        name = "herdr_inspect",
        description = "Inspect typed Herdr workspace, tab, pane, layout, agent, worktree, peer, and recovery state.",
        input_schema = tool_input_schema::<InspectParams>(),
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_inspect(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: InspectParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method()?, API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_wait",
        description = "Wait for a typed Herdr session event or pane-output match.",
        input_schema = tool_input_schema::<WaitParams>(),
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_wait(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: WaitParams = parse_control_params(arguments)?;
        let (method, timeout) = params.into_method()?;
        self.dispatch_control_method(method, timeout).await
    }

    #[tool(
        name = "herdr_workspace_control",
        description = "Create, focus, rename, or move a Herdr workspace.",
        input_schema = tool_input_schema::<WorkspaceControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_workspace_control(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: WorkspaceControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method(), API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_tab_control",
        description = "Create, focus, rename, or move a Herdr tab.",
        input_schema = tool_input_schema::<TabControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_tab_control(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: TabControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method(), API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_pane_control",
        description = "Split, move, focus, resize, rename, or send input to Herdr panes.",
        input_schema = tool_input_schema::<PaneControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_pane_control(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: PaneControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method()?, API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_layout_control",
        description = "Apply a Herdr layout or set a split ratio.",
        input_schema = tool_input_schema::<LayoutControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_layout_control(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: LayoutControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method(), API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_agent_control",
        description = "Send, rename, focus, or start a local Herdr agent.",
        input_schema = tool_input_schema::<AgentControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_agent_control(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: AgentControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method()?, LAUNCH_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_worktree_control",
        description = "Create or open a Herdr worktree workspace.",
        input_schema = tool_input_schema::<WorktreeControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_worktree_control(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: WorktreeControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method(), LAUNCH_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_peer_control",
        description = "Connect or disconnect SSH peers and control peer-qualified agents.",
        input_schema = tool_input_schema::<PeerControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_peer_control(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: PeerControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method()?, LAUNCH_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_recovery_control",
        description = "Retry one durable terminal recovery.",
        input_schema = tool_input_schema::<RecoveryControlParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_recovery_control(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: RecoveryControlParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method(), API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_destructive_action",
        description = "Close or permanently remove an explicitly identified Herdr resource.",
        input_schema = tool_input_schema::<DestructiveActionParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_destructive_action(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: DestructiveActionParams = parse_control_params(arguments)?;
        self.dispatch_control_method(params.into_method(), API_TIMEOUT)
            .await
    }

    #[tool(
        name = "herdr_snapshot",
        description = "Read the current snapshot of the selected Herdr session.",
        input_schema = tool_input_schema::<NoParams>(),
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_snapshot(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let _: NoParams = parse_params(arguments)?;
        if let Err(err) = self.api.require_compatible().await {
            return Ok(err.result());
        }
        let result = self
            .api
            .request(Method::SessionSnapshot(EmptyParams::default()), API_TIMEOUT)
            .await;
        let snapshot = match result {
            Ok(ResponseResult::SessionSnapshot { snapshot }) => snapshot,
            Ok(result) => return Ok(unexpected_result("session.snapshot", result).result()),
            Err(err) => return Ok(err.result()),
        };
        let value = match value_or_internal(snapshot) {
            Ok(value) => value,
            Err(err) => return Ok(err.result()),
        };
        let response = success(json!({ "snapshot": value }));
        let encoded_len = match serialized_tool_result_len(&response) {
            Ok(encoded_len) => encoded_len,
            Err(err) => return Ok(err.result()),
        };
        if encoded_len > SNAPSHOT_MAX_BYTES {
            return Ok(BridgeError::new(
                "result_too_large",
                "the session snapshot exceeds the 2 MiB bridge limit",
                false,
            )
            .with_details(json!({
                "actual_bytes": encoded_len,
                "max_bytes": SNAPSHOT_MAX_BYTES,
            }))
            .result());
        }
        Ok(response)
    }

    #[tool(
        name = "herdr_read_pane",
        description = "Read bounded visible or recent output from any pane in the selected session.",
        input_schema = tool_input_schema::<ReadPaneParams>(),
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_read_pane(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: ReadPaneParams = parse_params(arguments)?;
        validate_pane_id(&params.pane_id)?;
        if !(1..=1000).contains(&params.lines) {
            return Err(invalid_params("lines must be between 1 and 1000"));
        }
        if !(1..=PANE_READ_MAX_BYTES).contains(&params.max_bytes) {
            return Err(invalid_params(format!(
                "max_bytes must be between 1 and {PANE_READ_MAX_BYTES}"
            )));
        }
        if let Err(err) = self.api.require_compatible().await {
            return Ok(err.result());
        }
        let result = self
            .api
            .request(
                Method::PaneRead(PaneReadParams {
                    pane_id: params.pane_id,
                    source: params.source.into(),
                    lines: Some(params.lines),
                    format: params.format.into(),
                    strip_ansi: matches!(params.format, McpReadFormat::Text),
                }),
                API_TIMEOUT,
            )
            .await;
        let mut read = match result {
            Ok(ResponseResult::PaneRead { read }) => read,
            Ok(result) => return Ok(unexpected_result("pane.read", result).result()),
            Err(err) => return Ok(err.result()),
        };
        let initial = match pane_read_tool_result(&read) {
            Ok(result) => result,
            Err(err) => return Ok(err.result()),
        };
        let initial_len = match serialized_tool_result_len(&initial) {
            Ok(encoded_len) => encoded_len,
            Err(err) => return Ok(err.result()),
        };
        if initial_len <= params.max_bytes {
            return Ok(initial);
        }
        if matches!(params.format, McpReadFormat::Ansi) {
            return Ok(BridgeError::new(
                "result_too_large",
                "ANSI pane output exceeds max_bytes and cannot be safely truncated",
                false,
            )
            .with_details(json!({
                "actual_bytes": initial_len,
                "max_bytes": params.max_bytes,
            }))
            .result());
        }
        if truncate_utf8_tail(&mut read.text, params.max_bytes) {
            read.truncated = true;
        }
        read.truncated = true;
        match bounded_text_read_result(&mut read, params.max_bytes) {
            Ok(Some(result)) => Ok(result),
            Ok(None) => Ok(BridgeError::new(
                "result_too_large",
                "the pane result metadata alone exceeds max_bytes",
                false,
            )
            .with_details(json!({
                "actual_bytes": initial_len,
                "max_bytes": params.max_bytes,
            }))
            .result()),
            Err(err) => Ok(err.result()),
        }
    }

    #[tool(
        name = "herdr_pane_process_info",
        description = "Read process metadata for any pane in the selected Herdr session.",
        input_schema = tool_input_schema::<PaneIdParams>(),
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_pane_process_info(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: PaneIdParams = parse_params(arguments)?;
        validate_pane_id(&params.pane_id)?;
        if let Err(err) = self.api.require_compatible().await {
            return Ok(err.result());
        }
        let result = self
            .api
            .request(
                Method::PaneProcessInfo(PaneProcessInfoParams {
                    pane_id: Some(params.pane_id),
                }),
                API_TIMEOUT,
            )
            .await;
        let process_info = match result {
            Ok(ResponseResult::PaneProcessInfo { process_info }) => process_info,
            Ok(result) => return Ok(unexpected_result("pane.process_info", result).result()),
            Err(err) => return Ok(err.result()),
        };
        match value_or_internal(process_info) {
            Ok(value) => Ok(success(json!({ "process_info": value }))),
            Err(err) => Ok(err.result()),
        }
    }

    #[tool(
        name = "herdr_launch_cli",
        description = "Launch an argv vector without a shell and register it for desktop control.",
        input_schema = tool_input_schema::<LaunchCliParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_launch_cli(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: LaunchCliParams = parse_params(arguments)?;
        let cwd = validate_launch(&params)?;
        if let Err(err) = self.api.require_compatible().await {
            return Ok(err.result());
        }
        let existing_agents = match self.list_local_agents().await {
            Ok(agents) => agents,
            Err(err) => return Ok(err.result()),
        };
        if let Err(err) = self.queue_prune_stale_creating(&existing_agents).await {
            return Ok(err.result());
        }
        if existing_agents
            .iter()
            .any(|agent| agent.name.as_deref() == Some(&params.name))
        {
            return Ok(BridgeError::new(
                "launch_rejected",
                format!("an agent named {:?} already exists", params.name),
                false,
            )
            .with_details(json!({ "upstream_code": "duplicate_agent_name" }))
            .result());
        }

        let handle = match self
            .queue_create_channel(params.name.clone(), cwd.clone())
            .await
        {
            Ok(handle) => handle,
            Err(err) => return Ok(err.result()),
        };
        let cli_id = handle.cli_id.clone();
        let launch_marker = handle.launch_marker.clone();
        let mut env = params.env;
        for (key, value) in handle.env.iter().cloned() {
            env.insert(key, value);
        }
        let launch_result = self
            .api
            .request(
                Method::AgentStart(AgentStartParams {
                    name: launch_marker.clone(),
                    peer: None,
                    agent: None,
                    cwd: Some(params.cwd.clone()),
                    workspace_id: params.workspace_id,
                    tab_id: params.tab_id,
                    split: Some(params.split.into()),
                    focus: false,
                    argv: params.argv,
                    env,
                    transport: None,
                }),
                LAUNCH_TIMEOUT,
            )
            .await;
        let agent = match launch_result {
            Ok(ResponseResult::AgentStarted { agent, .. }) => agent,
            Ok(result) => {
                self.queue_abort_channel(handle).await;
                return Ok(unexpected_result("agent.start", result).result());
            }
            Err(err) if matches!(err.code.as_str(), "server_timeout" | "server_unavailable") => {
                let upstream_code = err.code.clone();
                let mut details = json!({
                    "cli_id": cli_id,
                    "outcome_unknown": true,
                    "upstream_code": upstream_code,
                });
                if let Some(upstream_details) = err.details {
                    details["upstream_details"] = upstream_details;
                }
                match self.active_identity(&cli_id).await {
                    Ok(identity) => return Ok(launch_tool_result(&identity)),
                    Err(reconcile_err) => {
                        details["reconciliation"] = json!({
                            "code": reconcile_err.code,
                            "message": reconcile_err.message,
                            "retryable": reconcile_err.retryable,
                        });
                    }
                }
                return Ok(BridgeError::new("launch_rejected", err.message, true)
                    .with_details(details)
                    .result());
            }
            Err(err) => {
                self.queue_abort_channel(handle).await;
                return Ok(remap_rejection(err, "launch_rejected").result());
            }
        };

        let identity = CliIdentity {
            cli_id: cli_id.clone(),
            pane_id: agent.pane_id.clone(),
            terminal_id: agent.terminal_id.clone(),
            workspace_id: agent.workspace_id.clone(),
            tab_id: agent.tab_id.clone(),
            name: params.name,
            cwd: cwd.to_string_lossy().into_owned(),
            state: CliState::Active,
            launch_marker: Some(launch_marker),
        };
        let identity = match self.queue_activate_channel(handle.clone(), identity).await {
            Ok(identity) => identity,
            Err(err) => {
                let _ = self
                    .api
                    .request(
                        Method::PaneClose(PaneTarget {
                            pane_id: agent.pane_id,
                        }),
                        API_TIMEOUT,
                    )
                    .await;
                self.queue_abort_channel(handle).await;
                return Ok(err.result());
            }
        };
        self.rename_agent_best_effort(&identity.pane_id, &identity.name)
            .await;
        Ok(launch_tool_result(&identity))
    }

    #[tool(
        name = "herdr_send_input",
        description = "Send text or key tokens to a registered, still-matching CLI.",
        input_schema = tool_input_schema::<SendInputParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_send_input(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: SendInputParams = parse_params(arguments)?;
        validate_send_input(&params)?;
        let identity = match self.preflight_identity(&params.cli_id).await {
            Ok(identity) => identity,
            Err(err) => return Ok(err.result()),
        };
        let text_bytes = params.text.as_ref().map_or(0, String::len);
        let key_count = params.keys.as_ref().map_or(0, Vec::len);
        let result = self
            .api
            .request(
                Method::PaneSendInput(PaneSendInputParams {
                    pane_id: identity.pane_id,
                    text: params.text.unwrap_or_default(),
                    keys: params.keys.unwrap_or_default(),
                }),
                API_TIMEOUT,
            )
            .await;
        match result {
            Ok(result) if is_ok_result(&result) => Ok(success(json!({
                "cli_id": params.cli_id,
                "text_bytes": text_bytes,
                "key_count": key_count,
            }))),
            Ok(result) => Ok(unexpected_result("pane.send_input", result).result()),
            Err(err) if err.code == "pane_not_found" => {
                self.mark_closed_best_effort(params.cli_id).await;
                Ok(BridgeError::new(
                    "cli_not_running",
                    "the registered CLI pane no longer exists",
                    false,
                )
                .result())
            }
            Err(err) if err.code == "invalid_key" => Err(invalid_params(err.message)),
            Err(err) => Ok(remap_rejection(err, "input_rejected").result()),
        }
    }

    #[tool(
        name = "herdr_interrupt_cli",
        description = "Send Ctrl-C to a registered, still-matching CLI.",
        input_schema = tool_input_schema::<CliIdParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_interrupt_cli(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: CliIdParams = parse_params(arguments)?;
        validate_queue_id("cli_id", &params.cli_id)?;
        let identity = match self.preflight_identity(&params.cli_id).await {
            Ok(identity) => identity,
            Err(err) => return Ok(err.result()),
        };
        let result = self
            .api
            .request(
                Method::PaneSendInput(PaneSendInputParams {
                    pane_id: identity.pane_id,
                    text: String::new(),
                    keys: vec!["ctrl+c".to_string()],
                }),
                API_TIMEOUT,
            )
            .await;
        match result {
            Ok(result) if is_ok_result(&result) => Ok(success(
                json!({ "cli_id": params.cli_id, "interrupted": true }),
            )),
            Ok(result) => Ok(unexpected_result("pane.send_input", result).result()),
            Err(err) if err.code == "pane_not_found" => {
                self.mark_closed_best_effort(params.cli_id).await;
                Ok(BridgeError::new(
                    "cli_not_running",
                    "the registered CLI pane no longer exists",
                    false,
                )
                .result())
            }
            Err(err) => Ok(remap_rejection(err, "input_rejected").result()),
        }
    }

    #[tool(
        name = "herdr_stop_cli",
        description = "Close a registered CLI pane without affecting an identity-replaced pane.",
        input_schema = tool_input_schema::<CliIdParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_stop_cli(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: CliIdParams = parse_params(arguments)?;
        validate_queue_id("cli_id", &params.cli_id)?;
        let registered = match self.queue_lookup(params.cli_id.clone()).await {
            Ok(identity) => identity,
            Err(err) => return Ok(err.result()),
        };
        if matches!(&registered.state, CliState::Closed) {
            return Ok(success(json!({
                "cli_id": params.cli_id,
                "stopped": true,
                "already_stopped": true,
            })));
        }
        let identity = match self.active_identity(&params.cli_id).await {
            Ok(identity) => identity,
            Err(err) if err.is_confirmed_absent() => {
                return Ok(success(json!({
                    "cli_id": params.cli_id,
                    "stopped": true,
                    "already_stopped": true,
                })));
            }
            Err(err) => return Ok(err.result()),
        };
        if let Err(err) = self.api.require_compatible().await {
            return Ok(err.result());
        }
        let pane_result = self
            .api
            .request(
                Method::PaneGet(PaneTarget {
                    pane_id: identity.pane_id.clone(),
                }),
                API_TIMEOUT,
            )
            .await;
        let pane = match pane_result {
            Ok(ResponseResult::PaneInfo { pane }) => pane,
            Ok(result) => return Ok(unexpected_result("pane.get", result).result()),
            Err(err) if err.code == "pane_not_found" => {
                if let Err(queue_err) = self.queue_mark_closed(params.cli_id.clone()).await {
                    return Ok(queue_err.result());
                }
                return Ok(success(json!({
                    "cli_id": params.cli_id,
                    "stopped": true,
                    "already_stopped": true,
                })));
            }
            Err(err) => return Ok(remap_rejection(err, "stop_rejected").result()),
        };
        if pane.terminal_id != identity.terminal_id {
            self.mark_closed_best_effort(params.cli_id).await;
            return Ok(BridgeError::new(
                "cli_identity_mismatch",
                "the pane now belongs to a different terminal and was not closed",
                false,
            )
            .result());
        }

        let close_result = self
            .api
            .request(
                Method::PaneClose(PaneTarget {
                    pane_id: identity.pane_id,
                }),
                API_TIMEOUT,
            )
            .await;
        let already_stopped = match close_result {
            Ok(result) if is_ok_result(&result) => false,
            Ok(result) => return Ok(unexpected_result("pane.close", result).result()),
            Err(err) if err.code == "pane_not_found" => true,
            Err(err) => return Ok(remap_rejection(err, "stop_rejected").result()),
        };
        if let Err(err) = self.queue_mark_closed(params.cli_id.clone()).await {
            return Ok(err.result());
        }
        Ok(success(json!({
            "cli_id": params.cli_id,
            "stopped": true,
            "already_stopped": already_stopped,
        })))
    }

    #[tool(
        name = "herdr_drain_messages",
        description = "Lease queued desktop messages from one registered CLI in FIFO order.",
        input_schema = tool_input_schema::<DrainMessagesParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn herdr_drain_messages(
        &self,
        arguments: JsonObject,
    ) -> Result<CallToolResult, ErrorData> {
        let params: DrainMessagesParams = parse_params(arguments)?;
        validate_queue_id("cli_id", &params.cli_id)?;
        if !(1..=MAX_DRAIN_LIMIT).contains(&params.limit) {
            return Err(invalid_params(format!(
                "limit must be between 1 and {MAX_DRAIN_LIMIT}"
            )));
        }
        let cli_id = params.cli_id;
        let queue = self.queue.clone();
        let drain_cli_id = cli_id.clone();
        let drain = match tokio::task::spawn_blocking(move || {
            queue.drain_bounded(&drain_cli_id, params.limit, |candidate| {
                serialized_tool_result_len(&drain_tool_result(candidate))
                    .is_ok_and(|encoded_len| encoded_len <= QUEUE_DRAIN_MAX_BYTES)
            })
        })
        .await
        .map_err(join_error)
        {
            Ok(Ok(drain)) => drain,
            Ok(Err(err)) => return Ok(queue_error(err).result()),
            Err(err) => return Ok(err.result()),
        };
        let response = drain_tool_result(&drain);
        let encoded_len = match serialized_tool_result_len(&response) {
            Ok(encoded_len) => encoded_len,
            Err(err) => {
                let queue = self.queue.clone();
                let lease_id = drain.lease_id.clone();
                let _ =
                    tokio::task::spawn_blocking(move || queue.release(&cli_id, &lease_id)).await;
                return Ok(err.result());
            }
        };
        if encoded_len > QUEUE_DRAIN_MAX_BYTES {
            let queue = self.queue.clone();
            let lease_id = drain.lease_id.clone();
            let _ = tokio::task::spawn_blocking(move || queue.release(&cli_id, &lease_id)).await;
            return Ok(BridgeError::new(
                "result_too_large",
                "the drained message lease exceeds the 1 MiB bridge limit",
                false,
            )
            .with_details(json!({
                "actual_bytes": encoded_len,
                "max_bytes": QUEUE_DRAIN_MAX_BYTES,
            }))
            .result());
        }
        Ok(response)
    }

    #[tool(
        name = "herdr_ack_messages",
        description = "Acknowledge a message lease and permanently remove its messages.",
        input_schema = tool_input_schema::<AckMessagesParams>(),
        annotations(
            read_only_hint = false,
            destructive_hint = true,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn herdr_ack_messages(&self, arguments: JsonObject) -> Result<CallToolResult, ErrorData> {
        let params: AckMessagesParams = parse_params(arguments)?;
        validate_queue_id("cli_id", &params.cli_id)?;
        validate_queue_id("lease_id", &params.lease_id)?;
        let cli_id = params.cli_id;
        let lease_id = params.lease_id;
        let queue = self.queue.clone();
        let ack = match tokio::task::spawn_blocking(move || queue.ack(&cli_id, &lease_id))
            .await
            .map_err(join_error)
        {
            Ok(Ok(ack)) => ack,
            Ok(Err(err)) => return Ok(queue_error(err).result()),
            Err(err) => return Ok(err.result()),
        };
        Ok(success(json!({
            "lease_id": ack.lease_id,
            "already_acked": ack.already_acked,
            "deleted": ack.deleted,
        })))
    }
}

#[rmcp::tool_handler(router = Self::tool_router())]
impl rmcp::ServerHandler for DesktopMcp {
    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        if !self.access_mode.allows_tool(request.name.as_ref())
            && FULL_CONTROL_TOOL_NAMES.contains(&request.name.as_ref())
        {
            return Ok(BridgeError::new(
                "full_control_required",
                "restart the MCP bridge with --full-control to use this tool",
                false,
            )
            .result());
        }
        let context = rmcp::handler::server::tool::ToolCallContext::new(self, request, context);
        Self::tool_router().call(context).await
    }

    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, ErrorData> {
        let mut tools = Self::tool_router().list_all();
        tools.retain(|tool| self.access_mode.allows_tool(tool.name.as_ref()));
        Ok(rmcp::model::ListToolsResult {
            tools,
            ..Default::default()
        })
    }

    async fn on_initialized(&self, _context: rmcp::service::NotificationContext<rmcp::RoleServer>) {
        self.start_presence_loop();
    }
}

pub(crate) fn run_stdio(access_mode: AccessMode) -> io::Result<i32> {
    let service = DesktopMcp::new(access_mode).map_err(|err| {
        io::Error::other(format!(
            "failed to initialize Herdr MCP bridge: {}",
            err.message
        ))
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let shutdown = service.clone();
        let running = service
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|err| io::Error::other(format!("failed to serve MCP over stdio: {err}")))?;
        let wait_result = running.waiting().await;
        shutdown.shutdown_presence().await;
        wait_result
            .map_err(|err| io::Error::other(format!("MCP stdio transport failed: {err}")))?;
        Ok(0)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_validation_rejects_shell_like_and_reserved_inputs() {
        let cwd = std::env::current_dir().expect("current directory");
        let mut params = LaunchCliParams {
            name: "codex".into(),
            argv: vec!["codex".into(), "--help".into()],
            cwd: cwd.to_string_lossy().into_owned(),
            env: HashMap::new(),
            workspace_id: None,
            tab_id: None,
            split: McpSplit::Right,
        };
        assert!(validate_launch(&params).is_ok());

        params
            .env
            .insert("HERDR_DESKTOP_CLI_ID".into(), "forged".into());
        assert!(validate_launch(&params).is_err());
        params.env.clear();
        params.argv[0] = "./codex".into();
        assert!(validate_launch(&params).is_err());
        params.argv[0] = "codex".into();
        params.cwd = "relative".into();
        assert!(validate_launch(&params).is_err());
    }

    #[test]
    fn send_input_requires_exactly_one_mode() {
        let neither = SendInputParams {
            cli_id: "cli".into(),
            text: None,
            keys: None,
        };
        assert!(validate_send_input(&neither).is_err());
        let both = SendInputParams {
            cli_id: "cli".into(),
            text: Some("hello".into()),
            keys: Some(vec!["enter".into()]),
        };
        assert!(validate_send_input(&both).is_err());
    }

    #[test]
    fn utf8_tail_cap_preserves_character_boundaries() {
        let mut text = "start 🦥 end".to_string();
        assert!(truncate_utf8_tail(&mut text, 6));
        assert!(text.len() <= 6);
        assert!(text.is_char_boundary(0));
        assert!(text.ends_with(" end"));
    }

    fn assert_text_mirrors_structured(result: &CallToolResult) {
        let encoded = serde_json::to_value(result).unwrap();
        let content = encoded["content"].as_array().expect("tool result content");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        let mirrored: Value =
            serde_json::from_str(content[0]["text"].as_str().expect("text content"))
                .expect("text content is JSON");
        assert_eq!(
            &mirrored,
            result
                .structured_content
                .as_ref()
                .expect("structured content")
        );
    }

    #[test]
    fn structured_results_include_json_text_content() {
        let ok = success(json!({ "answer": 42 }));
        assert_text_mirrors_structured(&ok);
        assert_eq!(
            ok.structured_content,
            Some(json!({
                "ok": true,
                "data": { "answer": 42 },
            }))
        );

        let error = BridgeError::new("example", "failed", false).result();
        assert_text_mirrors_structured(&error);
        assert_eq!(error.is_error, Some(true));
    }

    #[test]
    fn text_read_cap_measures_the_complete_serialized_result() {
        let mut read = PaneReadResult {
            pane_id: "pane-1".into(),
            workspace_id: "workspace-1".into(),
            tab_id: "tab-1".into(),
            source: ReadSource::Recent,
            format: ReadFormat::Text,
            text: format!("old{}new-tail", "\\\"".repeat(512)),
            revision: 1,
            truncated: true,
        };
        let unbounded = pane_read_tool_result(&read).unwrap();
        let unbounded_len = serialized_tool_result_len(&unbounded).unwrap();
        let max_bytes = unbounded_len / 2;
        let bounded = bounded_text_read_result(&mut read, max_bytes)
            .unwrap()
            .expect("metadata fits within cap");
        assert!(serialized_tool_result_len(&bounded).unwrap() <= max_bytes);
        assert_text_mirrors_structured(&bounded);
        let text = bounded.structured_content.unwrap()["data"]["read"]["text"]
            .as_str()
            .expect("read text")
            .to_string();
        assert!(text.ends_with("new-tail"));
        assert!(!text.starts_with("old"));
    }

    #[test]
    fn drain_result_size_includes_the_complete_tool_envelope() {
        let drain = DrainResult {
            lease_id: "lease_123".into(),
            lease_expires_at_unix_ms: 10,
            messages: Vec::new(),
            remaining: 0,
            quarantined: 0,
        };
        let result = drain_tool_result(&drain);
        assert_text_mirrors_structured(&result);
        let full_len = serialized_tool_result_len(&result).unwrap();
        let mut structured_only = serde_json::to_value(&result).unwrap();
        structured_only["content"] = json!([]);
        let structured_only_len = serde_json::to_vec(&structured_only).unwrap().len();
        assert!(full_len > structured_only_len);
    }

    #[test]
    fn restricted_and_full_control_tool_sets_are_exact() {
        let all_tools = DesktopMcp::tool_router().list_all();
        let mut restricted = all_tools
            .iter()
            .filter(|tool| AccessMode::Restricted.allows_tool(tool.name.as_ref()))
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        restricted.sort_unstable();
        let mut expected_restricted = RESTRICTED_TOOL_NAMES.to_vec();
        expected_restricted.sort_unstable();
        assert_eq!(restricted, expected_restricted);

        let mut full = all_tools
            .iter()
            .filter(|tool| FULL_CONTROL_TOOL_NAMES.contains(&tool.name.as_ref()))
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        full.sort_unstable();
        let mut expected_full = FULL_CONTROL_TOOL_NAMES.to_vec();
        expected_full.sort_unstable();
        assert_eq!(full, expected_full);
        assert_eq!(
            all_tools.len(),
            RESTRICTED_TOOL_NAMES.len() + FULL_CONTROL_TOOL_NAMES.len()
        );
    }

    #[test]
    fn hybrid_inspect_uses_tagged_action_and_params() {
        let params: InspectParams = serde_json::from_value(json!({
            "action": "workspace_list",
            "params": {},
        }))
        .unwrap();
        assert!(matches!(
            params.into_method().unwrap(),
            Method::WorkspaceList(_)
        ));
    }

    #[test]
    fn wait_defaults_to_sixty_seconds_and_caps_ten_minutes() {
        let params: WaitParams = serde_json::from_value(json!({
            "action": "event",
            "params": {
                "match_event": {
                    "event": "workspace_created"
                }
            }
        }))
        .unwrap();
        let (method, envelope) = params.into_method().unwrap();
        let Method::EventsWait(params) = method else {
            panic!("expected events.wait");
        };
        assert_eq!(params.timeout_ms, Some(DEFAULT_WAIT_MS));
        assert_eq!(
            envelope,
            Duration::from_millis(DEFAULT_WAIT_MS) + WAIT_ENVELOPE
        );
        assert!(normalize_wait_timeout(Some(MAX_WAIT_MS + 1)).is_err());
    }

    #[test]
    fn peer_actions_require_qualified_agent_targets() {
        assert!(validate_remote_agent_target("peer::agent").is_ok());
        assert!(validate_remote_agent_target("agent").is_err());
        assert!(validate_local_agent_target("peer::agent").is_err());
    }
}
