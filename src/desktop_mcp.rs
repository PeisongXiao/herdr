//! Private MCP bridge for desktop coding agents such as Codex Desktop.
//!
//! This module deliberately stays on the existing local API boundary. It never
//! starts, stops, discovers, subscribes to, or otherwise manages a Herdr server.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rmcp::model::{CallToolResult, JsonObject};
use rmcp::{tool, tool_router, ErrorData, ServiceExt};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::api::client::{ApiClient, ApiClientError, ConnectionTarget};
use crate::api::schema::{
    AgentInfo, AgentListParams, AgentRenameParams, AgentStartParams, EmptyParams, Method,
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

const HEALTH_TIMEOUT: Duration = Duration::from_secs(1);
const API_TIMEOUT: Duration = Duration::from_secs(5);
const LAUNCH_TIMEOUT: Duration = Duration::from_secs(10);

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

#[derive(Clone)]
struct DesktopMcp {
    api: ApiAdapter,
    queue: Arc<QueueManager>,
    session: Arc<str>,
}

impl DesktopMcp {
    fn new() -> Result<Self, BridgeError> {
        let session = crate::session::active_name()
            .unwrap_or_else(|| crate::session::DEFAULT_SESSION_NAME.to_string());
        let socket_path = crate::session::active_api_socket_path();
        let queue = QueueManager::for_active_session().map_err(queue_error)?;
        Ok(Self {
            api: ApiAdapter::new(socket_path),
            queue: Arc::new(queue),
            session: Arc::from(session),
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

#[tool_router(server_handler)]
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

pub(crate) fn run_stdio() -> io::Result<i32> {
    let service = DesktopMcp::new().map_err(|err| {
        io::Error::other(format!(
            "failed to initialize Herdr MCP bridge: {}",
            err.message
        ))
    })?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        let running = service
            .serve(rmcp::transport::stdio())
            .await
            .map_err(|err| io::Error::other(format!("failed to serve MCP over stdio: {err}")))?;
        running
            .waiting()
            .await
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
}
