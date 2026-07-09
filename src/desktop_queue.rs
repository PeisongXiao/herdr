//! Durable, capability-scoped messages from desktop-launched CLIs to Codex.
//!
//! This module is compiled only on macOS. The enqueue path intentionally has no
//! access to Herdr's API client or process lifecycle controls.

use std::collections::HashSet;
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
#[cfg(test)]
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const QUEUE_SCHEMA_VERSION: u32 = 1;
pub(crate) const QUEUE_VERSION_ENV: &str = "HERDR_DESKTOP_QUEUE_VERSION";
pub(crate) const CLI_ID_ENV: &str = "HERDR_DESKTOP_CLI_ID";
pub(crate) const QUEUE_DIR_ENV: &str = "HERDR_DESKTOP_QUEUE_DIR";

const PRIVATE_DIR_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;
const MAX_CHANNELS: usize = 32;
const MAX_MESSAGES_PER_CHANNEL: usize = 1_000;
const MAX_CHANNEL_BYTES: usize = 8 * 1024 * 1024;
const MAX_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_CORRELATION_ID_BYTES: usize = 128;
const MAX_DRAIN_BYTES: usize = 1024 * 1024;
const LEASE_DURATION: Duration = Duration::from_secs(10 * 60);
const RECEIPT_DURATION: Duration = Duration::from_secs(24 * 60 * 60);
const STALE_RECORD_DURATION: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueErrorKind {
    Invalid,
    PermissionDenied,
    Full,
    Corrupt,
    NotFound,
    CliNotFound,
    LeaseNotFound,
    Closed,
    LeaseExpired,
    Io,
}

#[derive(Debug)]
pub(crate) struct QueueError {
    kind: QueueErrorKind,
    message: String,
}

impl QueueError {
    fn new(kind: QueueErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    fn io(context: &str, err: io::Error) -> Self {
        let kind = if err.kind() == io::ErrorKind::PermissionDenied {
            QueueErrorKind::PermissionDenied
        } else {
            QueueErrorKind::Io
        };
        Self::new(kind, format!("{context}: {err}"))
    }

    pub(crate) fn kind(&self) -> QueueErrorKind {
        self.kind
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for QueueError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CliState {
    Creating,
    Active,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CliIdentity {
    pub(crate) cli_id: String,
    pub(crate) pane_id: String,
    pub(crate) terminal_id: String,
    pub(crate) workspace_id: String,
    pub(crate) tab_id: String,
    pub(crate) name: String,
    pub(crate) cwd: String,
    pub(crate) state: CliState,
    #[serde(default)]
    pub(crate) launch_marker: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ChannelHandle {
    pub(crate) cli_id: String,
    pub(crate) launch_marker: String,
    pub(crate) env: Vec<(String, String)>,
    channel_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct QueuedMessage {
    #[serde(rename = "schema")]
    schema_version: u32,
    pub(crate) message_id: String,
    pub(crate) sequence: u64,
    pub(crate) cli_id: String,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) kind: String,
    pub(crate) correlation_id: Option<String>,
    pub(crate) text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EnqueueReceipt {
    pub(crate) cli_id: String,
    pub(crate) message_id: String,
    pub(crate) sequence: u64,
    pub(crate) created_at_unix_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct DrainResult {
    pub(crate) lease_id: String,
    pub(crate) lease_expires_at_unix_ms: u64,
    pub(crate) messages: Vec<QueuedMessage>,
    pub(crate) remaining: usize,
    pub(crate) quarantined: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct AckResult {
    pub(crate) lease_id: String,
    pub(crate) already_acked: bool,
    pub(crate) deleted: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ChannelMetadata {
    #[serde(rename = "schema")]
    schema_version: u32,
    cli_id: String,
    session: String,
    name: String,
    cwd: String,
    state: CliState,
    next_sequence: u64,
    created_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    #[serde(default)]
    launch_marker: Option<String>,
    #[serde(default)]
    identity: Option<CliIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct IndexEntry {
    #[serde(rename = "schema")]
    schema_version: u32,
    channel_dir: String,
    identity: CliIdentity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseMetadata {
    #[serde(rename = "schema")]
    schema_version: u32,
    cli_id: String,
    lease_id: String,
    expires_at_unix_ms: u64,
    files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AckReceipt {
    #[serde(rename = "schema")]
    schema_version: u32,
    cli_id: String,
    lease_id: String,
    acked_at_unix_ms: u64,
    deleted: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct QueueManager {
    root: PathBuf,
    session: String,
}

impl QueueManager {
    pub(crate) fn for_active_session() -> Result<Self, QueueError> {
        let socket_path = absolute_path(crate::session::active_api_socket_path())?;
        let digest = Sha256::digest(socket_path.as_os_str().as_encoded_bytes());
        let session_key = hex_bytes(&digest[..8]);
        let session = crate::session::active_name()
            .unwrap_or_else(|| crate::session::DEFAULT_SESSION_NAME.to_string());
        let state_dir = crate::config::state_dir();
        fs::create_dir_all(&state_dir)
            .map_err(|err| QueueError::io("create Herdr state directory", err))?;
        let desktop_root = state_dir.join("desktop-mcp");
        ensure_private_dir(&desktop_root)?;
        let version_root = desktop_root.join("v1");
        ensure_private_dir(&version_root)?;
        let root = version_root.join(session_key);
        Self::new(root, session)
    }

    fn new(root: PathBuf, session: String) -> Result<Self, QueueError> {
        ensure_private_dir(&root)?;
        ensure_private_dir(&root.join("index"))?;
        ensure_private_dir(&root.join("channels"))?;
        ensure_private_dir(&root.join("quarantine"))?;
        let manager = Self { root, session };
        let _lock = PrivateFileLock::acquire(&manager.root.join("spool.lock"))?;
        manager.maintain()?;
        Ok(manager)
    }

    #[cfg(test)]
    fn for_test(root: PathBuf) -> Result<Self, QueueError> {
        Self::new(root, "test".to_string())
    }

    pub(crate) fn create_channel(
        &self,
        name: &str,
        cwd: &Path,
    ) -> Result<ChannelHandle, QueueError> {
        let _lock = PrivateFileLock::acquire(&self.root.join("spool.lock"))?;
        self.maintain()?;
        if self.index_entries()?.len() >= MAX_CHANNELS {
            return Err(QueueError::new(
                QueueErrorKind::Full,
                format!("desktop queue supports at most {MAX_CHANNELS} channels per session"),
            ));
        }

        for _ in 0..16 {
            let cli_id = format!("cli_{}", random_hex(16)?);
            let launch_marker = format!("herdr_mcp_{}", random_hex(16)?);
            let capability = random_hex(32)?;
            let channel_name = format!("{cli_id}.{capability}");
            let channel_dir = self.root.join("channels").join(&channel_name);
            match create_private_dir(&channel_dir) {
                Ok(()) => {
                    for child in ["tmp", "pending", "leases", "receipts", "quarantine"] {
                        create_private_dir(&channel_dir.join(child))?;
                    }
                    let _queue_lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
                    let now = unix_ms()?;
                    let identity = CliIdentity {
                        cli_id: cli_id.clone(),
                        pane_id: String::new(),
                        terminal_id: String::new(),
                        workspace_id: String::new(),
                        tab_id: String::new(),
                        name: name.to_string(),
                        cwd: cwd.to_string_lossy().into_owned(),
                        state: CliState::Creating,
                        launch_marker: Some(launch_marker.clone()),
                    };
                    let metadata = ChannelMetadata {
                        schema_version: QUEUE_SCHEMA_VERSION,
                        cli_id: cli_id.clone(),
                        session: self.session.clone(),
                        name: name.to_string(),
                        cwd: cwd.to_string_lossy().into_owned(),
                        state: CliState::Creating,
                        next_sequence: 1,
                        created_at_unix_ms: now,
                        updated_at_unix_ms: now,
                        launch_marker: Some(launch_marker.clone()),
                        identity: Some(identity.clone()),
                    };
                    atomic_json(&channel_dir, "channel.json", &metadata)?;
                    atomic_json(
                        &self.root.join("index"),
                        &format!("{cli_id}.json"),
                        &IndexEntry {
                            schema_version: QUEUE_SCHEMA_VERSION,
                            channel_dir: channel_name,
                            identity,
                        },
                    )?;
                    let env = vec![
                        (
                            QUEUE_VERSION_ENV.to_string(),
                            QUEUE_SCHEMA_VERSION.to_string(),
                        ),
                        (CLI_ID_ENV.to_string(), cli_id.clone()),
                        (
                            QUEUE_DIR_ENV.to_string(),
                            channel_dir.to_string_lossy().into_owned(),
                        ),
                    ];
                    return Ok(ChannelHandle {
                        cli_id,
                        launch_marker,
                        env,
                        channel_dir,
                    });
                }
                Err(err) if err.kind() == QueueErrorKind::Io => continue,
                Err(err) => return Err(err),
            }
        }
        Err(QueueError::new(
            QueueErrorKind::Io,
            "failed to allocate a unique desktop queue channel",
        ))
    }

    pub(crate) fn activate_channel(
        &self,
        handle: &ChannelHandle,
        mut identity: CliIdentity,
    ) -> Result<CliIdentity, QueueError> {
        let _spool_lock = PrivateFileLock::acquire(&self.root.join("spool.lock"))?;
        let mut entry = self.read_index(&handle.cli_id)?;
        if entry.channel_dir != channel_basename(&handle.channel_dir)?
            || entry.identity.state != CliState::Creating
            || entry.identity.launch_marker.as_deref() != Some(&handle.launch_marker)
        {
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                "desktop queue channel changed during CLI launch",
            ));
        }
        identity.cli_id = handle.cli_id.clone();
        identity.state = CliState::Active;
        identity.launch_marker = Some(handle.launch_marker.clone());
        entry.identity = identity.clone();
        let _queue_lock = PrivateFileLock::acquire(&handle.channel_dir.join("queue.lock"))?;
        let mut metadata: ChannelMetadata = read_json(&handle.channel_dir.join("channel.json"))?;
        validate_channel_metadata(&metadata, &handle.cli_id, &self.session)?;
        if metadata.launch_marker.as_deref() != Some(&handle.launch_marker) {
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                "desktop queue launch marker changed during CLI launch",
            ));
        }
        metadata.state = CliState::Active;
        metadata.updated_at_unix_ms = unix_ms()?;
        metadata.identity = Some(identity.clone());
        atomic_json(&handle.channel_dir, "channel.json", &metadata)?;
        self.write_index(&entry)?;
        Ok(identity)
    }

    pub(crate) fn activate_reconciled(
        &self,
        cli_id: &str,
        launch_marker: &str,
        mut identity: CliIdentity,
    ) -> Result<CliIdentity, QueueError> {
        validate_id("cli_id", cli_id)?;
        validate_id("launch_marker", launch_marker)?;
        let _spool_lock = PrivateFileLock::acquire(&self.root.join("spool.lock"))?;
        let mut entry = self.read_index(cli_id)?;
        if entry.identity.state != CliState::Creating
            || entry.identity.launch_marker.as_deref() != Some(launch_marker)
        {
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                "desktop queue launch record changed during reconciliation",
            ));
        }
        let channel_dir = self.channel_dir(&entry)?;
        let _queue_lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
        let mut metadata: ChannelMetadata = read_json(&channel_dir.join("channel.json"))?;
        validate_channel_metadata(&metadata, cli_id, &self.session)?;
        if metadata.state != CliState::Creating
            || metadata.launch_marker.as_deref() != Some(launch_marker)
        {
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                "desktop queue launch metadata changed during reconciliation",
            ));
        }
        identity.cli_id = cli_id.to_string();
        identity.state = CliState::Active;
        identity.launch_marker = Some(launch_marker.to_string());
        entry.identity = identity.clone();
        metadata.state = CliState::Active;
        metadata.updated_at_unix_ms = unix_ms()?;
        metadata.identity = Some(identity.clone());
        atomic_json(&channel_dir, "channel.json", &metadata)?;
        self.write_index(&entry)?;
        Ok(identity)
    }

    pub(crate) fn abort_empty_channel(&self, handle: &ChannelHandle) -> Result<(), QueueError> {
        let _spool_lock = PrivateFileLock::acquire(&self.root.join("spool.lock"))?;
        validate_private_dir(&handle.channel_dir)?;
        let _queue_lock = PrivateFileLock::acquire(&handle.channel_dir.join("queue.lock"))?;
        let has_messages = directory_has_json(&handle.channel_dir.join("pending"))?
            || lease_message_count(&handle.channel_dir.join("leases"))? > 0;
        if has_messages {
            let mut entry = self.read_index(&handle.cli_id)?;
            entry.identity.state = CliState::Closed;
            let mut metadata: ChannelMetadata =
                read_json(&handle.channel_dir.join("channel.json"))?;
            metadata.state = CliState::Closed;
            metadata.updated_at_unix_ms = unix_ms()?;
            metadata.identity = Some(entry.identity.clone());
            atomic_json(&handle.channel_dir, "channel.json", &metadata)?;
            self.write_index(&entry)?;
            return Ok(());
        }
        remove_private_file_if_exists(&self.index_path(&handle.cli_id))?;
        fs::remove_dir_all(&handle.channel_dir)
            .map_err(|err| QueueError::io("remove empty desktop queue channel", err))?;
        sync_dir(&self.root.join("channels"))?;
        Ok(())
    }

    pub(crate) fn lookup_cli(&self, cli_id: &str) -> Result<CliIdentity, QueueError> {
        validate_id("cli_id", cli_id)?;
        Ok(self.read_index(cli_id)?.identity)
    }

    pub(crate) fn mark_closed(&self, cli_id: &str) -> Result<CliIdentity, QueueError> {
        validate_id("cli_id", cli_id)?;
        let _spool_lock = PrivateFileLock::acquire(&self.root.join("spool.lock"))?;
        let mut entry = self.read_index(cli_id)?;
        entry.identity.state = CliState::Closed;
        let channel_dir = self.channel_dir(&entry)?;
        let _queue_lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
        let mut metadata: ChannelMetadata = read_json(&channel_dir.join("channel.json"))?;
        metadata.state = CliState::Closed;
        metadata.updated_at_unix_ms = unix_ms()?;
        metadata.identity = Some(entry.identity.clone());
        atomic_json(&channel_dir, "channel.json", &metadata)?;
        self.write_index(&entry)?;
        Ok(entry.identity)
    }

    pub(crate) fn prune_stale_creating(
        &self,
        live_markers: &HashSet<String>,
    ) -> Result<Vec<String>, QueueError> {
        let _spool_lock = PrivateFileLock::acquire(&self.root.join("spool.lock"))?;
        self.prune_stale_channels(Some(live_markers))
    }

    #[cfg(test)]
    pub(crate) fn drain(&self, cli_id: &str, limit: usize) -> Result<DrainResult, QueueError> {
        self.drain_bounded(cli_id, limit, |_| true)
    }

    pub(crate) fn drain_bounded<F>(
        &self,
        cli_id: &str,
        limit: usize,
        fits: F,
    ) -> Result<DrainResult, QueueError>
    where
        F: Fn(&DrainResult) -> bool,
    {
        validate_id("cli_id", cli_id)?;
        if limit == 0 || limit > 100 {
            return Err(QueueError::new(
                QueueErrorKind::Invalid,
                "drain limit must be between 1 and 100",
            ));
        }
        let entry = self.read_index(cli_id)?;
        let channel_dir = self.channel_dir(&entry)?;
        let _lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
        let now = unix_ms()?;
        recover_expired_leases(&channel_dir, cli_id, now)?;
        prune_receipts(&channel_dir, cli_id, now)?;

        let mut candidates = Vec::new();
        let mut quarantined = 0usize;
        for path in sorted_json_files(&channel_dir.join("pending"))? {
            let file_name = file_name_string(&path)?;
            let message: QueuedMessage = match read_json::<QueuedMessage>(&path) {
                Ok(message) if validate_queued_message(&path, &message, cli_id).is_ok() => message,
                Ok(_) | Err(_) => {
                    move_to_quarantine(&channel_dir, &path)?;
                    quarantined += 1;
                    continue;
                }
            };
            let message_bytes = serde_json::to_vec(&message)
                .map_err(|err| QueueError::new(QueueErrorKind::Corrupt, err.to_string()))?
                .len();
            candidates.push((file_name, message, message_bytes));
        }

        let lease_id = format!("lease_{}", random_hex(16)?);
        let expires_at_unix_ms = now.saturating_add(duration_ms(LEASE_DURATION));
        let mut messages = Vec::new();
        let mut selected_files = Vec::new();
        let mut encoded_bytes = 0usize;
        for (file_name, message, message_bytes) in candidates.iter().cloned() {
            if encoded_bytes.saturating_add(message_bytes) > MAX_DRAIN_BYTES {
                break;
            }
            let mut proposed_messages = messages.clone();
            proposed_messages.push(message.clone());
            let proposed = DrainResult {
                lease_id: lease_id.clone(),
                lease_expires_at_unix_ms: expires_at_unix_ms,
                messages: proposed_messages,
                remaining: candidates.len().saturating_sub(messages.len() + 1),
                quarantined,
            };
            if !fits(&proposed) {
                break;
            }
            encoded_bytes = encoded_bytes.saturating_add(message_bytes);
            selected_files.push(file_name);
            messages.push(message);
            if messages.len() >= limit {
                break;
            }
        }

        let lease_dir = channel_dir.join("leases").join(&lease_id);
        create_private_dir(&lease_dir)?;
        for file_name in &selected_files {
            fs::rename(
                channel_dir.join("pending").join(file_name),
                lease_dir.join(file_name),
            )
            .map_err(|err| QueueError::io("lease queued desktop message", err))?;
        }
        atomic_json(
            &lease_dir,
            "lease.json",
            &LeaseMetadata {
                schema_version: QUEUE_SCHEMA_VERSION,
                cli_id: cli_id.to_string(),
                lease_id: lease_id.clone(),
                expires_at_unix_ms,
                files: selected_files,
            },
        )?;
        sync_dir(&channel_dir.join("pending"))?;
        sync_dir(&channel_dir.join("leases"))?;
        let remaining = sorted_json_files(&channel_dir.join("pending"))?.len();
        Ok(DrainResult {
            lease_id,
            lease_expires_at_unix_ms: expires_at_unix_ms,
            messages,
            remaining,
            quarantined,
        })
    }

    pub(crate) fn release(&self, cli_id: &str, lease_id: &str) -> Result<(), QueueError> {
        validate_id("cli_id", cli_id)?;
        validate_id("lease_id", lease_id)?;
        let entry = self.read_index(cli_id)?;
        let channel_dir = self.channel_dir(&entry)?;
        let _lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
        let lease_dir = channel_dir.join("leases").join(lease_id);
        validate_private_dir(&lease_dir).map_err(|err| {
            if matches!(err.kind(), QueueErrorKind::Io | QueueErrorKind::NotFound) {
                QueueError::new(QueueErrorKind::LeaseNotFound, "message lease was not found")
            } else {
                err
            }
        })?;
        let lease: LeaseMetadata = read_json(&lease_dir.join("lease.json"))?;
        validate_lease(&lease_dir, &lease, cli_id)?;
        requeue_lease(&channel_dir, &lease_dir, &lease, cli_id)
    }

    pub(crate) fn ack(&self, cli_id: &str, lease_id: &str) -> Result<AckResult, QueueError> {
        validate_id("cli_id", cli_id)?;
        validate_id("lease_id", lease_id)?;
        let entry = self.read_index(cli_id)?;
        let channel_dir = self.channel_dir(&entry)?;
        let _lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
        let receipt_path = channel_dir
            .join("receipts")
            .join(format!("{lease_id}.json"));
        let now = unix_ms()?;
        if receipt_path.exists() {
            match read_json::<AckReceipt>(&receipt_path).and_then(|receipt| {
                validate_receipt(&receipt, cli_id, lease_id)?;
                Ok(receipt)
            }) {
                Ok(receipt)
                    if receipt
                        .acked_at_unix_ms
                        .saturating_add(duration_ms(RECEIPT_DURATION))
                        > now =>
                {
                    return Ok(AckResult {
                        lease_id: lease_id.to_string(),
                        already_acked: true,
                        deleted: receipt.deleted,
                    });
                }
                Ok(_) => remove_private_file_if_exists(&receipt_path)?,
                Err(err) => {
                    move_to_quarantine(&channel_dir, &receipt_path)?;
                    return Err(QueueError::new(
                        QueueErrorKind::Corrupt,
                        format!("invalid acknowledgment receipt: {err}"),
                    ));
                }
            }
        }

        let lease_dir = channel_dir.join("leases").join(lease_id);
        validate_private_dir(&lease_dir).map_err(|err| {
            if matches!(err.kind(), QueueErrorKind::Io | QueueErrorKind::NotFound) {
                QueueError::new(QueueErrorKind::LeaseNotFound, "message lease was not found")
            } else {
                err
            }
        })?;
        let lease: LeaseMetadata = read_json(&lease_dir.join("lease.json"))?;
        if let Err(err) = validate_lease(&lease_dir, &lease, cli_id) {
            recover_corrupt_lease(&channel_dir, &lease_dir, cli_id)?;
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                format!("invalid message lease: {err}"),
            ));
        }
        if lease.expires_at_unix_ms <= now {
            requeue_lease(&channel_dir, &lease_dir, &lease, cli_id)?;
            return Err(QueueError::new(
                QueueErrorKind::LeaseExpired,
                "message lease expired and was returned to the queue",
            ));
        }
        let deleted = lease
            .files
            .iter()
            .filter(|file_name| lease_dir.join(file_name).is_file())
            .count();
        atomic_json(
            &channel_dir.join("receipts"),
            &format!("{lease_id}.json"),
            &AckReceipt {
                schema_version: QUEUE_SCHEMA_VERSION,
                cli_id: cli_id.to_string(),
                lease_id: lease_id.to_string(),
                acked_at_unix_ms: now,
                deleted,
            },
        )?;
        fs::remove_dir_all(&lease_dir)
            .map_err(|err| QueueError::io("remove acknowledged desktop message lease", err))?;
        sync_dir(&channel_dir.join("leases"))?;
        Ok(AckResult {
            lease_id: lease_id.to_string(),
            already_acked: false,
            deleted,
        })
    }

    fn index_entries(&self) -> Result<Vec<IndexEntry>, QueueError> {
        let mut entries = Vec::new();
        for path in sorted_json_files(&self.root.join("index"))? {
            let expected_cli_id = path
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            match read_json::<IndexEntry>(&path).and_then(|entry| {
                validate_index_entry(&entry, expected_cli_id)?;
                Ok(entry)
            }) {
                Ok(entry) => entries.push(entry),
                Err(_) => move_to_root_quarantine(&self.root, &path)?,
            }
        }
        Ok(entries)
    }

    fn index_path(&self, cli_id: &str) -> PathBuf {
        self.root.join("index").join(format!("{cli_id}.json"))
    }

    fn read_index(&self, cli_id: &str) -> Result<IndexEntry, QueueError> {
        let path = self.index_path(cli_id);
        if !path.exists() {
            return Err(QueueError::new(
                QueueErrorKind::CliNotFound,
                format!("desktop CLI {cli_id} is not registered"),
            ));
        }
        let entry: IndexEntry = read_json(&path)?;
        validate_index_entry(&entry, cli_id)?;
        Ok(entry)
    }

    fn write_index(&self, entry: &IndexEntry) -> Result<(), QueueError> {
        atomic_json(
            &self.root.join("index"),
            &format!("{}.json", entry.identity.cli_id),
            entry,
        )
    }

    fn channel_dir(&self, entry: &IndexEntry) -> Result<PathBuf, QueueError> {
        validate_component("channel directory", &entry.channel_dir)?;
        let (channel_cli_id, _) = parse_channel_name(&entry.channel_dir)?;
        if channel_cli_id != entry.identity.cli_id {
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                "desktop CLI registry points at the wrong channel",
            ));
        }
        let path = self.root.join("channels").join(&entry.channel_dir);
        validate_private_dir(&path)?;
        Ok(path)
    }

    fn maintain(&self) -> Result<(), QueueError> {
        let now = unix_ms()?;
        prune_stale_temp_files(&self.root.join("index"), now)?;
        self.reconcile_channels(now)?;
        let _ = self.prune_stale_channels(None)?;
        self.prune_orphan_indexes()?;
        Ok(())
    }

    fn reconcile_channels(&self, now: u64) -> Result<(), QueueError> {
        let channels_dir = self.root.join("channels");
        for directory_entry in read_dir_entries(&channels_dir)? {
            let file_type = directory_entry
                .file_type()
                .map_err(|err| QueueError::io("inspect desktop queue channel", err))?;
            if file_type.is_symlink() {
                return Err(QueueError::new(
                    QueueErrorKind::PermissionDenied,
                    "desktop queue channel may not be a symlink",
                ));
            }
            if !file_type.is_dir() {
                continue;
            }
            let channel_dir = directory_entry.path();
            validate_private_dir(&channel_dir)?;
            let channel_name = file_name_string(&channel_dir)?;
            let (cli_id, _) = match parse_channel_name(&channel_name) {
                Ok(parts) => parts,
                Err(_) => continue,
            };
            let channel_path = channel_dir.join("channel.json");
            if !channel_path.exists() {
                if path_is_stale(&channel_dir, now)? {
                    fs::remove_dir_all(&channel_dir).map_err(|err| {
                        QueueError::io("remove incomplete desktop queue channel", err)
                    })?;
                    sync_dir(&channels_dir)?;
                }
                continue;
            }
            for child in ["tmp", "pending", "leases", "receipts", "quarantine"] {
                ensure_private_dir(&channel_dir.join(child))?;
            }
            let _queue_lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
            prune_stale_temp_files(&channel_dir, now)?;
            prune_stale_temp_files(&channel_dir.join("tmp"), now)?;
            prune_stale_temp_files(&channel_dir.join("receipts"), now)?;
            for lease_entry in read_dir_entries(&channel_dir.join("leases"))? {
                let lease_type = lease_entry
                    .file_type()
                    .map_err(|err| QueueError::io("inspect desktop message lease", err))?;
                if lease_type.is_symlink() {
                    return Err(QueueError::new(
                        QueueErrorKind::PermissionDenied,
                        "desktop message lease may not be a symlink",
                    ));
                }
                if lease_type.is_dir() {
                    validate_private_dir(&lease_entry.path())?;
                    prune_stale_temp_files(&lease_entry.path(), now)?;
                }
            }

            let mut metadata: ChannelMetadata = match read_json(&channel_path) {
                Ok(metadata) => metadata,
                Err(_) => {
                    move_to_quarantine(&channel_dir, &channel_path)?;
                    let index_path = self.index_path(&cli_id);
                    if index_path.exists() {
                        move_to_root_quarantine(&self.root, &index_path)?;
                    }
                    continue;
                }
            };
            if validate_channel_metadata(&metadata, &cli_id, &self.session).is_err() {
                move_to_quarantine(&channel_dir, &channel_path)?;
                let index_path = self.index_path(&cli_id);
                if index_path.exists() {
                    move_to_root_quarantine(&self.root, &index_path)?;
                }
                continue;
            }
            prune_receipts(&channel_dir, &cli_id, now)?;

            let index_path = self.index_path(&cli_id);
            let existing = if index_path.exists() {
                match read_json::<IndexEntry>(&index_path).and_then(|entry| {
                    validate_index_entry(&entry, &cli_id)?;
                    Ok(entry)
                }) {
                    Ok(entry) if entry.channel_dir == channel_name => Some(entry),
                    Ok(_) | Err(_) => {
                        move_to_root_quarantine(&self.root, &index_path)?;
                        None
                    }
                }
            } else {
                None
            };

            let mut identity = metadata
                .identity
                .clone()
                .or_else(|| existing.as_ref().map(|entry| entry.identity.clone()))
                .unwrap_or_else(|| placeholder_identity(&metadata));
            identity.cli_id.clone_from(&cli_id);
            identity.state = metadata.state.clone();
            let reconciled = IndexEntry {
                schema_version: QUEUE_SCHEMA_VERSION,
                channel_dir: channel_name,
                identity: identity.clone(),
            };
            if metadata.identity.as_ref() != Some(&identity) {
                metadata.identity = Some(identity);
                atomic_json(&channel_dir, "channel.json", &metadata)?;
            }
            if existing.as_ref() != Some(&reconciled) {
                self.write_index(&reconciled)?;
            }
        }
        Ok(())
    }

    fn prune_stale_channels(
        &self,
        live_markers: Option<&HashSet<String>>,
    ) -> Result<Vec<String>, QueueError> {
        let now = unix_ms()?;
        let mut pruned_creating = Vec::new();
        for entry in self.index_entries()? {
            let channel_dir = match self.channel_dir(&entry) {
                Ok(path) => path,
                Err(_) => continue,
            };
            let _queue_lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
            let metadata: ChannelMetadata = match read_json(&channel_dir.join("channel.json")) {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            if validate_channel_metadata(&metadata, &entry.identity.cli_id, &self.session).is_err()
            {
                continue;
            }
            prune_stale_temp_files(&channel_dir.join("tmp"), now)?;
            prune_receipts(&channel_dir, &entry.identity.cli_id, now)?;
            let empty = !directory_has_json(&channel_dir.join("pending"))?
                && lease_message_count(&channel_dir.join("leases"))? == 0
                && !directory_has_entries(&channel_dir.join("tmp"))?
                && !directory_has_json(&channel_dir.join("receipts"))?
                && !directory_has_entries(&channel_dir.join("quarantine"))?;
            let confirmed_absent = match metadata.launch_marker.as_ref() {
                None => true,
                Some(marker) => live_markers.is_some_and(|markers| !markers.contains(marker)),
            };
            let stale_creating = metadata.state == CliState::Creating
                && confirmed_absent
                && metadata
                    .updated_at_unix_ms
                    .saturating_add(duration_ms(STALE_RECORD_DURATION))
                    <= now;
            if empty && (metadata.state == CliState::Closed || stale_creating) {
                if stale_creating {
                    pruned_creating.push(entry.identity.cli_id.clone());
                }
                remove_private_file_if_exists(&self.index_path(&entry.identity.cli_id))?;
                fs::remove_dir_all(&channel_dir)
                    .map_err(|err| QueueError::io("remove stale desktop queue channel", err))?;
            }
        }
        sync_dir(&self.root.join("index"))?;
        sync_dir(&self.root.join("channels"))?;
        Ok(pruned_creating)
    }

    fn prune_orphan_indexes(&self) -> Result<(), QueueError> {
        for entry in self.index_entries()? {
            let channel_path = self.root.join("channels").join(&entry.channel_dir);
            if !channel_path.exists() {
                move_to_root_quarantine(&self.root, &self.index_path(&entry.identity.cli_id))?;
            }
        }
        Ok(())
    }
}

pub(crate) fn enqueue_from_env(
    kind: &str,
    correlation_id: Option<&str>,
    text: String,
) -> Result<EnqueueReceipt, QueueError> {
    validate_message(kind, correlation_id, &text)?;
    if std::env::var(QUEUE_VERSION_ENV).as_deref() != Ok("1") {
        return Err(QueueError::new(
            QueueErrorKind::PermissionDenied,
            "desktop queue capability is unavailable",
        ));
    }
    let cli_id = std::env::var(CLI_ID_ENV).map_err(|_| {
        QueueError::new(
            QueueErrorKind::PermissionDenied,
            "desktop queue CLI identity is unavailable",
        )
    })?;
    validate_id("cli_id", &cli_id)?;
    let channel_dir = PathBuf::from(std::env::var_os(QUEUE_DIR_ENV).ok_or_else(|| {
        QueueError::new(
            QueueErrorKind::PermissionDenied,
            "desktop queue capability directory is unavailable",
        )
    })?);
    if !channel_dir.is_absolute() {
        return Err(QueueError::new(
            QueueErrorKind::PermissionDenied,
            "desktop queue capability directory must be absolute",
        ));
    }
    validate_private_dir(&channel_dir)?;
    let _lock = PrivateFileLock::acquire(&channel_dir.join("queue.lock"))?;
    let mut metadata: ChannelMetadata = read_json(&channel_dir.join("channel.json"))?;
    if metadata.schema_version != QUEUE_SCHEMA_VERSION || metadata.cli_id != cli_id {
        return Err(QueueError::new(
            QueueErrorKind::PermissionDenied,
            "desktop queue capability does not match this CLI",
        ));
    }
    if metadata.state == CliState::Closed {
        return Err(QueueError::new(
            QueueErrorKind::Closed,
            "desktop queue channel is closed",
        ));
    }
    let now = unix_ms()?;
    recover_expired_leases(&channel_dir, &cli_id, now)?;
    prune_receipts(&channel_dir, &cli_id, now)?;
    let sequence = metadata.next_sequence;
    let message_id = format!("msg_{}", random_hex(16)?);
    let message = QueuedMessage {
        schema_version: QUEUE_SCHEMA_VERSION,
        message_id: message_id.clone(),
        sequence,
        cli_id: cli_id.clone(),
        created_at_unix_ms: now,
        kind: kind.to_string(),
        correlation_id: correlation_id.map(str::to_string),
        text,
    };
    let encoded_message = serde_json::to_vec(&message)
        .map_err(|err| QueueError::new(QueueErrorKind::Corrupt, err.to_string()))?;
    let (message_count, message_bytes) = queued_usage(&channel_dir)?;
    if message_count >= MAX_MESSAGES_PER_CHANNEL
        || message_bytes.saturating_add(encoded_message.len()) > MAX_CHANNEL_BYTES
    {
        return Err(QueueError::new(
            QueueErrorKind::Full,
            "desktop queue channel quota is full",
        ));
    }

    metadata.next_sequence = metadata.next_sequence.checked_add(1).ok_or_else(|| {
        QueueError::new(QueueErrorKind::Full, "desktop queue sequence is exhausted")
    })?;
    metadata.updated_at_unix_ms = now;
    atomic_json(&channel_dir, "channel.json", &metadata)?;

    let file_name = format!("{sequence:020}-{message_id}.json");
    atomic_message(&channel_dir, &file_name, &encoded_message)?;
    Ok(EnqueueReceipt {
        cli_id,
        message_id,
        sequence,
        created_at_unix_ms: now,
    })
}

fn validate_message(
    kind: &str,
    correlation_id: Option<&str>,
    text: &str,
) -> Result<(), QueueError> {
    if !matches!(kind, "info" | "progress" | "result" | "question" | "error") {
        return Err(QueueError::new(
            QueueErrorKind::Invalid,
            "message kind must be info, progress, result, question, or error",
        ));
    }
    if text.len() > MAX_MESSAGE_BYTES {
        return Err(QueueError::new(
            QueueErrorKind::Invalid,
            format!("message must contain at most {MAX_MESSAGE_BYTES} UTF-8 bytes"),
        ));
    }
    if let Some(correlation_id) = correlation_id {
        if correlation_id.is_empty()
            || correlation_id.len() > MAX_CORRELATION_ID_BYTES
            || !correlation_id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            return Err(QueueError::new(
                QueueErrorKind::Invalid,
                "correlation id must use 1 to 128 ASCII letters, numbers, '.', '_', ':', or '-'",
            ));
        }
    }
    Ok(())
}

fn validate_channel_metadata(
    metadata: &ChannelMetadata,
    cli_id: &str,
    session: &str,
) -> Result<(), QueueError> {
    if let Some(marker) = metadata.launch_marker.as_deref() {
        validate_id("launch_marker", marker)?;
    }
    if metadata.schema_version != QUEUE_SCHEMA_VERSION
        || metadata.cli_id != cli_id
        || metadata.session != session
        || metadata.identity.as_ref().is_some_and(|identity| {
            identity.cli_id != cli_id || identity.launch_marker != metadata.launch_marker
        })
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "desktop queue channel metadata is invalid",
        ));
    }
    Ok(())
}

fn validate_index_entry(entry: &IndexEntry, cli_id: &str) -> Result<(), QueueError> {
    validate_id("cli_id", cli_id)?;
    if let Some(marker) = entry.identity.launch_marker.as_deref() {
        validate_id("launch_marker", marker)?;
    }
    let (channel_cli_id, _) = parse_channel_name(&entry.channel_dir)?;
    if entry.schema_version != QUEUE_SCHEMA_VERSION
        || entry.identity.cli_id != cli_id
        || channel_cli_id != cli_id
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            format!("desktop CLI registry entry {cli_id} is invalid"),
        ));
    }
    Ok(())
}

fn parse_channel_name(channel_name: &str) -> Result<(String, String), QueueError> {
    validate_component("channel directory", channel_name)?;
    let (cli_id, capability) = channel_name.split_once('.').ok_or_else(|| {
        QueueError::new(
            QueueErrorKind::Corrupt,
            "desktop queue channel name is invalid",
        )
    })?;
    validate_id("cli_id", cli_id)?;
    if capability.len() != 64
        || !capability
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "desktop queue channel capability is invalid",
        ));
    }
    Ok((cli_id.to_string(), capability.to_string()))
}

fn placeholder_identity(metadata: &ChannelMetadata) -> CliIdentity {
    CliIdentity {
        cli_id: metadata.cli_id.clone(),
        pane_id: String::new(),
        terminal_id: String::new(),
        workspace_id: String::new(),
        tab_id: String::new(),
        name: metadata.name.clone(),
        cwd: metadata.cwd.clone(),
        state: metadata.state.clone(),
        launch_marker: metadata.launch_marker.clone(),
    }
}

fn validate_queued_message(
    path: &Path,
    message: &QueuedMessage,
    cli_id: &str,
) -> Result<(), QueueError> {
    validate_id("message_id", &message.message_id)?;
    validate_message(
        &message.kind,
        message.correlation_id.as_deref(),
        &message.text,
    )?;
    let expected_name = format!("{:020}-{}.json", message.sequence, message.message_id);
    if message.schema_version != QUEUE_SCHEMA_VERSION
        || message.cli_id != cli_id
        || file_name_string(path)? != expected_name
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "queued desktop message metadata is invalid",
        ));
    }
    Ok(())
}

fn validate_lease(lease_dir: &Path, lease: &LeaseMetadata, cli_id: &str) -> Result<(), QueueError> {
    validate_id("lease_id", &lease.lease_id)?;
    if lease.schema_version != QUEUE_SCHEMA_VERSION
        || lease.cli_id != cli_id
        || channel_basename(lease_dir)? != lease.lease_id
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "message lease metadata does not match its channel",
        ));
    }
    let mut expected = lease.files.clone();
    for file_name in &expected {
        validate_component("leased message", file_name)?;
        if file_name == "lease.json" || !file_name.ends_with(".json") {
            return Err(QueueError::new(
                QueueErrorKind::Corrupt,
                "message lease contains an invalid file name",
            ));
        }
    }
    expected.sort();
    if expected.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "message lease contains duplicate files",
        ));
    }
    let actual = sorted_json_files(lease_dir)?
        .into_iter()
        .filter(|path| path.file_name().and_then(|name| name.to_str()) != Some("lease.json"))
        .map(|path| file_name_string(&path))
        .collect::<Result<Vec<_>, _>>()?;
    if actual != expected {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "message lease file list does not match its contents",
        ));
    }
    for file_name in &expected {
        let path = lease_dir.join(file_name);
        let message: QueuedMessage = read_json(&path)?;
        validate_queued_message(&path, &message, cli_id)?;
    }
    Ok(())
}

fn validate_receipt(receipt: &AckReceipt, cli_id: &str, lease_id: &str) -> Result<(), QueueError> {
    if receipt.schema_version != QUEUE_SCHEMA_VERSION
        || receipt.cli_id != cli_id
        || receipt.lease_id != lease_id
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            "acknowledgment receipt does not match the requested lease",
        ));
    }
    Ok(())
}

fn validate_receipt_file(
    path: &Path,
    receipt: &AckReceipt,
    cli_id: &str,
) -> Result<(), QueueError> {
    let lease_id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| QueueError::new(QueueErrorKind::Corrupt, "invalid receipt file name"))?;
    validate_id("lease_id", lease_id)?;
    validate_receipt(receipt, cli_id, lease_id)
}

fn move_leased_message_to_pending(channel_dir: &Path, source: &Path) -> Result<(), QueueError> {
    let pending_dir = channel_dir.join("pending");
    let destination = pending_dir.join(file_name_string(source)?);
    if destination.exists() {
        if private_files_equal(source, &destination)? {
            remove_private_file_if_exists(source)?;
        } else {
            move_to_quarantine(channel_dir, source)?;
        }
        return Ok(());
    }
    fs::rename(source, &destination)
        .map_err(|err| QueueError::io("return desktop message to pending queue", err))?;
    sync_dir(&pending_dir)?;
    let source_dir = source.parent().ok_or_else(|| {
        QueueError::new(
            QueueErrorKind::Corrupt,
            "leased message has no parent directory",
        )
    })?;
    sync_dir(source_dir)
}

fn private_files_equal(left: &Path, right: &Path) -> Result<bool, QueueError> {
    let mut left_file = open_private_file(left, false)?;
    let mut right_file = open_private_file(right, false)?;
    let mut left_bytes = Vec::new();
    let mut right_bytes = Vec::new();
    left_file
        .read_to_end(&mut left_bytes)
        .map_err(|err| QueueError::io("read leased desktop message", err))?;
    right_file
        .read_to_end(&mut right_bytes)
        .map_err(|err| QueueError::io("read pending desktop message", err))?;
    Ok(left_bytes == right_bytes)
}

fn prune_stale_temp_files(dir: &Path, now: u64) -> Result<(), QueueError> {
    validate_private_dir(dir)?;
    for entry in read_dir_entries(dir)? {
        let file_type = entry
            .file_type()
            .map_err(|err| QueueError::io("inspect desktop queue temporary file", err))?;
        if file_type.is_symlink() {
            return Err(QueueError::new(
                QueueErrorKind::PermissionDenied,
                "desktop queue temporary record may not be a symlink",
            ));
        }
        let file_name = entry.file_name();
        if file_type.is_file()
            && file_name.to_string_lossy().starts_with(".tmp-")
            && path_is_stale(&entry.path(), now)?
        {
            validate_private_file(&entry.path())?;
            remove_private_file_if_exists(&entry.path())?;
        }
    }
    Ok(())
}

fn path_is_stale(path: &Path, now: u64) -> Result<bool, QueueError> {
    let modified = fs::symlink_metadata(path)
        .and_then(|metadata| metadata.modified())
        .map_err(|err| QueueError::io("inspect desktop queue record timestamp", err))?;
    let modified_ms = modified
        .duration_since(UNIX_EPOCH)
        .map_err(|err| QueueError::new(QueueErrorKind::Io, err.to_string()))?
        .as_millis();
    let modified_ms = u64::try_from(modified_ms)
        .map_err(|_| QueueError::new(QueueErrorKind::Io, "record timestamp is out of range"))?;
    Ok(modified_ms.saturating_add(duration_ms(STALE_RECORD_DURATION)) <= now)
}

fn recover_expired_leases(channel_dir: &Path, cli_id: &str, now: u64) -> Result<(), QueueError> {
    let leases_dir = channel_dir.join("leases");
    for entry in read_dir_entries(&leases_dir)? {
        let file_type = entry
            .file_type()
            .map_err(|err| QueueError::io("inspect desktop message lease", err))?;
        if file_type.is_symlink() {
            return Err(QueueError::new(
                QueueErrorKind::PermissionDenied,
                "desktop message lease may not be a symlink",
            ));
        }
        if !file_type.is_dir() {
            continue;
        }
        let lease_dir = entry.path();
        validate_private_dir(&lease_dir)?;
        let lease_path = lease_dir.join("lease.json");
        let lease: LeaseMetadata = match read_json(&lease_path) {
            Ok(lease) if validate_lease(&lease_dir, &lease, cli_id).is_ok() => lease,
            Err(_) => {
                recover_corrupt_lease(channel_dir, &lease_dir, cli_id)?;
                continue;
            }
            Ok(_) => {
                recover_corrupt_lease(channel_dir, &lease_dir, cli_id)?;
                continue;
            }
        };
        let receipt_path = channel_dir
            .join("receipts")
            .join(format!("{}.json", lease.lease_id));
        if receipt_path.exists() {
            match read_json::<AckReceipt>(&receipt_path).and_then(|receipt| {
                validate_receipt(&receipt, cli_id, &lease.lease_id)?;
                Ok(receipt)
            }) {
                Ok(_) => {
                    fs::remove_dir_all(&lease_dir).map_err(|err| {
                        QueueError::io("remove acknowledged desktop message lease", err)
                    })?;
                }
                Err(_) => {
                    move_to_quarantine(channel_dir, &receipt_path)?;
                    if lease.expires_at_unix_ms <= now {
                        requeue_lease(channel_dir, &lease_dir, &lease, cli_id)?;
                    }
                }
            }
        } else if lease.expires_at_unix_ms <= now {
            requeue_lease(channel_dir, &lease_dir, &lease, cli_id)?;
        }
    }
    sync_dir(&leases_dir)?;
    sync_dir(&channel_dir.join("pending"))
}

fn recover_corrupt_lease(
    channel_dir: &Path,
    lease_dir: &Path,
    cli_id: &str,
) -> Result<(), QueueError> {
    let lease_path = lease_dir.join("lease.json");
    if lease_path.exists() {
        move_to_quarantine(channel_dir, &lease_path)?;
    }
    for path in sorted_json_files(lease_dir)? {
        match read_json::<QueuedMessage>(&path) {
            Ok(message) if validate_queued_message(&path, &message, cli_id).is_ok() => {
                move_leased_message_to_pending(channel_dir, &path)?;
            }
            Ok(_) | Err(_) => move_to_quarantine(channel_dir, &path)?,
        }
    }
    fs::remove_dir_all(lease_dir)
        .map_err(|err| QueueError::io("remove recovered desktop message lease", err))?;
    sync_dir(&channel_dir.join("pending"))?;
    sync_dir(&channel_dir.join("leases"))
}

fn requeue_lease(
    channel_dir: &Path,
    lease_dir: &Path,
    lease: &LeaseMetadata,
    cli_id: &str,
) -> Result<(), QueueError> {
    for file_name in &lease.files {
        validate_component("leased message", file_name)?;
        let source = lease_dir.join(file_name);
        if source.exists() {
            let message: QueuedMessage = read_json(&source)?;
            validate_queued_message(&source, &message, cli_id)?;
            move_leased_message_to_pending(channel_dir, &source)?;
        }
    }
    fs::remove_dir_all(lease_dir)
        .map_err(|err| QueueError::io("remove expired desktop message lease", err))?;
    sync_dir(&channel_dir.join("pending"))?;
    sync_dir(&channel_dir.join("leases"))
}

fn prune_receipts(channel_dir: &Path, cli_id: &str, now: u64) -> Result<(), QueueError> {
    let receipts_dir = channel_dir.join("receipts");
    for path in sorted_json_files(&receipts_dir)? {
        match read_json::<AckReceipt>(&path) {
            Ok(receipt)
                if validate_receipt_file(&path, &receipt, cli_id).is_ok()
                    && receipt
                        .acked_at_unix_ms
                        .saturating_add(duration_ms(RECEIPT_DURATION))
                        <= now =>
            {
                remove_private_file_if_exists(&path)?;
            }
            Ok(receipt) if validate_receipt_file(&path, &receipt, cli_id).is_ok() => {}
            Ok(_) | Err(_) => move_to_quarantine(channel_dir, &path)?,
        }
    }
    sync_dir(&receipts_dir)
}

fn queued_usage(channel_dir: &Path) -> Result<(usize, usize), QueueError> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    for path in sorted_json_files(&channel_dir.join("pending"))? {
        let metadata = fs::metadata(&path)
            .map_err(|err| QueueError::io("inspect queued desktop message", err))?;
        count = count.saturating_add(1);
        bytes = bytes.saturating_add(usize::try_from(metadata.len()).unwrap_or(usize::MAX));
    }
    let leases_dir = channel_dir.join("leases");
    for entry in read_dir_entries(&leases_dir)? {
        let file_type = entry
            .file_type()
            .map_err(|err| QueueError::io("inspect desktop message lease", err))?;
        if file_type.is_symlink() {
            return Err(QueueError::new(
                QueueErrorKind::PermissionDenied,
                "desktop message lease may not be a symlink",
            ));
        }
        if !file_type.is_dir() {
            continue;
        }
        validate_private_dir(&entry.path())?;
        for path in sorted_json_files(&entry.path())? {
            if path.file_name().and_then(|name| name.to_str()) == Some("lease.json") {
                continue;
            }
            let metadata = fs::metadata(&path)
                .map_err(|err| QueueError::io("inspect leased desktop message", err))?;
            count = count.saturating_add(1);
            bytes = bytes.saturating_add(usize::try_from(metadata.len()).unwrap_or(usize::MAX));
        }
    }
    Ok((count, bytes))
}

fn lease_message_count(leases_dir: &Path) -> Result<usize, QueueError> {
    let mut count = 0usize;
    for entry in read_dir_entries(leases_dir)? {
        let file_type = entry
            .file_type()
            .map_err(|err| QueueError::io("inspect desktop message lease", err))?;
        if file_type.is_symlink() {
            return Err(QueueError::new(
                QueueErrorKind::PermissionDenied,
                "desktop message lease may not be a symlink",
            ));
        }
        if file_type.is_dir() {
            validate_private_dir(&entry.path())?;
            count = count.saturating_add(
                sorted_json_files(&entry.path())?
                    .into_iter()
                    .filter(|path| {
                        path.file_name().and_then(|name| name.to_str()) != Some("lease.json")
                    })
                    .count(),
            );
        }
    }
    Ok(count)
}

fn directory_has_json(path: &Path) -> Result<bool, QueueError> {
    Ok(!sorted_json_files(path)?.is_empty())
}

fn directory_has_entries(path: &Path) -> Result<bool, QueueError> {
    validate_private_dir(path)?;
    Ok(!read_dir_entries(path)?.is_empty())
}

fn move_to_quarantine(channel_dir: &Path, path: &Path) -> Result<(), QueueError> {
    move_to_quarantine_dir(&channel_dir.join("quarantine"), path)
}

fn move_to_root_quarantine(root: &Path, path: &Path) -> Result<(), QueueError> {
    move_to_quarantine_dir(&root.join("quarantine"), path)
}

fn move_to_quarantine_dir(quarantine_dir: &Path, path: &Path) -> Result<(), QueueError> {
    validate_private_dir(quarantine_dir)?;
    validate_private_file(path)?;
    let file_name = file_name_string(path)?;
    let destination = quarantine_dir.join(format!("{}-{file_name}", random_hex(8)?));
    let source_dir = path.parent().ok_or_else(|| {
        QueueError::new(
            QueueErrorKind::Corrupt,
            "queue file has no parent directory",
        )
    })?;
    fs::rename(path, destination)
        .map_err(|err| QueueError::io("quarantine corrupt desktop message", err))?;
    sync_dir(quarantine_dir)?;
    if source_dir != quarantine_dir {
        sync_dir(source_dir)?;
    }
    Ok(())
}

fn atomic_json<T: Serialize>(dir: &Path, file_name: &str, value: &T) -> Result<(), QueueError> {
    validate_private_dir(dir)?;
    validate_component("queue file", file_name)?;
    let target = dir.join(file_name);
    if target.exists() {
        validate_private_file(&target)?;
    }
    let bytes = serde_json::to_vec(value)
        .map_err(|err| QueueError::new(QueueErrorKind::Corrupt, err.to_string()))?;
    let tmp_name = format!(".tmp-{}", random_hex(12)?);
    let tmp_path = dir.join(tmp_name);
    let mut file = create_private_file(&tmp_path)?;
    file.write_all(&bytes)
        .map_err(|err| QueueError::io("write desktop queue file", err))?;
    file.sync_all()
        .map_err(|err| QueueError::io("sync desktop queue file", err))?;
    drop(file);
    fs::rename(&tmp_path, &target)
        .map_err(|err| QueueError::io("publish desktop queue file", err))?;
    sync_dir(dir)
}

fn atomic_message(channel_dir: &Path, file_name: &str, bytes: &[u8]) -> Result<(), QueueError> {
    validate_component("queued message", file_name)?;
    let tmp_dir = channel_dir.join("tmp");
    let pending_dir = channel_dir.join("pending");
    validate_private_dir(&tmp_dir)?;
    validate_private_dir(&pending_dir)?;
    let tmp_path = tmp_dir.join(format!(".tmp-{}", random_hex(12)?));
    let mut file = create_private_file(&tmp_path)?;
    file.write_all(bytes)
        .map_err(|err| QueueError::io("write desktop queue message", err))?;
    file.sync_all()
        .map_err(|err| QueueError::io("sync desktop queue message", err))?;
    drop(file);
    fs::rename(&tmp_path, pending_dir.join(file_name))
        .map_err(|err| QueueError::io("publish desktop queue message", err))?;
    sync_dir(&pending_dir)?;
    sync_dir(&tmp_dir)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, QueueError> {
    let mut file = open_private_file(path, false)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| QueueError::io("read desktop queue file", err))?;
    serde_json::from_slice(&bytes).map_err(|err| {
        QueueError::new(
            QueueErrorKind::Corrupt,
            format!("invalid desktop queue file {}: {err}", path.display()),
        )
    })
}

fn ensure_private_dir(path: &Path) -> Result<(), QueueError> {
    match create_private_dir(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == QueueErrorKind::Io && path.exists() => validate_private_dir(path),
        Err(err) => Err(err),
    }
}

fn create_private_dir(path: &Path) -> Result<(), QueueError> {
    let mut builder = DirBuilder::new();
    builder.mode(PRIVATE_DIR_MODE);
    builder.create(path).map_err(|err| {
        QueueError::io(&format!("create private directory {}", path.display()), err)
    })?;
    validate_private_dir(path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn validate_private_dir(path: &Path) -> Result<(), QueueError> {
    let metadata = fs::symlink_metadata(path).map_err(|err| {
        let kind = if err.kind() == io::ErrorKind::NotFound {
            QueueErrorKind::NotFound
        } else if err.kind() == io::ErrorKind::PermissionDenied {
            QueueErrorKind::PermissionDenied
        } else {
            QueueErrorKind::Io
        };
        QueueError::new(
            kind,
            format!("inspect private directory {}: {err}", path.display()),
        )
    })?;
    if !metadata.file_type().is_dir()
        || metadata.uid() != effective_uid()
        || metadata.mode() & 0o777 != PRIVATE_DIR_MODE
    {
        return Err(QueueError::new(
            QueueErrorKind::PermissionDenied,
            format!(
                "private directory {} must be owned by the current user with mode 0700",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn create_private_file(path: &Path) -> Result<File, QueueError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(PRIVATE_FILE_MODE)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|err| QueueError::io(&format!("create private file {}", path.display()), err))?;
    validate_open_private_file(path, &file)?;
    Ok(file)
}

fn open_private_file(path: &Path, create: bool) -> Result<File, QueueError> {
    if create {
        match create_private_file(path) {
            Ok(file) => return Ok(file),
            Err(_err) if path.exists() => {}
            Err(err) => return Err(err),
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|err| QueueError::io(&format!("open private file {}", path.display()), err))?;
    validate_open_private_file(path, &file)?;
    Ok(file)
}

fn validate_private_file(path: &Path) -> Result<(), QueueError> {
    let file = open_private_file(path, false)?;
    validate_open_private_file(path, &file)
}

fn validate_open_private_file(path: &Path, file: &File) -> Result<(), QueueError> {
    let metadata = file
        .metadata()
        .map_err(|err| QueueError::io("inspect private queue file", err))?;
    if !metadata.file_type().is_file()
        || metadata.uid() != effective_uid()
        || metadata.mode() & 0o777 != PRIVATE_FILE_MODE
        || metadata.nlink() != 1
    {
        return Err(QueueError::new(
            QueueErrorKind::PermissionDenied,
            format!(
                "private file {} must be regular, singly linked, current-user owned, and mode 0600",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn remove_private_file_if_exists(path: &Path) -> Result<(), QueueError> {
    match fs::remove_file(path) {
        Ok(()) => {
            if let Some(parent) = path.parent() {
                sync_dir(parent)?;
            }
            Ok(())
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(QueueError::io("remove private desktop queue file", err)),
    }
}

fn sync_dir(path: &Path) -> Result<(), QueueError> {
    File::open(path)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| QueueError::io(&format!("sync directory {}", path.display()), err))
}

fn sorted_json_files(path: &Path) -> Result<Vec<PathBuf>, QueueError> {
    validate_private_dir(path)?;
    let mut files = Vec::new();
    for entry in read_dir_entries(path)? {
        let file_type = entry
            .file_type()
            .map_err(|err| QueueError::io("inspect desktop queue directory entry", err))?;
        if file_type.is_symlink() {
            return Err(QueueError::new(
                QueueErrorKind::PermissionDenied,
                format!(
                    "desktop queue entry {} may not be a symlink",
                    entry.path().display()
                ),
            ));
        }
        let is_json = entry.path().extension().and_then(|ext| ext.to_str()) == Some("json");
        if file_type.is_file() && is_json {
            validate_private_file(&entry.path())?;
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

fn read_dir_entries(path: &Path) -> Result<Vec<fs::DirEntry>, QueueError> {
    fs::read_dir(path)
        .map_err(|err| QueueError::io(&format!("read directory {}", path.display()), err))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| QueueError::io(&format!("read directory {}", path.display()), err))
}

fn file_name_string(path: &Path) -> Result<String, QueueError> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
        .ok_or_else(|| QueueError::new(QueueErrorKind::Corrupt, "queue file name is not UTF-8"))
}

fn channel_basename(path: &Path) -> Result<String, QueueError> {
    file_name_string(path)
}

fn validate_id(label: &str, value: &str) -> Result<(), QueueError> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(QueueError::new(
            QueueErrorKind::Invalid,
            format!("{label} is invalid"),
        ));
    }
    Ok(())
}

fn validate_component(label: &str, value: &str) -> Result<(), QueueError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\0')
    {
        return Err(QueueError::new(
            QueueErrorKind::Corrupt,
            format!("{label} contains an invalid path component"),
        ));
    }
    Ok(())
}

fn random_hex(bytes: usize) -> Result<String, QueueError> {
    let mut random = vec![0u8; bytes];
    getrandom::fill(&mut random).map_err(|err| {
        QueueError::new(QueueErrorKind::Io, format!("secure random failed: {err}"))
    })?;
    Ok(hex_bytes(&random))
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn absolute_path(path: PathBuf) -> Result<PathBuf, QueueError> {
    if path.is_absolute() {
        Ok(path)
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(|err| QueueError::io("resolve active Herdr socket path", err))
    }
}

fn unix_ms() -> Result<u64, QueueError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| QueueError::new(QueueErrorKind::Io, err.to_string()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| QueueError::new(QueueErrorKind::Io, "system clock is out of range"))
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn effective_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions and does not mutate memory.
    unsafe { libc::geteuid() }
}

struct PrivateFileLock {
    file: File,
    _in_process: InProcessLockGuard,
}

#[derive(Default)]
struct InProcessLockState {
    owner: Option<std::thread::ThreadId>,
    depth: usize,
}

struct InProcessLockGuard;

fn in_process_lock() -> &'static (Mutex<InProcessLockState>, Condvar) {
    static LOCK: OnceLock<(Mutex<InProcessLockState>, Condvar)> = OnceLock::new();
    LOCK.get_or_init(|| (Mutex::new(InProcessLockState::default()), Condvar::new()))
}

impl InProcessLockGuard {
    fn acquire() -> Self {
        let current = std::thread::current().id();
        let (mutex, ready) = in_process_lock();
        let mut state = mutex.lock().unwrap_or_else(|err| err.into_inner());
        loop {
            match state.owner.as_ref() {
                None => {
                    state.owner = Some(current);
                    state.depth = 1;
                    return Self;
                }
                Some(owner) if owner == &current => {
                    state.depth = state.depth.saturating_add(1);
                    return Self;
                }
                Some(_) => {
                    state = ready.wait(state).unwrap_or_else(|err| err.into_inner());
                }
            }
        }
    }
}

impl Drop for InProcessLockGuard {
    fn drop(&mut self) {
        let current = std::thread::current().id();
        let (mutex, ready) = in_process_lock();
        let mut state = mutex.lock().unwrap_or_else(|err| err.into_inner());
        if state.owner.as_ref() != Some(&current) {
            return;
        }
        state.depth = state.depth.saturating_sub(1);
        if state.depth == 0 {
            state.owner = None;
            ready.notify_one();
        }
    }
}

impl PrivateFileLock {
    fn acquire(path: &Path) -> Result<Self, QueueError> {
        let file = open_private_file(path, true)?;
        let in_process = InProcessLockGuard::acquire();
        loop {
            // SAFETY: `file` owns a valid descriptor for the duration of the call.
            let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
            if result == 0 {
                return Ok(Self {
                    file,
                    _in_process: in_process,
                });
            }
            let err = io::Error::last_os_error();
            if err.kind() != io::ErrorKind::Interrupted {
                return Err(QueueError::io("lock desktop queue", err));
            }
        }
    }
}

impl Drop for PrivateFileLock {
    fn drop(&mut self) {
        // SAFETY: `file` remains open until after this drop implementation returns.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root(name: &str) -> PathBuf {
        let nonce = unix_ms().unwrap();
        std::env::temp_dir().join(format!(
            "herdr-desktop-queue-{name}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn active_identity(cli_id: &str) -> CliIdentity {
        CliIdentity {
            cli_id: cli_id.to_string(),
            pane_id: "w1:p1".to_string(),
            terminal_id: "terminal-1".to_string(),
            workspace_id: "w1".to_string(),
            tab_id: "w1:t1".to_string(),
            name: "worker".to_string(),
            cwd: "/tmp".to_string(),
            state: CliState::Active,
            launch_marker: None,
        }
    }

    fn install_capability(handle: &ChannelHandle) {
        for (key, value) in &handle.env {
            std::env::set_var(key, value);
        }
    }

    fn clear_capability(handle: &ChannelHandle) {
        for (key, _) in &handle.env {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn offline_enqueue_drain_and_idempotent_ack() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("roundtrip");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        let identity = manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);

        let receipt = enqueue_from_env("result", Some("turn-1"), "done".to_string()).unwrap();
        assert_eq!(receipt.cli_id, identity.cli_id);
        let drain = manager.drain(&identity.cli_id, 20).unwrap();
        assert_eq!(drain.messages.len(), 1);
        assert_eq!(drain.messages[0].text, "done");
        let ack = manager.ack(&identity.cli_id, &drain.lease_id).unwrap();
        assert_eq!(ack.deleted, 1);
        assert!(!ack.already_acked);
        assert!(
            manager
                .ack(&identity.cli_id, &drain.lease_id)
                .unwrap()
                .already_acked
        );

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn creating_channel_persists_marker_and_can_be_reconciled() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("reconcile-marker");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();

        let creating = manager.lookup_cli(&handle.cli_id).unwrap();
        assert_eq!(creating.state, CliState::Creating);
        assert_eq!(
            creating.launch_marker.as_deref(),
            Some(handle.launch_marker.as_str())
        );
        assert_eq!(
            manager
                .activate_reconciled(
                    &handle.cli_id,
                    "herdr_mcp_wrong",
                    active_identity(&handle.cli_id),
                )
                .unwrap_err()
                .kind(),
            QueueErrorKind::Corrupt
        );

        let active = manager
            .activate_reconciled(
                &handle.cli_id,
                &handle.launch_marker,
                active_identity(&handle.cli_id),
            )
            .unwrap();
        assert_eq!(active.state, CliState::Active);
        assert_eq!(
            active.launch_marker.as_deref(),
            Some(handle.launch_marker.as_str())
        );
        assert_eq!(manager.lookup_cli(&handle.cli_id).unwrap(), active);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bounded_drain_leases_only_fitting_messages_and_release_restores_fifo() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("bounded-drain");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        enqueue_from_env("result", None, "first".to_string()).unwrap();
        enqueue_from_env("result", None, "second".to_string()).unwrap();

        let first = manager
            .drain_bounded(&handle.cli_id, 20, |candidate| {
                candidate.messages.len() <= 1
            })
            .unwrap();
        assert_eq!(first.messages.len(), 1);
        assert_eq!(first.messages[0].text, "first");
        assert_eq!(first.remaining, 1);
        manager.release(&handle.cli_id, &first.lease_id).unwrap();

        let replay = manager.drain(&handle.cli_id, 20).unwrap();
        assert_eq!(
            replay
                .messages
                .iter()
                .map(|message| message.text.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn capability_must_match_cli_metadata() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("capability");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        install_capability(&handle);
        std::env::set_var(CLI_ID_ENV, "cli_wrong");

        let err = enqueue_from_env("info", None, "hello".to_string()).unwrap_err();
        assert_eq!(err.kind(), QueueErrorKind::PermissionDenied);

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_producers_are_fifo_and_unique() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("concurrent");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);

        let threads = (0..8)
            .map(|index| {
                std::thread::spawn(move || {
                    enqueue_from_env("progress", None, format!("message-{index}"))
                        .unwrap()
                        .sequence
                })
            })
            .collect::<Vec<_>>();
        let mut sequences = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=8).collect::<Vec<_>>());

        let drain = manager.drain(&handle.cli_id, 20).unwrap();
        assert_eq!(drain.messages.len(), 8);
        assert!(drain
            .messages
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn expired_lease_requeues_messages() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("expired");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        enqueue_from_env("question", None, "retry me".to_string()).unwrap();

        let first = manager.drain(&handle.cli_id, 1).unwrap();
        let lease_dir = handle.channel_dir.join("leases").join(&first.lease_id);
        let mut lease: LeaseMetadata = read_json(&lease_dir.join("lease.json")).unwrap();
        lease.expires_at_unix_ms = 0;
        atomic_json(&lease_dir, "lease.json", &lease).unwrap();

        let second = manager.drain(&handle.cli_id, 1).unwrap();
        assert_eq!(second.messages.len(), 1);
        assert_eq!(second.messages[0].text, "retry me");
        assert_ne!(first.lease_id, second.lease_id);

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_messages_are_quarantined() {
        let root = test_root("corrupt");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        let corrupt_path = handle
            .channel_dir
            .join("pending/00000000000000000001-bad.json");
        let mut corrupt = create_private_file(&corrupt_path).unwrap();
        corrupt.write_all(b"not-json").unwrap();
        corrupt.sync_all().unwrap();

        let drain = manager.drain(&handle.cli_id, 20).unwrap();
        assert!(drain.messages.is_empty());
        assert_eq!(drain.quarantined, 1);
        assert_eq!(
            sorted_json_files(&handle.channel_dir.join("quarantine"))
                .unwrap()
                .len(),
            1
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn wrong_directory_mode_is_rejected() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("mode");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        install_capability(&handle);
        fs::set_permissions(&handle.channel_dir, fs::Permissions::from_mode(0o755)).unwrap();

        let err = enqueue_from_env("info", None, "hello".to_string()).unwrap_err();
        assert_eq!(err.kind(), QueueErrorKind::PermissionDenied);

        fs::set_permissions(
            &handle.channel_dir,
            fs::Permissions::from_mode(PRIVATE_DIR_MODE),
        )
        .unwrap();
        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn private_directory_symlink_is_rejected() {
        use std::os::unix::fs::symlink;

        let root = test_root("symlink");
        let target = test_root("symlink-target");
        fs::create_dir_all(&target).unwrap();
        symlink(&target, &root).unwrap();

        let err = QueueManager::for_test(root.clone()).unwrap_err();
        assert_eq!(err.kind(), QueueErrorKind::PermissionDenied);

        fs::remove_file(root).unwrap();
        fs::remove_dir_all(target).unwrap();
    }

    #[test]
    fn corrupt_lease_metadata_requeues_without_losing_messages() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("corrupt-lease");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        enqueue_from_env("result", None, "first".to_string()).unwrap();
        enqueue_from_env("result", None, "second".to_string()).unwrap();

        let first = manager.drain(&handle.cli_id, 2).unwrap();
        let lease_dir = handle.channel_dir.join("leases").join(&first.lease_id);
        let mut lease: LeaseMetadata = read_json(&lease_dir.join("lease.json")).unwrap();
        lease.files.clear();
        lease.expires_at_unix_ms = 0;
        atomic_json(&lease_dir, "lease.json", &lease).unwrap();

        let recovered = manager.drain(&handle.cli_id, 2).unwrap();
        assert_eq!(
            recovered
                .messages
                .iter()
                .map(|message| message.text.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            sorted_json_files(&handle.channel_dir.join("quarantine"))
                .unwrap()
                .len(),
            1
        );

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn closed_channel_retains_live_ack_receipt() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("receipt-retention");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        enqueue_from_env("result", None, "done".to_string()).unwrap();
        let lease = manager.drain(&handle.cli_id, 1).unwrap();
        manager.ack(&handle.cli_id, &lease.lease_id).unwrap();
        manager.mark_closed(&handle.cli_id).unwrap();

        manager.create_channel("next", Path::new("/tmp")).unwrap();
        let repeated = manager.ack(&handle.cli_id, &lease.lease_id).unwrap();
        assert!(repeated.already_acked);
        assert_eq!(repeated.deleted, 1);

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restart_reconciles_split_identity_update() {
        let root = test_root("reconcile");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        let expected = manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        let mut stale = manager.read_index(&handle.cli_id).unwrap();
        stale.identity =
            placeholder_identity(&read_json(&handle.channel_dir.join("channel.json")).unwrap());
        stale.identity.state = CliState::Creating;
        atomic_json(
            &manager.root.join("index"),
            &format!("{}.json", handle.cli_id),
            &stale,
        )
        .unwrap();

        let restarted = QueueManager::for_test(root.clone()).unwrap();
        assert_eq!(restarted.lookup_cli(&handle.cli_id).unwrap(), expected);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn queued_messages_survive_manager_restart() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("restart-message");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        enqueue_from_env("result", None, "persisted".to_string()).unwrap();
        drop(manager);

        let restarted = QueueManager::for_test(root.clone()).unwrap();
        let lease = restarted.drain(&handle.cli_id, 20).unwrap();
        assert_eq!(lease.messages.len(), 1);
        assert_eq!(lease.messages[0].text, "persisted");

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn restart_quarantines_and_rebuilds_corrupt_index() {
        let root = test_root("corrupt-index");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        let expected = manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        let index_path = manager.index_path(&handle.cli_id);
        let mut file = open_private_file(&index_path, false).unwrap();
        file.set_len(0).unwrap();
        file.write_all(b"not-json").unwrap();
        file.sync_all().unwrap();

        let restarted = QueueManager::for_test(root.clone()).unwrap();
        assert_eq!(restarted.lookup_cli(&handle.cli_id).unwrap(), expected);
        assert_eq!(
            sorted_json_files(&root.join("quarantine")).unwrap().len(),
            1
        );

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn stale_temporary_and_creating_records_are_pruned() {
        let root = test_root("stale-records");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        let tmp_path = handle.channel_dir.join("tmp/.tmp-abandoned");
        create_private_file(&tmp_path).unwrap().sync_all().unwrap();
        prune_stale_temp_files(
            &handle.channel_dir.join("tmp"),
            unix_ms()
                .unwrap()
                .saturating_add(duration_ms(STALE_RECORD_DURATION) + 1),
        )
        .unwrap();
        assert!(!tmp_path.exists());

        let mut metadata: ChannelMetadata =
            read_json(&handle.channel_dir.join("channel.json")).unwrap();
        metadata.updated_at_unix_ms = 0;
        atomic_json(&handle.channel_dir, "channel.json", &metadata).unwrap();
        remove_private_file_if_exists(&manager.index_path(&handle.cli_id)).unwrap();

        let restarted = QueueManager::for_test(root.clone()).unwrap();
        assert!(handle.channel_dir.exists());
        assert_eq!(
            restarted
                .lookup_cli(&handle.cli_id)
                .unwrap()
                .launch_marker
                .as_deref(),
            Some(handle.launch_marker.as_str())
        );

        let pruned = restarted.prune_stale_creating(&HashSet::new()).unwrap();
        assert_eq!(pruned.as_slice(), std::slice::from_ref(&handle.cli_id));
        assert!(!handle.channel_dir.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn confirmed_live_marker_and_nonempty_channel_block_stale_pruning() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("stale-live-marker");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let live = manager.create_channel("live", Path::new("/tmp")).unwrap();
        let mut live_metadata: ChannelMetadata =
            read_json(&live.channel_dir.join("channel.json")).unwrap();
        live_metadata.updated_at_unix_ms = 0;
        atomic_json(&live.channel_dir, "channel.json", &live_metadata).unwrap();

        let live_markers = HashSet::from([live.launch_marker.clone()]);
        assert!(manager
            .prune_stale_creating(&live_markers)
            .unwrap()
            .is_empty());
        assert!(live.channel_dir.exists());

        let queued = manager.create_channel("queued", Path::new("/tmp")).unwrap();
        install_capability(&queued);
        enqueue_from_env("info", None, "preserve me".to_string()).unwrap();
        clear_capability(&queued);
        let mut queued_metadata: ChannelMetadata =
            read_json(&queued.channel_dir.join("channel.json")).unwrap();
        queued_metadata.updated_at_unix_ms = 0;
        atomic_json(&queued.channel_dir, "channel.json", &queued_metadata).unwrap();

        assert!(manager
            .prune_stale_creating(&live_markers)
            .unwrap()
            .is_empty());
        assert!(queued.channel_dir.exists());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn markerless_stale_creating_record_is_pruned_during_maintenance() {
        let root = test_root("stale-markerless");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("legacy", Path::new("/tmp")).unwrap();
        let mut metadata: ChannelMetadata =
            read_json(&handle.channel_dir.join("channel.json")).unwrap();
        metadata.updated_at_unix_ms = 0;
        metadata.launch_marker = None;
        if let Some(identity) = metadata.identity.as_mut() {
            identity.launch_marker = None;
        }
        atomic_json(&handle.channel_dir, "channel.json", &metadata).unwrap();

        QueueManager::for_test(root.clone()).unwrap();
        assert!(!handle.channel_dir.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn serialized_byte_quota_does_not_overshoot() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("byte-quota");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        let filler_path = handle
            .channel_dir
            .join("pending/00000000000000000000-filler.json");
        let filler = create_private_file(&filler_path).unwrap();
        filler.set_len((MAX_CHANNEL_BYTES - 1) as u64).unwrap();
        filler.sync_all().unwrap();

        let before = queued_usage(&handle.channel_dir).unwrap();
        let err = enqueue_from_env("info", None, "x".to_string()).unwrap_err();
        assert_eq!(err.kind(), QueueErrorKind::Full);
        assert_eq!(queued_usage(&handle.channel_dir).unwrap(), before);

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_producers_and_drainer_do_not_duplicate_messages() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("producer-drainer");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);

        let complete = Arc::new(AtomicBool::new(false));
        let drain_complete = Arc::clone(&complete);
        let drain_manager = manager.clone();
        let drain_cli_id = handle.cli_id.clone();
        let drainer = std::thread::spawn(move || {
            let mut sequences = Vec::new();
            loop {
                // Sample completion before taking the queue lock. A lease that
                // began before the producers finished can legitimately report
                // zero remaining even though a producer enqueues immediately
                // after it releases the lock.
                let producers_done = drain_complete.load(Ordering::Acquire);
                let lease = drain_manager.drain(&drain_cli_id, 7).unwrap();
                sequences.extend(lease.messages.iter().map(|message| message.sequence));
                drain_manager.ack(&drain_cli_id, &lease.lease_id).unwrap();
                if producers_done && lease.remaining == 0 {
                    assert_eq!(sequences.len(), 40);
                    return sequences;
                }
                std::thread::yield_now();
            }
        });
        let producers = (0..4)
            .map(|producer| {
                std::thread::spawn(move || {
                    for message in 0..10 {
                        enqueue_from_env(
                            "progress",
                            None,
                            format!("producer-{producer}-{message}"),
                        )
                        .unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        for producer in producers {
            producer.join().unwrap();
        }
        complete.store(true, Ordering::Release);
        let mut sequences = drainer.join().unwrap();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=40).collect::<Vec<_>>());

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_drainers_lease_each_message_once() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("concurrent-drainers");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        for index in 0..40 {
            enqueue_from_env("progress", None, format!("message-{index}")).unwrap();
        }

        let drainers = (0..2)
            .map(|_| {
                let drain_manager = manager.clone();
                let cli_id = handle.cli_id.clone();
                std::thread::spawn(move || {
                    let mut sequences = Vec::new();
                    loop {
                        let lease = drain_manager.drain(&cli_id, 3).unwrap();
                        let empty = lease.messages.is_empty();
                        sequences.extend(lease.messages.iter().map(|message| message.sequence));
                        drain_manager.ack(&cli_id, &lease.lease_id).unwrap();
                        if empty {
                            return sequences;
                        }
                    }
                })
            })
            .collect::<Vec<_>>();
        let mut sequences = drainers
            .into_iter()
            .flat_map(|drainer| drainer.join().unwrap())
            .collect::<Vec<_>>();
        sequences.sort_unstable();
        assert_eq!(sequences, (1..=40).collect::<Vec<_>>());

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn expired_receipt_allows_empty_closed_channel_pruning() {
        let _guard = crate::config::test_config_env_lock().lock().unwrap();
        let root = test_root("expired-receipt");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        install_capability(&handle);
        enqueue_from_env("result", None, "done".to_string()).unwrap();
        let lease = manager.drain(&handle.cli_id, 1).unwrap();
        manager.ack(&handle.cli_id, &lease.lease_id).unwrap();
        manager.mark_closed(&handle.cli_id).unwrap();
        let receipt_path = handle
            .channel_dir
            .join("receipts")
            .join(format!("{}.json", lease.lease_id));
        let mut receipt: AckReceipt = read_json(&receipt_path).unwrap();
        receipt.acked_at_unix_ms = 0;
        atomic_json(
            &handle.channel_dir.join("receipts"),
            &format!("{}.json", lease.lease_id),
            &receipt,
        )
        .unwrap();

        manager.create_channel("next", Path::new("/tmp")).unwrap();
        assert_eq!(
            manager.lookup_cli(&handle.cli_id).unwrap_err().kind(),
            QueueErrorKind::CliNotFound
        );

        clear_capability(&handle);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn interior_symlinks_are_rejected() {
        use std::os::unix::fs::symlink;

        let root = test_root("interior-symlink");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        manager
            .activate_channel(&handle, active_identity(&handle.cli_id))
            .unwrap();
        symlink(
            handle.channel_dir.join("channel.json"),
            handle.channel_dir.join("pending/linked.json"),
        )
        .unwrap();

        let err = manager.drain(&handle.cli_id, 1).unwrap_err();
        assert_eq!(err.kind(), QueueErrorKind::PermissionDenied);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn queue_files_and_directories_are_private() {
        let root = test_root("permissions");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let handle = manager.create_channel("worker", Path::new("/tmp")).unwrap();
        for directory in [
            handle.channel_dir.clone(),
            handle.channel_dir.join("tmp"),
            handle.channel_dir.join("pending"),
            handle.channel_dir.join("leases"),
            handle.channel_dir.join("receipts"),
            handle.channel_dir.join("quarantine"),
        ] {
            assert_eq!(
                fs::symlink_metadata(directory).unwrap().mode() & 0o777,
                0o700
            );
        }
        for file in [
            handle.channel_dir.join("channel.json"),
            handle.channel_dir.join("queue.lock"),
            manager.index_path(&handle.cli_id),
            manager.root.join("spool.lock"),
        ] {
            assert_eq!(fs::symlink_metadata(file).unwrap().mode() & 0o777, 0o600);
        }

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn channel_quota_rejects_the_thirty_third_channel() {
        let root = test_root("channel-quota");
        let manager = QueueManager::for_test(root.clone()).unwrap();
        let mut handles = Vec::new();
        for index in 0..MAX_CHANNELS {
            handles.push(
                manager
                    .create_channel(&format!("worker-{index}"), Path::new("/tmp"))
                    .unwrap(),
            );
        }
        let err = manager
            .create_channel("one-too-many", Path::new("/tmp"))
            .unwrap_err();
        assert_eq!(err.kind(), QueueErrorKind::Full);

        drop(handles);
        fs::remove_dir_all(root).unwrap();
    }
}
