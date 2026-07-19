//! Durable recovery records for automatic remote-terminal parking and resume.
//!
//! Resume records are session-scoped. Recovery identity is installation-scoped
//! so a deleted session cannot accidentally delete the credentials needed to
//! discover terminals that may still be parked on a remote Herdr server.

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const RESUME_SCHEMA_VERSION: u32 = 2;

const LEGACY_RESUME_SCHEMA_VERSION: u32 = 1;
const RECOVERY_IDENTITY_SCHEMA_VERSION: u32 = 1;
const PARKED_ADMIN_TOKEN_SCHEMA_VERSION: u32 = 1;
const PRIVATE_DIR_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;
const STALE_TEMP_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const RANDOM_RECOVERY_ID_BYTES: usize = 16;
const RANDOM_TOKEN_BYTES: usize = 32;
const RECOVERY_IDENTITY_LOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// A persisted bearer credential whose debug representation never contains
/// the credential. Callers must opt in explicitly to use the clear value.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub(crate) struct SecretToken(String);

impl SecretToken {
    #[cfg(test)]
    pub(crate) fn new(value: String) -> io::Result<Self> {
        if value.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "recovery token must not be empty",
            ));
        }
        Ok(Self(value))
    }

    pub(crate) fn expose_secret(&self) -> &str {
        &self.0
    }

    pub(crate) fn generate() -> io::Result<Self> {
        Ok(Self(random_hex(RANDOM_TOKEN_BYTES)?))
    }

    fn is_valid(&self) -> bool {
        !self.0.is_empty()
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretToken([REDACTED])")
    }
}

/// SSH coordinates needed to reach the remote host again. Never includes
/// managed control paths; those are per-process and meaningless after stop.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeSsh {
    pub(crate) target: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) ssh_args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) session: Option<String>,
}

/// Display metadata for the delegated pane, used by recovery listings and to
/// restore pane labels.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeAgent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cwd: Option<String>,
}

/// Where the local owner pane lived when it was parked.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumePlacement {
    pub(crate) workspace_id: String,
    pub(crate) public_tab_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) public_pane_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) pane_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeRecord {
    pub(crate) schema: u32,
    pub(crate) remote_terminal_id: String,
    pub(crate) remote_pane_id: String,
    pub(crate) peer_id: String,
    pub(crate) ssh: ResumeSsh,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) agent: Option<ResumeAgent>,
    pub(crate) placement: ResumePlacement,
    pub(crate) handed_off_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) last_error: Option<String>,
}

impl ResumeRecord {
    fn is_valid(&self) -> bool {
        self.schema == RESUME_SCHEMA_VERSION
            && !self.remote_terminal_id.is_empty()
            && !self.remote_pane_id.is_empty()
            && !self.peer_id.is_empty()
            && !self.ssh.target.is_empty()
            && !self.placement.workspace_id.is_empty()
            && !self.placement.public_tab_id.is_empty()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ResumeLifecycle {
    /// The ticket and credentials are durable, but the park response has not
    /// been observed. Status reconciliation determines the actual remote state.
    ParkingPending,
    /// The remote server confirmed that the terminal is hidden and parked.
    Parked,
    /// The owner requested termination, but remote confirmation has not yet
    /// been observed. Keep the recovery capability until that succeeds.
    TerminationPending,
    /// Placement succeeded locally, but deleting the durable ticket did not.
    PlacedCleanupPending,
    /// Schema-v1 records used the former visible handoff protocol.
    LegacyVisibleHandoff,
}

impl ResumeLifecycle {
    pub(crate) fn may_leave_remote_terminal(self) -> bool {
        !matches!(self, Self::PlacedCleanupPending)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeRecoveryState {
    pub(crate) lifecycle: ResumeLifecycle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) park_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) resume_token: Option<SecretToken>,
}

impl Default for ResumeRecoveryState {
    fn default() -> Self {
        Self {
            lifecycle: ResumeLifecycle::LegacyVisibleHandoff,
            park_id: None,
            resume_token: None,
        }
    }
}

impl ResumeRecoveryState {
    fn parking(park_id: String, resume_token: SecretToken) -> io::Result<Self> {
        let state = Self {
            lifecycle: ResumeLifecycle::ParkingPending,
            park_id: Some(park_id),
            resume_token: Some(resume_token),
        };
        if state.is_valid() {
            Ok(state)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "parking recovery credentials must not be empty",
            ))
        }
    }

    fn is_valid(&self) -> bool {
        let credentials_are_valid = match (&self.park_id, &self.resume_token) {
            (None, None) => true,
            (Some(park_id), Some(token)) => !park_id.is_empty() && token.is_valid(),
            _ => false,
        };
        if !credentials_are_valid {
            return false;
        }
        match self.lifecycle {
            ResumeLifecycle::ParkingPending
            | ResumeLifecycle::Parked
            | ResumeLifecycle::TerminationPending
            | ResumeLifecycle::PlacedCleanupPending => {
                self.park_id.is_some() && self.resume_token.is_some()
            }
            ResumeLifecycle::LegacyVisibleHandoff => {
                self.park_id.is_none() && self.resume_token.is_none()
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ResumeFileV1 {
    #[serde(default)]
    schema: u32,
    #[serde(default)]
    records: Vec<ResumeRecord>,
}

#[derive(Serialize, Deserialize)]
struct ResumeFileV2 {
    schema: u32,
    #[serde(default)]
    records: Vec<StoredResumeRecord>,
}

#[derive(Serialize, Deserialize)]
struct StoredResumeRecord {
    #[serde(flatten)]
    record: ResumeRecord,
    #[serde(default)]
    recovery: ResumeRecoveryState,
}

#[derive(Default)]
struct StoreContents {
    records: Vec<ResumeRecord>,
    recovery: BTreeMap<String, ResumeRecoveryState>,
}

/// Session-scoped, transactionally persisted store of resume records.
pub(crate) struct ResumeStore {
    path: PathBuf,
    records: Vec<ResumeRecord>,
    recovery: BTreeMap<String, ResumeRecoveryState>,
}

impl ResumeStore {
    /// Open the store for the active session, including an explicit socket
    /// override when one is in effect.
    pub(crate) fn for_active_session() -> io::Result<Self> {
        Self::for_socket_path(crate::session::active_api_socket_path())
    }

    /// Open the store for an arbitrary named session. `None` and `default`
    /// address the default session without consulting process-global session
    /// environment state.
    pub(crate) fn for_session(name: Option<&str>) -> io::Result<Self> {
        let name = match name {
            Some(crate::session::DEFAULT_SESSION_NAME) | None => None,
            Some(name) => {
                crate::session::validate_name(name).map_err(io::Error::other)?;
                Some(name)
            }
        };
        Self::for_socket_path(crate::session::api_socket_path_for(name))
    }

    fn for_socket_path(socket_path: PathBuf) -> io::Result<Self> {
        let socket_path = absolute_path(socket_path)?;
        let session_key = session_key(&socket_path);
        let root = crate::config::state_dir().join("remote-resume");
        let current_root = root.join("v2");
        ensure_private_dir(&current_root)?;
        let current_path = current_root.join(format!("{session_key}.json"));
        let legacy_path = root.join("v1").join(format!("{session_key}.json"));
        Self::open_with_legacy(current_path, legacy_path)
    }

    /// Open a store at an explicit path. Primarily for tests.
    pub(crate) fn open(path: PathBuf) -> io::Result<Self> {
        cleanup_temp_files(&path);
        let Some(bytes) = read_private_file(&path)? else {
            return Ok(Self {
                path,
                records: Vec::new(),
                recovery: BTreeMap::new(),
            });
        };
        let (contents, migrated) = load_contents(&path, &bytes)?;
        let store = Self {
            path,
            records: contents.records,
            recovery: contents.recovery,
        };
        if migrated {
            store.persist(&store.records, &store.recovery)?;
        }
        Ok(store)
    }

    fn open_with_legacy(path: PathBuf, legacy_path: PathBuf) -> io::Result<Self> {
        if path.exists() || !legacy_path.exists() {
            return Self::open(path);
        }
        let mut store = Self::open(legacy_path.clone())?;
        store.path = path;
        store.persist(&store.records, &store.recovery)?;
        match fs::remove_file(&legacy_path) {
            Ok(()) => sync_dir(legacy_path.parent())?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::warn!(
                path = %legacy_path.display(),
                error = %err,
                "migrated remote resume store but could not remove the legacy file"
            ),
        }
        Ok(store)
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn records(&self) -> &[ResumeRecord] {
        &self.records
    }

    pub(crate) fn recovery_state(&self, remote_terminal_id: &str) -> Option<&ResumeRecoveryState> {
        self.recovery.get(remote_terminal_id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn recoverable_count(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn potential_orphan_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| {
                self.recovery
                    .get(&record.remote_terminal_id)
                    .is_none_or(|state| state.lifecycle.may_leave_remote_terminal())
            })
            .count()
    }

    pub(crate) fn find(&self, remote_terminal_id: &str) -> Option<&ResumeRecord> {
        self.records
            .iter()
            .find(|record| record.remote_terminal_id == remote_terminal_id)
    }

    /// Compatibility insertion for schema-v1 visible handoff callers.
    #[cfg(test)]
    pub(crate) fn upsert(&mut self, record: ResumeRecord) -> io::Result<()> {
        let state = self
            .recovery
            .get(&record.remote_terminal_id)
            .cloned()
            .unwrap_or_default();
        self.upsert_with_state(record, state)
    }

    /// Persist the recovery capability before sending the park request.
    pub(crate) fn upsert_parking(
        &mut self,
        record: ResumeRecord,
        park_id: String,
        resume_token: SecretToken,
    ) -> io::Result<()> {
        let state = ResumeRecoveryState::parking(park_id, resume_token)?;
        self.upsert_with_state(record, state)
    }

    fn upsert_with_state(
        &mut self,
        record: ResumeRecord,
        state: ResumeRecoveryState,
    ) -> io::Result<()> {
        if !record.is_valid() || !state.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid remote resume record",
            ));
        }
        let terminal_id = record.remote_terminal_id.clone();
        let mut records = self.records.clone();
        records.retain(|existing| existing.remote_terminal_id != terminal_id);
        records.push(record);
        let mut recovery = self.recovery.clone();
        recovery.insert(terminal_id, state);
        self.commit(records, recovery)
    }

    pub(crate) fn mark_parked(&mut self, remote_terminal_id: &str) -> io::Result<bool> {
        self.set_lifecycle(remote_terminal_id, ResumeLifecycle::Parked)
    }

    pub(crate) fn mark_termination_pending(
        &mut self,
        remote_terminal_id: &str,
    ) -> io::Result<bool> {
        self.set_lifecycle(remote_terminal_id, ResumeLifecycle::TerminationPending)
    }

    pub(crate) fn mark_placed_cleanup_pending(
        &mut self,
        remote_terminal_id: &str,
    ) -> io::Result<bool> {
        self.set_lifecycle(remote_terminal_id, ResumeLifecycle::PlacedCleanupPending)
    }

    fn set_lifecycle(
        &mut self,
        remote_terminal_id: &str,
        lifecycle: ResumeLifecycle,
    ) -> io::Result<bool> {
        if !self
            .records
            .iter()
            .any(|record| record.remote_terminal_id == remote_terminal_id)
        {
            return Ok(false);
        }
        let mut recovery = self.recovery.clone();
        let Some(state) = recovery.get_mut(remote_terminal_id) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resume record has no lifecycle state",
            ));
        };
        state.lifecycle = lifecycle;
        if !state.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "resume record cannot enter that lifecycle without park credentials",
            ));
        }
        self.commit(self.records.clone(), recovery)?;
        Ok(true)
    }

    pub(crate) fn remove(&mut self, remote_terminal_id: &str) -> io::Result<bool> {
        let mut records = self.records.clone();
        records.retain(|record| record.remote_terminal_id != remote_terminal_id);
        if records.len() == self.records.len() {
            return Ok(false);
        }
        let mut recovery = self.recovery.clone();
        recovery.remove(remote_terminal_id);
        self.commit(records, recovery)?;
        Ok(true)
    }

    pub(crate) fn clear(&mut self) -> io::Result<usize> {
        let removed = self.records.len();
        if removed != 0 {
            self.commit(Vec::new(), BTreeMap::new())?;
        }
        Ok(removed)
    }

    pub(crate) fn set_last_error(
        &mut self,
        remote_terminal_id: &str,
        message: Option<String>,
    ) -> io::Result<()> {
        let mut records = self.records.clone();
        let Some(record) = records
            .iter_mut()
            .find(|record| record.remote_terminal_id == remote_terminal_id)
        else {
            return Ok(());
        };
        record.last_error = message;
        self.commit(records, self.recovery.clone())
    }

    fn commit(
        &mut self,
        records: Vec<ResumeRecord>,
        recovery: BTreeMap<String, ResumeRecoveryState>,
    ) -> io::Result<()> {
        self.persist(&records, &recovery)?;
        self.records = records;
        self.recovery = recovery;
        Ok(())
    }

    fn persist(
        &self,
        records: &[ResumeRecord],
        recovery: &BTreeMap<String, ResumeRecoveryState>,
    ) -> io::Result<()> {
        let file = ResumeFileV2 {
            schema: RESUME_SCHEMA_VERSION,
            records: records
                .iter()
                .cloned()
                .map(|record| StoredResumeRecord {
                    recovery: recovery
                        .get(&record.remote_terminal_id)
                        .cloned()
                        .unwrap_or_default(),
                    record,
                })
                .collect(),
        };
        let encoded = serde_json::to_vec_pretty(&file).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to encode remote resume records: {err}"),
            )
        })?;
        atomic_write_private(&self.path, &encoded)
    }
}

/// Installation-scoped credentials used to discover parked terminals on one
/// stable remote peer.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct RecoveryCredentials {
    pub(crate) origin_id: String,
    pub(crate) discovery_token: SecretToken,
}

impl fmt::Debug for RecoveryCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecoveryCredentials")
            .field("origin_id", &self.origin_id)
            .field("discovery_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct RecoveryIdentityFile {
    schema: u32,
    origin_id: String,
    #[serde(default)]
    peers: BTreeMap<String, SecretToken>,
}

/// Global recovery identity. This file is intentionally outside all session
/// directories and session-scoped resume stores.
pub(crate) struct RecoveryIdentityStore {
    path: PathBuf,
    identity: RecoveryIdentityFile,
}

impl RecoveryIdentityStore {
    pub(crate) fn open_global() -> io::Result<Self> {
        let root = crate::config::state_dir().join("remote-resume");
        ensure_private_dir(&root)?;
        Self::open(root.join("recovery-identity-v1.json"))
    }

    fn open(path: PathBuf) -> io::Result<Self> {
        let _lock = RecoveryIdentityLock::acquire(&path)?;
        cleanup_temp_files(&path);
        let (identity, created) = match load_recovery_identity(&path)? {
            Some(identity) => (identity, false),
            None => (
                RecoveryIdentityFile {
                    schema: RECOVERY_IDENTITY_SCHEMA_VERSION,
                    origin_id: random_hex(RANDOM_TOKEN_BYTES)?,
                    peers: BTreeMap::new(),
                },
                true,
            ),
        };
        let store = Self { path, identity };
        if created {
            store.persist(&store.identity)?;
        }
        Ok(store)
    }

    pub(crate) fn origin_id(&self) -> &str {
        &self.identity.origin_id
    }

    pub(crate) fn credentials_for_peer(
        &mut self,
        peer_id: &str,
    ) -> io::Result<RecoveryCredentials> {
        if peer_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "remote peer ID must not be empty",
            ));
        }

        let _lock = RecoveryIdentityLock::acquire(&self.path)?;
        cleanup_temp_files(&self.path);
        let stored_identity = load_recovery_identity(&self.path)?;
        let must_restore_file = stored_identity.is_none();
        let mut identity = stored_identity.unwrap_or_else(|| self.identity.clone());
        if let Some(token) = identity.peers.get(peer_id).cloned() {
            if must_restore_file {
                self.persist(&identity)?;
            }
            let credentials = RecoveryCredentials {
                origin_id: identity.origin_id.clone(),
                discovery_token: token,
            };
            self.identity = identity;
            return Ok(credentials);
        }
        let token = SecretToken::generate()?;
        identity.peers.insert(peer_id.to_string(), token.clone());
        self.persist(&identity)?;
        self.identity = identity;
        Ok(RecoveryCredentials {
            origin_id: self.identity.origin_id.clone(),
            discovery_token: token,
        })
    }

    fn persist(&self, identity: &RecoveryIdentityFile) -> io::Result<()> {
        let encoded = serde_json::to_vec_pretty(identity).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to encode recovery identity: {err}"),
            )
        })?;
        atomic_write_private(&self.path, &encoded)
    }
}

fn load_recovery_identity(path: &Path) -> io::Result<Option<RecoveryIdentityFile>> {
    let Some(bytes) = read_private_file(path)? else {
        return Ok(None);
    };
    let identity: RecoveryIdentityFile = serde_json::from_slice(&bytes).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("failed to decode recovery identity: {err}"),
        )
    })?;
    validate_recovery_identity(&identity)?;
    Ok(Some(identity))
}

/// The recovery identity is shared by every named session. A stable sidecar
/// lock keeps creation and read-modify-write updates atomic across processes;
/// locking the identity file itself would be unsafe because persistence
/// replaces that inode with an atomic rename.
struct RecoveryIdentityLock {
    _file_lock: crate::platform::ExclusiveFileLock,
}

impl RecoveryIdentityLock {
    fn acquire(identity_path: &Path) -> io::Result<Self> {
        let parent = identity_path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "recovery identity path {} has no parent",
                    identity_path.display()
                ),
            )
        })?;
        ensure_private_dir(parent)?;
        let lock_path = Self::path_for(identity_path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(PRIVATE_FILE_MODE)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&lock_path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "recovery identity lock {} is not a regular file",
                    lock_path.display()
                ),
            ));
        }
        if metadata.permissions().mode() & 0o777 != PRIVATE_FILE_MODE {
            file.set_permissions(fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
        }
        let file_lock =
            crate::platform::ExclusiveFileLock::acquire(file, RECOVERY_IDENTITY_LOCK_TIMEOUT)?;
        Ok(Self {
            _file_lock: file_lock,
        })
    }

    fn path_for(identity_path: &Path) -> io::Result<PathBuf> {
        let mut lock_name = identity_path
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "recovery identity path has no file name",
                )
            })?
            .to_os_string();
        lock_name.push(".lock");
        Ok(identity_path.with_file_name(lock_name))
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ParkedAdminTokenFile {
    schema: u32,
    token: SecretToken,
}

/// Installation-local capability for administrative parked-terminal API
/// calls. This is intentionally distinct from per-origin discovery tokens:
/// possessing one remote peer's credential must not grant local server admin.
pub(crate) struct LocalAdminTokenStore {
    path: PathBuf,
    file: ParkedAdminTokenFile,
}

impl fmt::Debug for LocalAdminTokenStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAdminTokenStore")
            .field("path", &self.path)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl LocalAdminTokenStore {
    pub(crate) fn open_global() -> io::Result<Self> {
        let root = crate::config::state_dir().join("remote-resume");
        ensure_private_dir(&root)?;
        let path = root.join("parked-admin-token-v1.json");
        cleanup_temp_files(&path);
        let file = match read_private_file(&path)? {
            Some(bytes) => {
                let file: ParkedAdminTokenFile = serde_json::from_slice(&bytes).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("failed to decode parked-terminal admin token: {err}"),
                    )
                })?;
                if file.schema != PARKED_ADMIN_TOKEN_SCHEMA_VERSION || !file.token.is_valid() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid parked-terminal admin token file",
                    ));
                }
                file
            }
            None => {
                let file = ParkedAdminTokenFile {
                    schema: PARKED_ADMIN_TOKEN_SCHEMA_VERSION,
                    token: SecretToken::generate()?,
                };
                let encoded = serde_json::to_vec_pretty(&file).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("failed to encode parked-terminal admin token: {err}"),
                    )
                })?;
                atomic_write_private(&path, &encoded)?;
                file
            }
        };
        Ok(Self { path, file })
    }

    pub(crate) fn token(&self) -> &SecretToken {
        &self.file.token
    }

    pub(crate) fn verify(&self, candidate: &str) -> bool {
        constant_time_eq(
            self.file.token.expose_secret().as_bytes(),
            candidate.as_bytes(),
        )
    }
}

fn constant_time_eq(expected: &[u8], candidate: &[u8]) -> bool {
    let max_len = expected.len().max(candidate.len());
    let mut difference = expected.len() ^ candidate.len();
    for index in 0..max_len {
        difference |= usize::from(
            expected.get(index).copied().unwrap_or_default()
                ^ candidate.get(index).copied().unwrap_or_default(),
        );
    }
    difference == 0
}

fn validate_recovery_identity(identity: &RecoveryIdentityFile) -> io::Result<()> {
    if identity.schema != RECOVERY_IDENTITY_SCHEMA_VERSION
        || identity.origin_id.is_empty()
        || identity
            .peers
            .iter()
            .any(|(peer, token)| peer.is_empty() || !token.is_valid())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid recovery identity file",
        ));
    }
    Ok(())
}

fn load_contents(path: &Path, bytes: &[u8]) -> io::Result<(StoreContents, bool)> {
    match decode_contents(bytes) {
        Ok(contents) => Ok(contents),
        Err(err) => {
            let quarantine = path.with_extension(format!("corrupt-{}", unix_ms()));
            if fs::rename(path, &quarantine).is_ok() {
                let _ = sync_dir(path.parent());
                tracing::warn!(
                    path = %quarantine.display(),
                    error = %err,
                    "quarantined unreadable remote resume store"
                );
            }
            Ok((StoreContents::default(), false))
        }
    }
}

fn decode_contents(bytes: &[u8]) -> io::Result<(StoreContents, bool)> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(io::Error::other)?;
    let schema = value
        .get("schema")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .unwrap_or_default();
    match schema {
        LEGACY_RESUME_SCHEMA_VERSION => {
            let mut file: ResumeFileV1 = serde_json::from_value(value).map_err(io::Error::other)?;
            let mut contents = StoreContents::default();
            for mut record in file.records.drain(..) {
                record.schema = RESUME_SCHEMA_VERSION;
                if !record.is_valid() {
                    continue;
                }
                let terminal_id = record.remote_terminal_id.clone();
                contents
                    .records
                    .retain(|existing| existing.remote_terminal_id != terminal_id);
                contents.records.push(record);
                contents
                    .recovery
                    .insert(terminal_id, ResumeRecoveryState::default());
            }
            Ok((contents, true))
        }
        RESUME_SCHEMA_VERSION => {
            let file: ResumeFileV2 = serde_json::from_value(value).map_err(io::Error::other)?;
            let mut contents = StoreContents::default();
            for entry in file.records {
                if !entry.record.is_valid() || !entry.recovery.is_valid() {
                    continue;
                }
                let terminal_id = entry.record.remote_terminal_id.clone();
                contents
                    .records
                    .retain(|existing| existing.remote_terminal_id != terminal_id);
                contents.records.push(entry.record);
                contents.recovery.insert(terminal_id, entry.recovery);
            }
            Ok((contents, false))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported remote resume schema {schema}"),
        )),
    }
}

fn absolute_path(path: PathBuf) -> io::Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn session_key(socket_path: &Path) -> String {
    let digest = Sha256::digest(socket_path.as_os_str().as_encoded_bytes());
    hex_bytes(&digest[..8])
}

fn read_private_file(path: &Path) -> io::Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "private recovery path {} is not a regular file",
                path.display()
            ),
        ));
    }
    if metadata.permissions().mode() & 0o777 != PRIVATE_FILE_MODE {
        fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_FILE_MODE))?;
    }
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

fn atomic_write_private(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("private recovery path {} has no parent", path.display()),
        )
    })?;
    ensure_private_dir(parent)?;
    cleanup_temp_files(path);
    let (tmp_path, mut tmp_file) = create_unique_temp(path)?;
    let mut cleanup = TempCleanup::new(tmp_path.clone());
    tmp_file.write_all(bytes)?;
    tmp_file.sync_all()?;
    drop(tmp_file);
    fs::rename(&tmp_path, path)?;
    cleanup.disarm();
    sync_dir(Some(parent))?;
    Ok(())
}

fn create_unique_temp(path: &Path) -> io::Result<(PathBuf, File)> {
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "temporary file has no parent")
    })?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("recovery");
    for _ in 0..128 {
        let random = random_hex(8)?;
        let tmp_path = parent.join(format!(".{file_name}.tmp-{}-{random}", std::process::id()));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(PRIVATE_FILE_MODE)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&tmp_path)
        {
            Ok(file) => return Ok((tmp_path, file)),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a unique recovery temporary file",
    ))
}

struct TempCleanup {
    path: Option<PathBuf>,
}

impl TempCleanup {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TempCleanup {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

fn cleanup_temp_files(path: &Path) {
    let legacy_tmp = path.with_extension("json.tmp");
    if let Err(err) = fs::remove_file(&legacy_tmp) {
        if err.kind() != io::ErrorKind::NotFound {
            tracing::warn!(
                path = %legacy_tmp.display(),
                error = %err,
                "could not remove legacy remote resume temporary file"
            );
        }
    }
    let Some(parent) = path.parent() else { return };
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("recovery");
    let prefix = format!(".{file_name}.tmp-");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with(&prefix) {
            continue;
        }
        let stale = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .ok()
            .and_then(|modified| SystemTime::now().duration_since(modified).ok())
            .is_some_and(|age| age >= STALE_TEMP_AGE);
        if stale {
            let _ = fs::remove_file(entry.path());
        }
    }
}

fn ensure_private_dir(path: &Path) -> io::Result<()> {
    if !path.exists() {
        DirBuilder::new()
            .recursive(true)
            .mode(PRIVATE_DIR_MODE)
            .create(path)?;
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "recovery directory {} is not a real directory",
                path.display()
            ),
        ));
    }
    if metadata.permissions().mode() & 0o777 != PRIVATE_DIR_MODE {
        fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIR_MODE))?;
    }
    Ok(())
}

fn sync_dir(path: Option<&Path>) -> io::Result<()> {
    let Some(path) = path else { return Ok(()) };
    File::open(path)?.sync_all()
}

fn random_hex(byte_count: usize) -> io::Result<String> {
    let mut random = vec![0_u8; byte_count];
    getrandom::fill(&mut random).map_err(|err| {
        io::Error::other(format!("failed to generate recovery credential: {err}"))
    })?;
    Ok(hex_bytes(&random))
}

pub(crate) fn generate_recovery_id() -> io::Result<String> {
    random_hex(RANDOM_RECOVERY_ID_BYTES)
}

pub(crate) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_record(terminal: &str) -> ResumeRecord {
        ResumeRecord {
            schema: RESUME_SCHEMA_VERSION,
            remote_terminal_id: terminal.to_string(),
            remote_pane_id: "w1:p2".to_string(),
            peer_id: "workbox".to_string(),
            ssh: ResumeSsh {
                target: "workbox".to_string(),
                ssh_args: vec!["-p".to_string(), "2222".to_string()],
                session: Some("agents".to_string()),
            },
            agent: Some(ResumeAgent {
                name: Some("reviewer".to_string()),
                agent: Some("opencode".to_string()),
                cwd: Some("/repo".to_string()),
            }),
            placement: ResumePlacement {
                workspace_id: "w1".to_string(),
                public_tab_id: "w1:t1".to_string(),
                public_pane_id: Some("w1:p2".to_string()),
                pane_index: Some(1),
            },
            handed_off_at_unix_ms: 42,
            last_error: None,
        }
    }

    fn store_path(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "herdr-resume-test-{label}-{}-{}",
            std::process::id(),
            unix_ms()
        ));
        fs::create_dir_all(&dir).expect("create test dir");
        dir.join("store.json")
    }

    fn parking_token() -> SecretToken {
        SecretToken::new("resume-secret".to_string()).expect("token")
    }

    #[test]
    fn round_trip_preserves_records_and_private_mode() {
        let path = store_path("round-trip");
        let mut store = ResumeStore::open(path.clone()).expect("open empty store");
        store.upsert(test_record("term_a")).expect("upsert a");
        store
            .upsert_parking(test_record("term_b"), "park-b".to_string(), parking_token())
            .expect("upsert b");

        let reopened = ResumeStore::open(path).expect("reopen store");
        assert_eq!(reopened.records().len(), 2);
        assert_eq!(reopened.find("term_a"), Some(&test_record("term_a")));
        assert_eq!(
            reopened
                .recovery_state("term_b")
                .map(|state| state.lifecycle),
            Some(ResumeLifecycle::ParkingPending)
        );
        let mode = reopened.path().metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, PRIVATE_FILE_MODE);
    }

    #[test]
    fn lifecycle_transitions_and_cleanup_state_are_durable() {
        let path = store_path("lifecycle");
        let mut store = ResumeStore::open(path.clone()).expect("open store");
        store
            .upsert_parking(test_record("term_a"), "park-a".to_string(), parking_token())
            .expect("upsert");
        assert!(store.mark_parked("term_a").expect("mark parked"));
        assert!(store
            .mark_termination_pending("term_a")
            .expect("mark termination pending"));

        let pending = ResumeStore::open(path.clone()).expect("reopen pending");
        assert_eq!(
            pending
                .recovery_state("term_a")
                .map(|state| state.lifecycle),
            Some(ResumeLifecycle::TerminationPending)
        );
        assert_eq!(pending.potential_orphan_count(), 1);

        assert!(store
            .mark_placed_cleanup_pending("term_a")
            .expect("mark cleanup"));

        let reopened = ResumeStore::open(path).expect("reopen");
        let state = reopened.recovery_state("term_a").expect("state");
        assert_eq!(state.lifecycle, ResumeLifecycle::PlacedCleanupPending);
        assert_eq!(state.park_id.as_deref(), Some("park-a"));
        assert_eq!(
            state.resume_token.as_ref().map(SecretToken::expose_secret),
            Some("resume-secret")
        );
        assert_eq!(reopened.potential_orphan_count(), 0);
    }

    #[test]
    fn schema_v1_records_migrate_to_legacy_lifecycle() {
        let path = store_path("v1-migration");
        let mut record = test_record("term_a");
        record.schema = LEGACY_RESUME_SCHEMA_VERSION;
        let legacy = ResumeFileV1 {
            schema: LEGACY_RESUME_SCHEMA_VERSION,
            records: vec![record],
        };
        fs::write(&path, serde_json::to_vec(&legacy).unwrap()).unwrap();

        let store = ResumeStore::open(path.clone()).expect("migrate store");

        assert_eq!(store.find("term_a").unwrap().schema, RESUME_SCHEMA_VERSION);
        assert_eq!(
            store.recovery_state("term_a").map(|state| state.lifecycle),
            Some(ResumeLifecycle::LegacyVisibleHandoff)
        );
        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted["schema"], RESUME_SCHEMA_VERSION);
    }

    #[test]
    fn failed_commit_preserves_memory_and_cleans_unique_temp() {
        let path = store_path("failed-commit");
        let mut store = ResumeStore::open(path.clone()).expect("open store");
        store.upsert(test_record("term_a")).expect("seed store");
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();

        assert!(store.upsert(test_record("term_b")).is_err());
        assert!(store.find("term_a").is_some());
        assert!(store.find("term_b").is_none());
        let prefix = ".store.json.tmp-";
        assert!(!fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with(prefix)));
    }

    #[test]
    fn legacy_fixed_temp_file_does_not_block_future_writes() {
        let path = store_path("legacy-temp");
        let legacy_tmp = path.with_extension("json.tmp");
        fs::write(&legacy_tmp, b"crash residue").unwrap();
        let mut store = ResumeStore::open(path).expect("open store");

        store.upsert(test_record("term_a")).expect("write store");

        assert!(!legacy_tmp.exists());
    }

    #[test]
    fn corrupt_store_is_quarantined_and_rebuilt() {
        let path = store_path("corrupt");
        fs::write(&path, b"not json").expect("seed corrupt store");
        let mut store = ResumeStore::open(path.clone()).expect("open corrupt store");
        assert!(store.is_empty());
        assert!(fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("store.corrupt-")));
        store.upsert(test_record("term_a")).expect("rebuild");
        assert_eq!(ResumeStore::open(path).unwrap().records().len(), 1);
    }

    #[test]
    fn recovery_identity_is_stable_per_peer_and_redacts_debug_output() {
        let path = store_path("identity");
        let mut identity = RecoveryIdentityStore::open(path.clone()).expect("identity");
        let first = identity.credentials_for_peer("peer-a").expect("peer a");
        let repeated = identity
            .credentials_for_peer("peer-a")
            .expect("peer a again");
        let other = identity.credentials_for_peer("peer-b").expect("peer b");

        assert_eq!(first, repeated);
        assert_eq!(first.origin_id, other.origin_id);
        assert_ne!(first.discovery_token, other.discovery_token);
        assert!(!format!("{first:?}").contains(first.discovery_token.expose_secret()));
        assert!(
            !format!("{:?}", first.discovery_token).contains(first.discovery_token.expose_secret())
        );
        let mode = path.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, PRIVATE_FILE_MODE);

        let mut reopened = RecoveryIdentityStore::open(path).expect("reopen identity");
        assert_eq!(reopened.credentials_for_peer("peer-a").unwrap(), repeated);
    }

    #[test]
    fn stale_named_session_stores_merge_peer_credentials_without_lost_updates() {
        let path = store_path("identity-concurrent-sessions");
        let mut first_session = RecoveryIdentityStore::open(path.clone()).expect("first session");
        let mut second_session = RecoveryIdentityStore::open(path.clone()).expect("second session");

        let first = first_session
            .credentials_for_peer("peer-a")
            .expect("first peer");
        let second = second_session
            .credentials_for_peer("peer-b")
            .expect("second peer");

        let persisted = load_recovery_identity(&path)
            .expect("read persisted identity")
            .expect("persisted identity");
        assert_eq!(persisted.origin_id, first.origin_id);
        assert_eq!(persisted.origin_id, second.origin_id);
        assert_eq!(persisted.peers.get("peer-a"), Some(&first.discovery_token));
        assert_eq!(persisted.peers.get("peer-b"), Some(&second.discovery_token));
        assert_eq!(persisted.peers.len(), 2);

        let lock_mode = RecoveryIdentityLock::path_for(&path)
            .expect("lock path")
            .metadata()
            .expect("lock metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(lock_mode, PRIVATE_FILE_MODE);
    }

    #[test]
    fn remove_clear_and_error_updates_are_transactional() {
        let path = store_path("mutations");
        let mut store = ResumeStore::open(path.clone()).unwrap();
        store.upsert(test_record("term_a")).unwrap();
        store.upsert(test_record("term_b")).unwrap();
        store
            .set_last_error("term_a", Some("temporary failure".to_string()))
            .unwrap();
        assert!(store.remove("term_b").unwrap());
        assert_eq!(store.clear().unwrap(), 1);
        assert!(ResumeStore::open(path).unwrap().is_empty());
    }
}
