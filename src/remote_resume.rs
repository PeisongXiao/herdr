//! Durable resume records for automatic remote-pane handoff and re-acquire.
//!
//! When `[remote] auto_remote_handoff` is enabled, a graceful server stop
//! hands each delegated remote pane back to its host and persists one record
//! per pane here. The next server start re-acquires the recorded panes;
//! `herdr remote-resume` retries records that could not be resumed without
//! interactive SSH authentication.
//!
//! Records are scoped to the local Herdr session (hashed API socket path),
//! written atomically, and pruned when a pane is re-acquired or confirmed
//! gone on its host. A record is never proof that the remote pane still
//! exists; it is a hint that is validated during re-acquire.

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const RESUME_SCHEMA_VERSION: u32 = 1;

const PRIVATE_DIR_MODE: u32 = 0o700;
const PRIVATE_FILE_MODE: u32 = 0o600;

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

/// Display metadata for the delegated pane, used by `herdr remote-resume`
/// listings and to restore pane labels.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResumeAgent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cwd: Option<String>,
}

/// Where the local owner pane lived when it was handed off. Used to place
/// the re-acquired pane and to retire the placeholder shell left by session
/// restore.
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct ResumeFile {
    #[serde(default)]
    schema: u32,
    #[serde(default)]
    records: Vec<ResumeRecord>,
}

/// Session-scoped, atomically persisted store of resume records.
pub(crate) struct ResumeStore {
    path: PathBuf,
    records: Vec<ResumeRecord>,
}

impl ResumeStore {
    /// Open the store for the active Herdr session, creating it if needed.
    pub(crate) fn for_active_session() -> io::Result<Self> {
        let socket_path = crate::session::active_api_socket_path();
        let socket_path = if socket_path.is_absolute() {
            socket_path
        } else {
            std::env::current_dir()?.join(socket_path)
        };
        let digest = Sha256::digest(socket_path.as_os_str().as_encoded_bytes());
        let session_key = hex_bytes(&digest[..8]);
        let root = crate::config::state_dir().join("remote-resume").join("v1");
        ensure_private_dir(&root)?;
        Self::open(root.join(format!("{session_key}.json")))
    }

    /// Open a store at an explicit path. Primarily for tests.
    pub(crate) fn open(path: PathBuf) -> io::Result<Self> {
        let records = match fs::read(&path) {
            Ok(bytes) => load_records(&path, &bytes)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(err),
        };
        Ok(Self { path, records })
    }

    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn records(&self) -> &[ResumeRecord] {
        &self.records
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn find(&self, remote_terminal_id: &str) -> Option<&ResumeRecord> {
        self.records
            .iter()
            .find(|record| record.remote_terminal_id == remote_terminal_id)
    }

    pub(crate) fn upsert(&mut self, record: ResumeRecord) -> io::Result<()> {
        self.records
            .retain(|existing| existing.remote_terminal_id != record.remote_terminal_id);
        self.records.push(record);
        self.save()
    }

    pub(crate) fn remove(&mut self, remote_terminal_id: &str) -> io::Result<bool> {
        let before = self.records.len();
        self.records
            .retain(|record| record.remote_terminal_id != remote_terminal_id);
        let removed = self.records.len() != before;
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    pub(crate) fn set_last_error(
        &mut self,
        remote_terminal_id: &str,
        message: Option<String>,
    ) -> io::Result<()> {
        if let Some(record) = self
            .records
            .iter_mut()
            .find(|record| record.remote_terminal_id == remote_terminal_id)
        {
            record.last_error = message;
            self.save()?;
        }
        Ok(())
    }

    fn save(&self) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            ensure_private_dir(parent)?;
        }
        let file = ResumeFile {
            schema: RESUME_SCHEMA_VERSION,
            records: self.records.clone(),
        };
        let encoded = serde_json::to_vec_pretty(&file).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("failed to encode remote resume records: {err}"),
            )
        })?;
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut tmp_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(PRIVATE_FILE_MODE)
                .open(&tmp)?;
            tmp_file.write_all(&encoded)?;
            tmp_file.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        if let Ok(file) = OpenOptions::new().read(true).open(&self.path) {
            let _ = file.sync_all();
        }
        sync_dir(self.path.parent())?;
        Ok(())
    }
}

fn load_records(path: &Path, bytes: &[u8]) -> io::Result<Vec<ResumeRecord>> {
    let parsed: Result<ResumeFile, _> = serde_json::from_slice(bytes);
    match parsed {
        Ok(file) if file.schema == RESUME_SCHEMA_VERSION => Ok(file
            .records
            .into_iter()
            .filter(ResumeRecord::is_valid)
            .collect()),
        _ => {
            // Corrupt or incompatible stores are preserved for inspection but
            // never trusted; re-acquire must not guess from bad data.
            let quarantine = path.with_extension(format!("corrupt-{}", unix_ms()));
            if fs::rename(path, &quarantine).is_ok() {
                tracing::warn!(
                    path = %quarantine.display(),
                    "quarantined unreadable remote resume store"
                );
            }
            Ok(Vec::new())
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
                "remote resume directory {} is not a real directory",
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
    match File::open(path) {
        Ok(file) => file.sync_all(),
        Err(_) => Ok(()),
    }
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

    #[test]
    fn round_trip_preserves_records() {
        let path = store_path("round-trip");
        let mut store = ResumeStore::open(path.clone()).expect("open empty store");
        assert!(store.is_empty());
        store.upsert(test_record("term_a")).expect("upsert a");
        store.upsert(test_record("term_b")).expect("upsert b");

        let reopened = ResumeStore::open(path).expect("reopen store");
        assert_eq!(reopened.records().len(), 2);
        assert_eq!(reopened.find("term_a"), Some(&test_record("term_a")));
        let mode = reopened
            .path()
            .metadata()
            .expect("store metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, PRIVATE_FILE_MODE);
    }

    #[test]
    fn upsert_replaces_existing_terminal_record() {
        let path = store_path("upsert");
        let mut store = ResumeStore::open(path).expect("open store");
        store.upsert(test_record("term_a")).expect("upsert");
        let mut updated = test_record("term_a");
        updated.peer_id = "other".to_string();
        store.upsert(updated.clone()).expect("upsert again");
        assert_eq!(store.records().len(), 1);
        assert_eq!(store.find("term_a"), Some(&updated));
    }

    #[test]
    fn remove_deletes_matching_record() {
        let path = store_path("remove");
        let mut store = ResumeStore::open(path.clone()).expect("open store");
        store.upsert(test_record("term_a")).expect("upsert");
        assert!(store.remove("term_a").expect("remove"));
        assert!(!store.remove("term_a").expect("remove again"));
        let reopened = ResumeStore::open(path).expect("reopen");
        assert!(reopened.is_empty());
    }

    #[test]
    fn corrupt_store_is_quarantined_and_rebuilt() {
        let path = store_path("corrupt");
        fs::write(&path, b"not json").expect("seed corrupt store");
        let mut store = ResumeStore::open(path.clone()).expect("open corrupt store");
        assert!(store.is_empty());
        let parent = path.parent().expect("parent");
        let quarantined = fs::read_dir(parent)
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with("store.corrupt-")
            });
        assert!(quarantined, "corrupt store was moved aside");
        store.upsert(test_record("term_a")).expect("rebuild");
        let reopened = ResumeStore::open(path).expect("reopen");
        assert_eq!(reopened.records().len(), 1);
    }

    #[test]
    fn invalid_records_are_dropped_on_load() {
        let path = store_path("invalid");
        let mut store = ResumeStore::open(path.clone()).expect("open store");
        let mut record = test_record("term_a");
        record.schema = 999;
        store.upsert(record).expect("upsert");
        // Rewrite the file by hand with the invalid record plus a valid one.
        let mut contents: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).expect("read store")).expect("parse store");
        contents["records"]
            .as_array_mut()
            .expect("records array")
            .push(serde_json::to_value(test_record("term_b")).expect("encode record"));
        fs::write(&path, serde_json::to_vec(&contents).expect("encode store"))
            .expect("write store");
        let reopened = ResumeStore::open(path).expect("reopen");
        assert_eq!(reopened.records().len(), 1);
        assert!(reopened.find("term_b").is_some());
    }

    #[test]
    fn set_last_error_persists() {
        let path = store_path("last-error");
        let mut store = ResumeStore::open(path.clone()).expect("open store");
        store.upsert(test_record("term_a")).expect("upsert");
        store
            .set_last_error("term_a", Some("authentication failed".to_string()))
            .expect("set error");
        let reopened = ResumeStore::open(path).expect("reopen");
        assert_eq!(
            reopened
                .find("term_a")
                .and_then(|record| record.last_error.as_deref()),
            Some("authentication failed")
        );
    }
}
