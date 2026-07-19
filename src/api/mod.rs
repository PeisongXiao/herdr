pub mod client;
mod event_hub;
pub mod schema;
mod server;
mod status;
mod subscriptions;
mod wait;

pub use event_hub::EventHub;
pub use server::{start_server, start_server_with_capabilities, ServerHandle};
pub use status::{read_runtime_status_at, RuntimeStatus};

use std::path::{Path, PathBuf};

use tokio::sync::mpsc;

use crate::api::schema::{Method, Request};

pub const SOCKET_PATH_ENV_VAR: &str = "HERDR_SOCKET_PATH";

pub(crate) fn request_changes_ui(request: &Request) -> bool {
    matches!(
        &request.method,
        Method::ServerReloadConfig(_)
            | Method::ServerReloadAgentManifests(_)
            | Method::NotificationShow(_)
            | Method::WorkspaceCreate(_)
            | Method::WorkspaceFocus(_)
            | Method::WorkspaceRename(_)
            | Method::WorkspaceMove(_)
            | Method::WorkspaceClose(_)
            | Method::WorktreeCreate(_)
            | Method::WorktreeOpen(_)
            | Method::WorktreeRemove(_)
            | Method::TabCreate(_)
            | Method::TabFocus(_)
            | Method::TabRename(_)
            | Method::TabMove(_)
            | Method::TabClose(_)
            | Method::LayoutApply(_)
            | Method::LayoutSetSplitRatio(_)
            | Method::AgentRename(_)
            | Method::AgentFocus(_)
            | Method::AgentStart(_)
            | Method::PeerAgentRename(_)
            | Method::PeerAgentStart(_)
            | Method::AgentAttachPrepare(_)
            | Method::TerminalDelegateCreate(_)
            | Method::TerminalDelegateClaim(_)
            | Method::TerminalDelegateTerminate(_)
            | Method::TerminalDelegateHandoff(_)
            | Method::TerminalDelegatePark(_)
            | Method::TerminalParkedResume(_)
            | Method::TerminalParkedResolve(_)
            | Method::TerminalParkedAdminResolve(_)
            | Method::RemoteResume(_)
            | Method::TerminalRecoveryRetry(_)
            | Method::TerminalRecoveryDiscard(_)
            | Method::PaneSplit(_)
            | Method::PaneSwap(_)
            | Method::PaneMove(_)
            | Method::PaneZoom(_)
            | Method::PaneFocusDirection(_)
            | Method::PaneResize(_)
            | Method::PaneFocus(_)
            | Method::PaneRename(_)
            | Method::PaneReportAgent(_)
            | Method::PaneReportAgentSession(_)
            | Method::PaneReportMetadata(_)
            | Method::PaneClearAgentAuthority(_)
            | Method::PaneReleaseAgent(_)
            | Method::PaneClose(_)
            | Method::PeerRegister(_)
            | Method::PeerConnectSsh(_)
            | Method::PeerDisconnectSsh(_)
            | Method::PeerPresentationActivate(_)
            | Method::PeerUnregister(_)
            | Method::PluginActionInvoke(_)
            | Method::PluginPaneOpen(_)
            | Method::PluginPaneFocus(_)
            | Method::PluginPaneClose(_)
    )
}

pub struct ApiRequestMessage {
    pub request: Request,
    pub respond_to: std::sync::mpsc::Sender<String>,
}

pub type ApiRequestSender = mpsc::UnboundedSender<ApiRequestMessage>;

pub fn socket_path() -> PathBuf {
    crate::session::active_api_socket_path()
}

pub fn peer_socket_path() -> PathBuf {
    derive_peer_socket_path(&socket_path(), peer_socket_instance_id())
}

fn peer_socket_instance_id() -> &'static str {
    use std::sync::OnceLock;

    static INSTANCE_ID: OnceLock<String> = OnceLock::new();
    INSTANCE_ID.get_or_init(|| {
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{:x}-{started:x}", std::process::id())
    })
}

// macOS has the smallest supported sockaddr_un.sun_path at 104 bytes. Leave
// one byte for the trailing NUL so paths produced here remain portable.
const MAX_UNIX_SOCKET_PATH_BYTES: usize = 103;
// Ten URL-safe Base64 characters carry 60 bits while fitting "herdr.sock".
const NORMAL_PEER_BASENAME_CHARS: usize = 10;
// The temp fallback has room for a 96-bit process/startup-scoped identifier.
const FALLBACK_PEER_HASH_CHARS: usize = 16;

pub(crate) fn derive_peer_socket_path(api_socket_path: &Path, instance_id: &str) -> PathBuf {
    derive_peer_socket_path_with_temp_dir(api_socket_path, instance_id, &std::env::temp_dir())
}

fn derive_peer_socket_path_with_temp_dir(
    api_socket_path: &Path,
    instance_id: &str,
    temp_dir: &Path,
) -> PathBuf {
    let opaque_id = opaque_peer_socket_id(api_socket_path, instance_id);
    let parent = api_socket_path.parent().unwrap_or_else(|| Path::new(""));

    if let Some(api_basename) = api_socket_path.file_name() {
        if api_basename.as_encoded_bytes().len() >= NORMAL_PEER_BASENAME_CHARS {
            let mut basename = opaque_id[..NORMAL_PEER_BASENAME_CHARS].to_string();
            if api_basename.as_encoded_bytes() == basename.as_bytes() {
                rotate_last_basename_char(&mut basename);
            }

            let candidate = parent.join(basename);
            if candidate != api_socket_path
                && encoded_path_len(&candidate) <= encoded_path_len(api_socket_path)
                && encoded_path_len(&candidate) <= MAX_UNIX_SOCKET_PATH_BYTES
            {
                return candidate;
            }
        }
    }

    let fallback_basename = format!("hp-{}", &opaque_id[..FALLBACK_PEER_HASH_CHARS]);
    for fallback_dir in fallback_dirs(temp_dir) {
        let fallback = fallback_dir.join(&fallback_basename);
        if fallback != api_socket_path && encoded_path_len(&fallback) <= MAX_UNIX_SOCKET_PATH_BYTES
        {
            return fallback;
        }
    }

    // This can only occur on a platform whose own temporary directory exceeds
    // its Unix-socket path budget. Returning the shortest candidate preserves
    // the actionable bind error instead of silently colliding with another API.
    PathBuf::from(fallback_basename)
}

fn fallback_dirs(temp_dir: &Path) -> Vec<&Path> {
    #[cfg(unix)]
    {
        let canonical_tmp = Path::new("/tmp");
        if temp_dir == canonical_tmp {
            vec![temp_dir]
        } else {
            vec![temp_dir, canonical_tmp]
        }
    }
    #[cfg(not(unix))]
    {
        vec![temp_dir]
    }
}

fn opaque_peer_socket_id(api_socket_path: &Path, instance_id: &str) -> String {
    use base64::Engine as _;
    use sha2::{Digest as _, Sha256};

    let path_bytes = api_socket_path.as_os_str().as_encoded_bytes();
    let mut hasher = Sha256::new();
    hasher.update(b"herdr-peer-socket-v1");
    hasher.update((path_bytes.len() as u64).to_le_bytes());
    hasher.update(path_bytes);
    hasher.update((instance_id.len() as u64).to_le_bytes());
    hasher.update(instance_id.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn encoded_path_len(path: &Path) -> usize {
    path.as_os_str().as_encoded_bytes().len()
}

fn rotate_last_basename_char(basename: &mut String) {
    if let Some(last) = basename.pop() {
        basename.push(if last == '-' { '_' } else { '-' });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn peer_socket_path_is_compact_deterministic_and_identity_scoped() {
        let api_path = Path::new("/tmp/herdr.sock");
        let derived = derive_peer_socket_path(api_path, "123-abc");

        assert_eq!(derived, derive_peer_socket_path(api_path, "123-abc"));
        assert_eq!(derived.parent(), api_path.parent());
        assert_ne!(derived, api_path);
        assert!(encoded_path_len(&derived) <= encoded_path_len(api_path));
        assert_ne!(derived, derive_peer_socket_path(api_path, "456-def"));
        assert_ne!(
            derived,
            derive_peer_socket_path(Path::new("/tmp/other.sock"), "123-abc")
        );
    }

    #[test]
    fn peer_socket_path_uses_compact_temp_fallback_for_overlong_parent() {
        let long_parent = PathBuf::from("p".repeat(MAX_UNIX_SOCKET_PATH_BYTES + 1));
        let api_path = long_parent.join("herdr.sock");
        let temp_dir = std::env::temp_dir();
        let derived = derive_peer_socket_path(&api_path, "123-abc");

        assert_eq!(derived.parent(), Some(temp_dir.as_path()));
        assert_ne!(derived, api_path);
        assert!(encoded_path_len(&derived) <= MAX_UNIX_SOCKET_PATH_BYTES);
        assert_eq!(
            derived
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::len),
            Some("hp-".len() + FALLBACK_PEER_HASH_CHARS)
        );
        assert_ne!(derived, derive_peer_socket_path(&api_path, "456-def"));
        assert_ne!(
            derived,
            derive_peer_socket_path(&long_parent.join("other.sock"), "123-abc")
        );
    }

    #[test]
    fn peer_socket_path_uses_full_fallback_entropy_for_short_api_basename() {
        let api_path = Path::new("/tmp/a");
        let derived = derive_peer_socket_path_with_temp_dir(api_path, "123-abc", Path::new("/tmp"));

        assert_ne!(derived, api_path);
        assert_eq!(
            derived
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::len),
            Some("hp-".len() + FALLBACK_PEER_HASH_CHARS)
        );
    }

    #[cfg(unix)]
    #[test]
    fn peer_socket_path_uses_canonical_tmp_when_configured_temp_is_too_long() {
        let long_parent = PathBuf::from(format!("/{}", "p".repeat(MAX_UNIX_SOCKET_PATH_BYTES)));
        let api_path = long_parent.join("herdr.sock");
        let long_temp = PathBuf::from(format!("/{}", "t".repeat(MAX_UNIX_SOCKET_PATH_BYTES)));
        let derived = derive_peer_socket_path_with_temp_dir(&api_path, "123-abc", &long_temp);

        assert_eq!(derived.parent(), Some(Path::new("/tmp")));
        assert!(encoded_path_len(&derived) <= MAX_UNIX_SOCKET_PATH_BYTES);
    }
}
