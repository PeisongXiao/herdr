//! End-to-end coverage for peer federation and the macOS SSH transport.

#![cfg(unix)]

mod support;

use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use portable_pty::{native_pty_system, Child, CommandBuilder, MasterPty, PtySize};
use serde_json::{json, Value};
use support::{
    client_handshake, register_spawned_herdr_pid, unregister_spawned_herdr_pid, CURRENT_PROTOCOL,
};

const IO_TIMEOUT: Duration = Duration::from_secs(60);
const START_TIMEOUT: Duration = Duration::from_secs(15);

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(label: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let path = PathBuf::from(format!("/tmp/hf-{label}-{}-{nanos:x}", std::process::id()));
        fs::create_dir_all(&path).expect("create federation test root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct SpawnedHerdr {
    master: Option<Box<dyn MasterPty + Send>>,
    child: Box<dyn Child + Send + Sync>,
}

impl SpawnedHerdr {
    fn pid(&self) -> u32 {
        self.child.process_id().expect("Herdr child process id")
    }

    fn stop_gracefully(&mut self, api_socket: &Path) {
        let pid = self.pid();
        let response = api_request(
            api_socket,
            json!({"id": "test:stop", "method": "server.stop", "params": {}}),
        );
        assert_no_error(&response, "stop server");

        wait_until(Duration::from_secs(10), || {
            UnixStream::connect(api_socket).is_err()
        });
        drop(self.master.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
        unregister_spawned_herdr_pid(Some(pid));
    }
}

impl Drop for SpawnedHerdr {
    fn drop(&mut self) {
        let pid = self.child.process_id();
        let _ = self.child.kill();
        drop(self.master.take());
        let _ = self.child.wait();
        unregister_spawned_herdr_pid(pid);
    }
}

struct RemovePath(PathBuf);

impl Drop for RemovePath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn write_test_config(config_home: &Path) {
    for app_dir in ["herdr-dev", "herdr"] {
        let dir = config_home.join(app_dir);
        fs::create_dir_all(&dir).expect("create Herdr config directory");
        fs::write(dir.join("config.toml"), "onboarding = false\n")
            .expect("write Herdr test config");
    }
}

fn spawn_server(
    config_home: &Path,
    state_home: &Path,
    runtime_dir: &Path,
    api_socket: &Path,
    client_socket: &Path,
) -> SpawnedHerdr {
    spawn_server_inner(
        config_home,
        state_home,
        runtime_dir,
        None,
        Some(api_socket),
        Some(client_socket),
    )
}

fn spawn_named_server(
    config_home: &Path,
    state_home: &Path,
    runtime_dir: &Path,
    session: &str,
) -> SpawnedHerdr {
    spawn_server_inner(
        config_home,
        state_home,
        runtime_dir,
        Some(session),
        None,
        None,
    )
}

fn spawn_server_inner(
    config_home: &Path,
    state_home: &Path,
    runtime_dir: &Path,
    session: Option<&str>,
    api_socket: Option<&Path>,
    client_socket: Option<&Path>,
) -> SpawnedHerdr {
    write_test_config(config_home);
    fs::create_dir_all(state_home).expect("create state home");
    fs::create_dir_all(runtime_dir).expect("create runtime directory");

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 24,
            cols: 80,
            pixel_width: 0,
            pixel_height: 0,
        })
        .expect("open server pty");
    let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_herdr"));
    if let Some(session) = session {
        command.arg("--session");
        command.arg(session);
    }
    command.arg("server");
    command.env("XDG_CONFIG_HOME", config_home);
    command.env("XDG_STATE_HOME", state_home);
    command.env("XDG_RUNTIME_DIR", runtime_dir);
    command.env("SHELL", "/bin/sh");
    command.env_remove("HERDR_ENV");
    command.env_remove("HERDR_SESSION");
    match api_socket {
        Some(path) => command.env("HERDR_SOCKET_PATH", path),
        None => command.env_remove("HERDR_SOCKET_PATH"),
    }
    match client_socket {
        Some(path) => command.env("HERDR_CLIENT_SOCKET_PATH", path),
        None => command.env_remove("HERDR_CLIENT_SOCKET_PATH"),
    }

    let child = pair
        .slave
        .spawn_command(command)
        .expect("spawn Herdr server");
    register_spawned_herdr_pid(child.process_id());
    drop(pair.slave);
    SpawnedHerdr {
        master: Some(pair.master),
        child,
    }
}

fn wait_for_socket(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if UnixStream::connect(path).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("socket did not become reachable at {}", path.display());
}

fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    assert!(
        condition(),
        "condition did not become true within {timeout:?}"
    );
}

fn discover_peer_socket(directory: &Path, excluded: &[&Path]) -> PathBuf {
    let mut found = Vec::new();
    let deadline = Instant::now() + START_TIMEOUT;
    while Instant::now() < deadline {
        found = fs::read_dir(directory)
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| !excluded.contains(&path.as_path()))
            .filter(|path| {
                fs::symlink_metadata(path)
                    .map(|metadata| metadata.file_type().is_socket())
                    .unwrap_or(false)
            })
            .filter(|path| UnixStream::connect(path).is_ok())
            .collect();
        if found.len() == 1 {
            return found.pop().expect("one peer socket");
        }
        thread::sleep(Duration::from_millis(25));
    }
    let entries = fs::read_dir(directory)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    panic!(
        "could not identify one peer socket in {}; reachable={found:?}; entries={entries:?}",
        directory.display()
    );
}

fn api_request(socket: &Path, request: Value) -> Value {
    let mut stream = UnixStream::connect(socket)
        .unwrap_or_else(|error| panic!("connect to {}: {error}", socket.display()));
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .expect("set API read timeout");
    stream
        .set_write_timeout(Some(IO_TIMEOUT))
        .expect("set API write timeout");
    writeln!(stream, "{request}").expect("write API request");
    stream.flush().expect("flush API request");
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .expect("read API response");
    assert!(!response.is_empty(), "empty response for request {request}");
    serde_json::from_str(&response).unwrap_or_else(|error| {
        panic!("decode API response for {request}: {error}; response={response:?}")
    })
}

fn assert_no_error(response: &Value, context: &str) {
    assert!(
        response.get("error").is_none(),
        "{context} failed: {response}"
    );
}

fn assert_protocol_capabilities(response: &Value) {
    assert_no_error(response, "ping");
    assert_eq!(response["result"]["type"], "pong");
    assert_eq!(
        response["result"]["protocol"].as_u64(),
        Some(u64::from(CURRENT_PROTOCOL))
    );
    assert_eq!(response["result"]["capabilities"]["peer_federation"], true);
    assert_eq!(
        response["result"]["capabilities"]["remote_presentation"],
        true
    );
}

fn ping(socket: &Path, id: &str) -> Value {
    api_request(socket, json!({"id": id, "method": "ping", "params": {}}))
}

fn peer_list(socket: &Path, id: &str) -> Value {
    api_request(
        socket,
        json!({"id": id, "method": "peer.list", "params": {}}),
    )
}

#[test]
fn two_servers_discover_each_other_with_protocol_17_and_clean_up() {
    let root = TestRoot::new("pair");
    let a_dir = root.path().join("a");
    let b_dir = root.path().join("b");
    fs::create_dir_all(&a_dir).expect("create server A directory");
    fs::create_dir_all(&b_dir).expect("create server B directory");
    let a_api = a_dir.join("origin.sock");
    let a_client = a_dir.join("origin-client.sock");
    let b_api = b_dir.join("remote.sock");
    let b_client = b_dir.join("remote-client.sock");

    let mut a = spawn_server(
        &root.path().join("ac"),
        &root.path().join("as"),
        &root.path().join("ar"),
        &a_api,
        &a_client,
    );
    let mut b = spawn_server(
        &root.path().join("bc"),
        &root.path().join("bs"),
        &root.path().join("br"),
        &b_api,
        &b_client,
    );
    wait_for_socket(&a_api, START_TIMEOUT);
    wait_for_socket(&a_client, START_TIMEOUT);
    wait_for_socket(&b_api, START_TIMEOUT);
    wait_for_socket(&b_client, START_TIMEOUT);
    let a_peer = discover_peer_socket(&a_dir, &[&a_api, &a_client]);
    let b_peer = discover_peer_socket(&b_dir, &[&b_api, &b_client]);

    for (socket, id) in [
        (&a_api, "a-normal"),
        (&a_peer, "a-peer"),
        (&b_api, "b-normal"),
        (&b_peer, "b-peer"),
    ] {
        assert_protocol_capabilities(&ping(socket, id));
    }

    let mut current = UnixStream::connect(&a_client).expect("connect current protocol client");
    let (version, error) =
        client_handshake(&mut current, CURRENT_PROTOCOL, 80, 24).expect("protocol 17 handshake");
    assert_eq!(version, CURRENT_PROTOCOL);
    assert_eq!(error, None);
    drop(current);

    let mut old = UnixStream::connect(&b_client).expect("connect old protocol client");
    let (version, error) = client_handshake(&mut old, CURRENT_PROTOCOL - 1, 80, 24)
        .expect("protocol 16 rejection handshake");
    assert_eq!(version, CURRENT_PROTOCOL);
    assert!(
        error
            .as_deref()
            .is_some_and(|message| message.contains("older than server version 17")),
        "protocol 16 should be rejected before normal message decoding: {error:?}"
    );
    drop(old);

    for (socket, id, peer_id, label, peer_socket) in [
        (&a_api, "register-b", "server-b", "server B", &b_peer),
        (&b_api, "register-a", "server-a", "server A", &a_peer),
    ] {
        let response = api_request(
            socket,
            json!({
                "id": id,
                "method": "peer.register",
                "params": {
                    "peer": {
                        "id": peer_id,
                        "label": label,
                        "status": "connected",
                        "transport": {
                            "type": "api_socket",
                            "api_socket": peer_socket,
                        }
                    }
                }
            }),
        );
        assert_no_error(&response, "register peer");
    }

    let a_list = peer_list(&a_api, "list-a");
    let b_list = peer_list(&b_api, "list-b");
    assert_eq!(a_list["result"]["peers"][0]["id"], "server-b");
    assert_eq!(b_list["result"]["peers"][0]["id"], "server-a");

    for (socket, id, peer_id) in [
        (&a_api, "health-b", "server-b"),
        (&b_api, "health-a", "server-a"),
    ] {
        let response = api_request(
            socket,
            json!({
                "id": id,
                "method": "peer.health",
                "params": {"peer_id": peer_id}
            }),
        );
        assert_no_error(&response, "peer health");
        assert_eq!(response["result"]["peer"]["status"], "connected");
    }

    let denied = api_request(
        &b_peer,
        json!({"id": "denied", "method": "workspace.list", "params": {}}),
    );
    assert_eq!(denied["error"]["code"], "method_not_allowed");

    for (socket, id, peer_id) in [
        (&a_api, "unregister-b", "server-b"),
        (&b_api, "unregister-a", "server-a"),
    ] {
        let response = api_request(
            socket,
            json!({
                "id": id,
                "method": "peer.unregister",
                "params": {"peer_id": peer_id}
            }),
        );
        assert_no_error(&response, "unregister peer");
    }
    assert_eq!(peer_list(&a_api, "empty-a")["result"]["peers"], json!([]));
    assert_eq!(peer_list(&b_api, "empty-b")["result"]["peers"], json!([]));

    a.stop_gracefully(&a_api);
    b.stop_gracefully(&b_api);
    wait_until(Duration::from_secs(5), || {
        UnixStream::connect(&a_api).is_err()
            && UnixStream::connect(&a_peer).is_err()
            && UnixStream::connect(&b_api).is_err()
            && UnixStream::connect(&b_peer).is_err()
    });
}

#[cfg(target_os = "macos")]
struct SpawnedSshd {
    child: Option<std::process::Child>,
    log_path: PathBuf,
}

#[cfg(target_os = "macos")]
impl SpawnedSshd {
    fn stop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        let pid = child.id();
        let _ = child.kill();
        let status = child.wait().expect("wait for test sshd");
        unregister_spawned_herdr_pid(Some(pid));
        assert!(
            !status.success(),
            "test sshd unexpectedly exited successfully instead of being terminated"
        );
    }
}

#[cfg(target_os = "macos")]
impl Drop for SpawnedSshd {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let pid = child.id();
            let _ = child.kill();
            let _ = child.wait();
            unregister_spawned_herdr_pid(Some(pid));
        }
    }
}

#[cfg(target_os = "macos")]
struct SshFixture {
    daemon: SpawnedSshd,
    target: String,
    ssh_args: Vec<String>,
    port: u16,
}

#[cfg(target_os = "macos")]
fn command_exists(path: &Path) -> bool {
    fs::metadata(path)
        .map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(target_os = "macos")]
fn command_output(path: &Path, args: &[&str]) -> std::process::Output {
    Command::new(path)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("run {}: {error}", path.display()))
}

#[cfg(target_os = "macos")]
fn current_username() -> String {
    let output = command_output(Path::new("/usr/bin/id"), &["-un"]);
    assert!(output.status.success(), "id -un failed: {output:?}");
    String::from_utf8(output.stdout)
        .expect("UTF-8 username")
        .trim()
        .to_string()
}

#[cfg(target_os = "macos")]
fn free_loopback_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("ephemeral local address")
        .port()
}

#[cfg(target_os = "macos")]
fn start_sshd(root: &Path, remote_config: &Path, remote_state: &Path) -> Option<SshFixture> {
    let ssh = Path::new("/usr/bin/ssh");
    let sshd = Path::new("/usr/sbin/sshd");
    let ssh_keygen = Path::new("/usr/bin/ssh-keygen");
    let missing = [ssh, sshd, ssh_keygen]
        .into_iter()
        .filter(|path| !command_exists(path))
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        eprintln!(
            "SKIP macOS unprivileged sshd loopback: missing prerequisites: {}",
            missing.join(", ")
        );
        return None;
    }

    let ssh_root = root.join("ssh");
    let bin_dir = ssh_root.join("bin");
    fs::create_dir_all(&bin_dir).expect("create SSH fixture directory");
    fs::set_permissions(&ssh_root, fs::Permissions::from_mode(0o700))
        .expect("restrict SSH fixture directory");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_herdr"), bin_dir.join("herdr"))
        .expect("link Herdr test binary into remote PATH");

    let host_key = ssh_root.join("host_ed25519");
    let client_key = ssh_root.join("client_ed25519");
    for key in [&host_key, &client_key] {
        let output = command_output(
            ssh_keygen,
            &[
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-f",
                key.to_str().expect("UTF-8 key path"),
            ],
        );
        assert!(
            output.status.success(),
            "ssh-keygen failed for {}: {}",
            key.display(),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let authorized_keys = ssh_root.join("authorized_keys");
    fs::copy(client_key.with_extension("pub"), &authorized_keys).expect("create authorized_keys");
    fs::set_permissions(&authorized_keys, fs::Permissions::from_mode(0o600))
        .expect("restrict authorized_keys");

    let port = free_loopback_port();
    let username = current_username();
    let config_path = ssh_root.join("sshd_config");
    let pid_path = ssh_root.join("sshd.pid");
    let log_path = ssh_root.join("sshd.log");
    let remote_path = format!("{}:/usr/bin:/bin:/usr/sbin:/sbin", bin_dir.display());
    let config = format!(
        "HostKey {}\n\
         AuthorizedKeysFile {}\n\
         PidFile {}\n\
         Port {}\n\
         ListenAddress 127.0.0.1\n\
         Protocol 2\n\
         UsePAM no\n\
         PasswordAuthentication no\n\
         KbdInteractiveAuthentication no\n\
         PubkeyAuthentication yes\n\
         AuthenticationMethods publickey\n\
         PermitRootLogin no\n\
         StrictModes no\n\
         AllowUsers {}\n\
         AllowAgentForwarding no\n\
         AllowTcpForwarding yes\n\
         AllowStreamLocalForwarding yes\n\
         X11Forwarding no\n\
         PermitTunnel no\n\
         PermitTTY yes\n\
         UseDNS no\n\
         LogLevel VERBOSE\n\
         SetEnv XDG_CONFIG_HOME={} XDG_STATE_HOME={} PATH={}\n",
        host_key.display(),
        authorized_keys.display(),
        pid_path.display(),
        port,
        username,
        remote_config.display(),
        remote_state.display(),
        remote_path,
    );
    fs::write(&config_path, config).expect("write sshd_config");

    let validation = command_output(
        sshd,
        &["-t", "-f", config_path.to_str().expect("UTF-8 config path")],
    );
    assert!(
        validation.status.success(),
        "temporary sshd configuration is invalid: {}",
        String::from_utf8_lossy(&validation.stderr)
    );

    let log = fs::File::create(&log_path).expect("create sshd log");
    let child = Command::new(sshd)
        .args(["-D", "-e", "-f"])
        .arg(&config_path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .expect("spawn temporary sshd");
    register_spawned_herdr_pid(Some(child.id()));
    let mut daemon = SpawnedSshd {
        child: Some(child),
        log_path,
    };
    let target = format!("{username}@127.0.0.1");
    let ssh_args = vec![
        "-p".into(),
        port.to_string(),
        "-i".into(),
        client_key.display().to_string(),
        "-o".into(),
        "IdentitiesOnly=yes".into(),
        "-o".into(),
        "StrictHostKeyChecking=no".into(),
        "-o".into(),
        "UserKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "GlobalKnownHostsFile=/dev/null".into(),
        "-o".into(),
        "LogLevel=ERROR".into(),
        "-o".into(),
        "ConnectTimeout=5".into(),
    ];

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut last_error = String::new();
    while Instant::now() < deadline {
        if let Some(status) = daemon
            .child
            .as_mut()
            .and_then(|child| child.try_wait().ok().flatten())
        {
            let log = fs::read_to_string(&daemon.log_path).unwrap_or_default();
            panic!("temporary sshd exited early with {status}: {log}");
        }
        let output = Command::new(ssh)
            .args(&ssh_args)
            .arg(&target)
            .arg("printf '%s\\n' \"$XDG_CONFIG_HOME\"; command -v herdr")
            .output()
            .expect("probe temporary sshd");
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                stdout
                    .lines()
                    .any(|line| line == remote_config.display().to_string()),
                "sshd did not provide isolated XDG_CONFIG_HOME: {stdout:?}"
            );
            assert!(
                stdout
                    .lines()
                    .any(|line| line == bin_dir.join("herdr").display().to_string()),
                "sshd did not resolve the test Herdr binary: {stdout:?}"
            );
            return Some(SshFixture {
                daemon,
                target,
                ssh_args,
                port,
            });
        }
        last_error = String::from_utf8_lossy(&output.stderr).into_owned();
        thread::sleep(Duration::from_millis(100));
    }
    let log = fs::read_to_string(&daemon.log_path).unwrap_or_default();
    panic!("temporary sshd did not accept the test key: {last_error}\n{log}");
}

#[cfg(target_os = "macos")]
fn descendant_ssh_pids(root_pid: u32, port: u16) -> Vec<u32> {
    let output = Command::new("/bin/ps")
        .args(["-axo", "pid=,ppid=,command="])
        .output()
        .expect("list processes");
    assert!(output.status.success(), "ps failed");
    let rows = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse::<u32>().ok()?;
            let ppid = fields.next()?.parse::<u32>().ok()?;
            let command = fields.collect::<Vec<_>>().join(" ");
            Some((pid, ppid, command))
        })
        .collect::<Vec<_>>();
    let mut descendants = HashSet::from([root_pid]);
    loop {
        let before = descendants.len();
        for (pid, ppid, _) in &rows {
            if descendants.contains(ppid) {
                descendants.insert(*pid);
            }
        }
        if descendants.len() == before {
            break;
        }
    }
    let port = port.to_string();
    rows.into_iter()
        .filter(|(pid, _, command)| {
            *pid != root_pid
                && descendants.contains(pid)
                && command.contains("/ssh")
                && command.contains(&port)
        })
        .map(|(pid, _, _)| pid)
        .collect()
}

#[cfg(target_os = "macos")]
#[test]
fn macos_unprivileged_sshd_federates_and_revokes_the_reverse_bridge() {
    let root = TestRoot::new("sshd");
    let origin_dir = root.path().join("o");
    fs::create_dir_all(&origin_dir).expect("create origin directory");
    let origin_config = root.path().join("oc");
    let origin_state = root.path().join("os");
    let origin_runtime = root.path().join("or");
    let origin_api = origin_dir.join("origin.sock");
    let origin_client = origin_dir.join("origin-client.sock");

    let remote_config = root.path().join("rc");
    let remote_state = root.path().join("rs");
    let remote_runtime = root.path().join("rr");
    let session = format!("rf-{}", std::process::id());
    let remote_session_dir = remote_config
        .join("herdr-dev")
        .join("sessions")
        .join(&session);
    let remote_api = remote_session_dir.join("herdr.sock");
    let remote_client = remote_session_dir.join("herdr-client.sock");

    let mut origin = spawn_server(
        &origin_config,
        &origin_state,
        &origin_runtime,
        &origin_api,
        &origin_client,
    );
    let mut remote = spawn_named_server(&remote_config, &remote_state, &remote_runtime, &session);
    wait_for_socket(&origin_api, START_TIMEOUT);
    wait_for_socket(&origin_client, START_TIMEOUT);
    wait_for_socket(&remote_api, START_TIMEOUT);
    wait_for_socket(&remote_client, START_TIMEOUT);
    assert_protocol_capabilities(&ping(&origin_api, "origin-ping"));
    assert_protocol_capabilities(&ping(&remote_api, "remote-ping"));

    let Some(mut ssh) = start_sshd(root.path(), &remote_config, &remote_state) else {
        origin.stop_gracefully(&origin_api);
        remote.stop_gracefully(&remote_api);
        return;
    };
    let connect = api_request(
        &origin_api,
        json!({
            "id": "ssh-connect",
            "method": "peer.connect_ssh",
            "params": {
                "target": ssh.target,
                "ssh_args": ssh.ssh_args,
                "session": session,
                "label": "loopback"
            }
        }),
    );
    assert_no_error(&connect, "SSH peer connect");
    assert_eq!(connect["result"]["type"], "peer_ssh_connected");
    let peer_id = connect["result"]["peer"]["id"]
        .as_str()
        .expect("SSH peer id")
        .to_string();
    let connection_id = connect["result"]["connection_id"]
        .as_str()
        .expect("SSH connection id")
        .to_string();
    assert_eq!(connect["result"]["attach"]["type"], "ssh");
    let delegation_id = connect["result"]["attach"]["delegation"]["delegation_id"]
        .as_str()
        .expect("delegation id")
        .to_string();
    let delegation_epoch = connect["result"]["attach"]["delegation"]["epoch"]
        .as_u64()
        .expect("delegation epoch");

    let local_peers = peer_list(&origin_api, "local-peers");
    assert_eq!(local_peers["result"]["peers"][0]["id"], peer_id);
    let remote_peers = peer_list(&remote_api, "remote-peers");
    let remote_peer = remote_peers["result"]["peers"]
        .as_array()
        .and_then(|peers| peers.first())
        .expect("origin registered on remote");
    assert_eq!(remote_peer["transport"]["type"], "api_socket");
    let reverse_socket = PathBuf::from(
        remote_peer["transport"]["api_socket"]
            .as_str()
            .expect("reverse API socket"),
    );
    let _reverse_socket_cleanup = RemovePath(reverse_socket.clone());
    wait_for_socket(&reverse_socket, START_TIMEOUT);

    assert_protocol_capabilities(&ping(&reverse_socket, "reverse-ping"));
    let allowed = api_request(
        &reverse_socket,
        json!({"id": "metadata-list", "method": "peer.agent.list", "params": {}}),
    );
    assert_no_error(&allowed, "peer metadata list on reverse socket");
    for denied in [
        json!({
            "id": "read-denied",
            "method": "peer.agent.read",
            "params": {"target": "missing", "source": "detection"}
        }),
        json!({
            "id": "send-denied",
            "method": "peer.agent.send",
            "params": {"target": "missing", "text": "hello"}
        }),
        json!({"id": "workspace-denied", "method": "workspace.list", "params": {}}),
    ] {
        let response = api_request(&reverse_socket, denied);
        assert_eq!(response["error"]["code"], "method_not_allowed");
    }

    let delegation = api_request(
        &remote_api,
        json!({
            "id": "delegation-before-disconnect",
            "method": "terminal.delegate.status",
            "params": {
                "delegation_id": delegation_id,
                "epoch": delegation_epoch
            }
        }),
    );
    assert_no_error(&delegation, "read pending delegated terminal");
    assert_eq!(delegation["result"]["delegation"]["status"], "pending");

    wait_until(Duration::from_secs(5), || {
        !descendant_ssh_pids(origin.pid(), ssh.port).is_empty()
    });
    let disconnect = api_request(
        &origin_api,
        json!({
            "id": "ssh-disconnect",
            "method": "peer.disconnect_ssh",
            "params": {
                "peer_id": peer_id,
                "connection_id": connection_id
            }
        }),
    );
    assert_no_error(&disconnect, "SSH peer disconnect");

    wait_until(Duration::from_secs(20), || {
        peer_list(&remote_api, "remote-cleanup")["result"]["peers"] == json!([])
    });
    wait_until(Duration::from_secs(20), || {
        UnixStream::connect(&reverse_socket).is_err()
    });
    wait_until(Duration::from_secs(10), || {
        descendant_ssh_pids(origin.pid(), ssh.port).is_empty()
    });
    let delegation = api_request(
        &remote_api,
        json!({
            "id": "delegation-after-disconnect",
            "method": "terminal.delegate.status",
            "params": {
                "delegation_id": delegation_id,
                "epoch": delegation_epoch
            }
        }),
    );
    let delegation_cleaned = delegation["error"]["code"] == "terminal_delegation_not_found"
        || matches!(
            delegation["result"]["delegation"]["status"].as_str(),
            Some("failed" | "terminated")
        );
    assert!(
        delegation_cleaned,
        "delegated terminal remained live after peer cleanup: {delegation}"
    );
    assert_eq!(
        peer_list(&origin_api, "origin-cleanup")["result"]["peers"],
        json!([])
    );

    origin.stop_gracefully(&origin_api);
    remote.stop_gracefully(&remote_api);
    ssh.daemon.stop();
}
