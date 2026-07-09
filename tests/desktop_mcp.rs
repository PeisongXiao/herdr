#![cfg(target_os = "macos")]

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const TOOL_NAMES: [&str; 10] = [
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

fn unique_test_dir(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    // macOS Unix-domain socket paths are limited to 104 bytes. Keep the test
    // root deliberately short so the nested session sockets remain portable.
    PathBuf::from(format!("/tmp/hm-{label}-{}-{id}", std::process::id()))
}

struct McpChild {
    child: Child,
    stdin: Option<ChildStdin>,
    responses: Receiver<Result<Value, String>>,
    stderr: Arc<Mutex<Vec<u8>>>,
}

impl McpChild {
    fn spawn() -> Self {
        let base = unique_test_dir("stdio");
        fs::create_dir_all(&base).expect("create MCP test directory");
        let session = unique_session_name("stdio");
        Self::spawn_for(&base, &session)
    }

    fn spawn_for(base: &std::path::Path, session: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["--session", session, "mcp", "serve"])
            .env("XDG_CONFIG_HOME", base.join("config"))
            .env("XDG_STATE_HOME", base.join("state"))
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_CLIENT_SOCKET_PATH")
            .env_remove("HERDR_ENV")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn MCP bridge");
        let stdout = child.stdout.take().expect("MCP stdout");
        let mut stderr_reader = child.stderr.take().expect("MCP stderr");
        let stdin = child.stdin.take().expect("MCP stdin");
        let (tx, responses) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let parsed = match line {
                    Ok(line) => serde_json::from_str(&line)
                        .map_err(|err| format!("non-JSON stdout {line:?}: {err}")),
                    Err(err) => Err(format!("failed to read MCP stdout: {err}")),
                };
                if tx.send(parsed).is_err() {
                    break;
                }
            }
        });
        let stderr = Arc::new(Mutex::new(Vec::new()));
        let stderr_writer = stderr.clone();
        thread::spawn(move || {
            let mut bytes = Vec::new();
            let _ = stderr_reader.read_to_end(&mut bytes);
            if let Ok(mut output) = stderr_writer.lock() {
                *output = bytes;
            }
        });
        Self {
            child,
            stdin: Some(stdin),
            responses,
            stderr,
        }
    }

    fn initialize(&mut self) -> Value {
        let initialized = self.request(
            1,
            "initialize",
            json!({
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": { "name": "herdr-test", "version": "1" },
            }),
        );
        self.send(json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }));
        initialized
    }

    fn send(&mut self, value: Value) {
        let stdin = self.stdin.as_mut().expect("MCP stdin remains open");
        serde_json::to_writer(&mut *stdin, &value).expect("write MCP request");
        stdin.write_all(b"\n").expect("terminate MCP request");
        stdin.flush().expect("flush MCP request");
    }

    fn receive(&self) -> Value {
        self.responses
            .recv_timeout(Duration::from_secs(5))
            .expect("timed out waiting for MCP response")
            .unwrap_or_else(|message| panic!("{message}"))
    }

    fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        }));
        let response = self.receive();
        assert_eq!(response["id"], id);
        response
    }

    fn close_and_wait(mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            match self.child.try_wait().expect("poll MCP child") {
                Some(status) => {
                    let stderr = self
                        .stderr
                        .lock()
                        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                        .unwrap_or_else(|_| "<stderr lock poisoned>".into());
                    assert!(
                        status.success(),
                        "MCP child exited with {status}; stderr={stderr}"
                    );
                    break;
                }
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
                None => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!("MCP child did not exit after stdin EOF");
                }
            }
        }
    }
}

fn unique_session_name(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("m-{label}-{}-{id}", std::process::id())
}

struct SessionServer {
    base: PathBuf,
    session: String,
    pid: Option<u32>,
}

impl SessionServer {
    fn start() -> Self {
        let base = unique_test_dir("live");
        fs::create_dir_all(&base).expect("create live MCP test directory");
        let session = unique_session_name("live");
        let output = Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["--session", &session, "server", "ensure", "--json"])
            .env("XDG_CONFIG_HOME", base.join("config"))
            .env("XDG_STATE_HOME", base.join("state"))
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_CLIENT_SOCKET_PATH")
            .env_remove("HERDR_ENV")
            .output()
            .expect("start Herdr session server");
        assert!(
            output.status.success(),
            "server ensure failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let response: Value = serde_json::from_slice(&output.stdout).expect("server ensure JSON");
        let pid = response["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok());
        Self { base, session, pid }
    }

    fn stop(&mut self) {
        if self.pid.is_none() {
            return;
        }
        let stopped = Command::new(env!("CARGO_BIN_EXE_herdr"))
            .args(["--session", &self.session, "server", "stop"])
            .env("XDG_CONFIG_HOME", self.base.join("config"))
            .env("XDG_STATE_HOME", self.base.join("state"))
            .env_remove("HERDR_SOCKET_PATH")
            .env_remove("HERDR_CLIENT_SOCKET_PATH")
            .env_remove("HERDR_ENV")
            .status()
            .is_ok_and(|status| status.success());
        if stopped {
            self.pid = None;
        } else if let Some(pid) = self.pid.take() {
            unsafe {
                libc::kill(pid as libc::pid_t, libc::SIGTERM);
            }
        }
    }
}

fn find_file(root: &std::path::Path, file_name: &str) -> PathBuf {
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)
            .unwrap_or_else(|err| panic!("read {}: {err}", directory.display()))
        {
            let entry = entry.expect("directory entry");
            let file_type = entry.file_type().expect("entry type");
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if entry.file_name() == file_name {
                return entry.path();
            }
        }
    }
    panic!("did not find {file_name} beneath {}", root.display());
}

fn age_cli_as_creating(base: &std::path::Path, cli_id: &str) -> (String, PathBuf) {
    let index_path = find_file(&base.join("state"), &format!("{cli_id}.json"));
    let mut index: Value = serde_json::from_slice(&fs::read(&index_path).expect("read CLI index"))
        .expect("parse CLI index");
    let marker = index["identity"]["launch_marker"]
        .as_str()
        .expect("launch marker")
        .to_string();
    let channel_name = index["channel_dir"]
        .as_str()
        .expect("channel directory")
        .to_string();
    index["identity"]["state"] = json!("creating");
    fs::write(
        &index_path,
        serde_json::to_vec(&index).expect("encode CLI index"),
    )
    .expect("write CLI index");

    let session_root = index_path
        .parent()
        .and_then(std::path::Path::parent)
        .expect("queue session root");
    let channel_path = session_root
        .join("channels")
        .join(channel_name)
        .join("channel.json");
    let mut channel: Value =
        serde_json::from_slice(&fs::read(&channel_path).expect("read channel metadata"))
            .expect("parse channel metadata");
    channel["state"] = json!("creating");
    channel["updated_at_unix_ms"] = json!(0);
    channel["identity"]["state"] = json!("creating");
    fs::write(
        channel_path,
        serde_json::to_vec(&channel).expect("encode channel metadata"),
    )
    .expect("write channel metadata");
    (marker, index_path)
}

impl Drop for SessionServer {
    fn drop(&mut self) {
        self.stop();
        let _ = fs::remove_dir_all(&self.base);
    }
}

impl Drop for McpChild {
    fn drop(&mut self) {
        drop(self.stdin.take());
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn stdio_server_lists_only_the_ten_tools_and_health_works_offline() {
    let mut mcp = McpChild::spawn();
    let initialized = mcp.initialize();
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");
    assert!(initialized["result"]["capabilities"]["tools"].is_object());
    assert!(initialized["result"]["capabilities"]["prompts"].is_null());
    assert!(initialized["result"]["capabilities"]["resources"].is_null());

    let listed = mcp.request(2, "tools/list", json!({}));
    let tools = listed["result"]["tools"]
        .as_array()
        .expect("tools/list array");
    let mut names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();
    names.sort_unstable();
    assert_eq!(names, TOOL_NAMES);
    for tool in tools {
        let name = tool["name"].as_str().expect("tool name");
        let read_only = matches!(
            name,
            "herdr_health" | "herdr_snapshot" | "herdr_read_pane" | "herdr_pane_process_info"
        );
        let destructive = matches!(name, "herdr_stop_cli" | "herdr_ack_messages");
        assert_eq!(tool["annotations"]["readOnlyHint"], read_only, "{name}");
        assert_eq!(
            tool["annotations"]["destructiveHint"], destructive,
            "{name}"
        );
        assert_eq!(tool["annotations"]["openWorldHint"], false);
        assert_eq!(tool["inputSchema"]["additionalProperties"], false);
    }

    let health = mcp.request(
        3,
        "tools/call",
        json!({ "name": "herdr_health", "arguments": {} }),
    );
    assert_eq!(health["result"]["isError"], false);
    assert_eq!(health["result"]["content"], json!([]));
    assert_eq!(health["result"]["structuredContent"]["ok"], true);
    let data = &health["result"]["structuredContent"]["data"];
    assert_eq!(data["queue_schema"], 1);
    assert_eq!(data["required_protocol"], 17);
    assert_eq!(data["herdr_available"], false);

    let invalid = mcp.request(
        4,
        "tools/call",
        json!({
            "name": "herdr_health",
            "arguments": { "unexpected": true },
        }),
    );
    assert_eq!(invalid["error"]["code"], -32602);

    let invalid_read = mcp.request(
        5,
        "tools/call",
        json!({
            "name": "herdr_read_pane",
            "arguments": { "pane_id": "pane", "lines": 0 },
        }),
    );
    assert_eq!(invalid_read["error"]["code"], -32602);

    let invalid_launch = mcp.request(
        6,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "test",
                "argv": ["printf", "hello"],
                "cwd": "relative",
            },
        }),
    );
    assert_eq!(invalid_launch["error"]["code"], -32602);

    let snapshot = mcp.request(
        7,
        "tools/call",
        json!({ "name": "herdr_snapshot", "arguments": {} }),
    );
    assert_eq!(snapshot["result"]["isError"], true);
    assert_eq!(snapshot["result"]["content"], json!([]));
    assert_eq!(snapshot["result"]["structuredContent"]["ok"], false);
    assert_eq!(
        snapshot["result"]["structuredContent"]["error"]["code"],
        "server_unavailable"
    );

    let drain = mcp.request(
        8,
        "tools/call",
        json!({
            "name": "herdr_drain_messages",
            "arguments": { "cli_id": "unknown_cli" },
        }),
    );
    assert_eq!(drain["result"]["isError"], true);
    assert_eq!(drain["result"]["structuredContent"]["ok"], false);

    let ack = mcp.request(
        9,
        "tools/call",
        json!({
            "name": "herdr_ack_messages",
            "arguments": { "cli_id": "unknown_cli", "lease_id": "unknown_lease" },
        }),
    );
    assert_eq!(ack["result"]["isError"], true);
    assert_eq!(ack["result"]["structuredContent"]["ok"], false);

    let malformed_cli_id = mcp.request(
        10,
        "tools/call",
        json!({
            "name": "herdr_drain_messages",
            "arguments": { "cli_id": "not-a-cli-id" },
        }),
    );
    assert_eq!(malformed_cli_id["error"]["code"], -32602);
    mcp.close_and_wait();
}

#[test]
fn live_session_launch_input_read_interrupt_and_stop_round_trip() {
    let server = SessionServer::start();
    let mut mcp = McpChild::spawn_for(&server.base, &server.session);
    let initialized = mcp.initialize();
    assert_eq!(initialized["result"]["protocolVersion"], "2025-11-25");

    let health = mcp.request(
        2,
        "tools/call",
        json!({ "name": "herdr_health", "arguments": {} }),
    );
    let health_data = &health["result"]["structuredContent"]["data"];
    assert_eq!(health_data["herdr_available"], true);
    assert_eq!(health_data["herdr_protocol"], 17);
    assert_eq!(health_data["selected_session"], server.session);

    let launched = mcp.request(
        3,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "mcp-cat",
                "argv": ["/bin/cat"],
                "cwd": server.base.to_string_lossy(),
                "split": "right",
            },
        }),
    );
    assert_eq!(launched["result"]["isError"], false, "{launched}");
    assert_eq!(launched["result"]["content"], json!([]));
    let launch_data = &launched["result"]["structuredContent"]["data"];
    let cli_id = launch_data["cli_id"]
        .as_str()
        .expect("launched cli_id")
        .to_string();
    let pane_id = launch_data["pane_id"]
        .as_str()
        .expect("launched pane_id")
        .to_string();

    let sent = mcp.request(
        4,
        "tools/call",
        json!({
            "name": "herdr_send_input",
            "arguments": { "cli_id": &cli_id, "text": "bridge-round-trip\n" },
        }),
    );
    assert_eq!(sent["result"]["isError"], false, "{sent}");

    let mut observed = false;
    for request_id in 5..15 {
        let read = mcp.request(
            request_id,
            "tools/call",
            json!({
                "name": "herdr_read_pane",
                "arguments": {
                    "pane_id": &pane_id,
                    "source": "recent",
                    "lines": 40,
                    "format": "text",
                },
            }),
        );
        assert_eq!(read["result"]["isError"], false, "{read}");
        if read["result"]["structuredContent"]["data"]["read"]["text"]
            .as_str()
            .is_some_and(|text| text.contains("bridge-round-trip"))
        {
            observed = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(observed, "sent input did not appear in pane output");

    let process = mcp.request(
        15,
        "tools/call",
        json!({
            "name": "herdr_pane_process_info",
            "arguments": { "pane_id": &pane_id },
        }),
    );
    assert_eq!(process["result"]["isError"], false, "{process}");

    let interrupted = mcp.request(
        16,
        "tools/call",
        json!({
            "name": "herdr_interrupt_cli",
            "arguments": { "cli_id": &cli_id },
        }),
    );
    assert_eq!(interrupted["result"]["isError"], false, "{interrupted}");

    let stopped = mcp.request(
        17,
        "tools/call",
        json!({
            "name": "herdr_stop_cli",
            "arguments": { "cli_id": &cli_id },
        }),
    );
    assert_eq!(stopped["result"]["isError"], false, "{stopped}");
    assert_eq!(
        stopped["result"]["structuredContent"]["data"]["stopped"],
        true
    );

    let stopped_again = mcp.request(
        18,
        "tools/call",
        json!({
            "name": "herdr_stop_cli",
            "arguments": { "cli_id": &cli_id },
        }),
    );
    assert_eq!(stopped_again["result"]["isError"], false);
    assert_eq!(
        stopped_again["result"]["structuredContent"]["data"]["already_stopped"],
        true
    );
    mcp.close_and_wait();
}

#[test]
fn queued_messages_can_be_drained_and_acked_after_server_shutdown() {
    let mut server = SessionServer::start();
    let mut mcp = McpChild::spawn_for(&server.base, &server.session);
    mcp.initialize();

    let launched = mcp.request(
        2,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "mcp-offline-enqueue",
                "argv": [
                    env!("CARGO_BIN_EXE_herdr"),
                    "desktop",
                    "enqueue",
                    "--message",
                    "queued-while-live"
                ],
                "cwd": server.base.to_string_lossy(),
            },
        }),
    );
    assert_eq!(launched["result"]["isError"], false, "{launched}");
    let cli_id = launched["result"]["structuredContent"]["data"]["cli_id"]
        .as_str()
        .expect("launched cli_id")
        .to_string();

    thread::sleep(Duration::from_millis(400));
    server.stop();
    let health = mcp.request(
        3,
        "tools/call",
        json!({ "name": "herdr_health", "arguments": {} }),
    );
    assert_eq!(
        health["result"]["structuredContent"]["data"]["herdr_available"],
        false
    );

    let mut leased = None;
    for request_id in 4..14 {
        let drained = mcp.request(
            request_id,
            "tools/call",
            json!({
                "name": "herdr_drain_messages",
                "arguments": { "cli_id": &cli_id },
            }),
        );
        assert_eq!(drained["result"]["isError"], false, "{drained}");
        assert_eq!(drained["result"]["content"], json!([]));
        let data = &drained["result"]["structuredContent"]["data"];
        let lease_id = data["lease_id"].as_str().expect("lease id").to_string();
        if data["messages"]
            .as_array()
            .is_some_and(|messages| !messages.is_empty())
        {
            assert_eq!(data["messages"][0]["text"], "queued-while-live");
            leased = Some(lease_id);
            break;
        }
        let ack_empty = mcp.request(
            request_id + 20,
            "tools/call",
            json!({
                "name": "herdr_ack_messages",
                "arguments": { "cli_id": &cli_id, "lease_id": lease_id },
            }),
        );
        assert_eq!(ack_empty["result"]["isError"], false, "{ack_empty}");
        thread::sleep(Duration::from_millis(50));
    }
    let lease_id = leased.expect("desktop message was queued before server shutdown");
    let ack = mcp.request(
        40,
        "tools/call",
        json!({
            "name": "herdr_ack_messages",
            "arguments": { "cli_id": &cli_id, "lease_id": &lease_id },
        }),
    );
    assert_eq!(ack["result"]["isError"], false, "{ack}");
    assert_eq!(ack["result"]["structuredContent"]["data"]["deleted"], 1);
    let ack_again = mcp.request(
        41,
        "tools/call",
        json!({
            "name": "herdr_ack_messages",
            "arguments": { "cli_id": &cli_id, "lease_id": &lease_id },
        }),
    );
    assert_eq!(
        ack_again["result"]["structuredContent"]["data"]["already_acked"],
        true
    );
    mcp.close_and_wait();
}

#[test]
fn mcp_stdin_eof_does_not_stop_server_or_launched_cli() {
    let server = SessionServer::start();
    let mut mcp = McpChild::spawn_for(&server.base, &server.session);
    mcp.initialize();
    let launched = mcp.request(
        2,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "mcp-eof-cat",
                "argv": ["/bin/cat"],
                "cwd": server.base.to_string_lossy(),
            },
        }),
    );
    assert_eq!(launched["result"]["isError"], false, "{launched}");
    let pane_id = launched["result"]["structuredContent"]["data"]["pane_id"]
        .as_str()
        .expect("launched pane id")
        .to_string();
    mcp.close_and_wait();

    let mut observer = McpChild::spawn_for(&server.base, &server.session);
    observer.initialize();
    let health = observer.request(
        2,
        "tools/call",
        json!({ "name": "herdr_health", "arguments": {} }),
    );
    assert_eq!(
        health["result"]["structuredContent"]["data"]["herdr_available"],
        true
    );
    let process = observer.request(
        3,
        "tools/call",
        json!({
            "name": "herdr_pane_process_info",
            "arguments": { "pane_id": pane_id },
        }),
    );
    assert_eq!(process["result"]["isError"], false, "{process}");
    observer.close_and_wait();
}

#[test]
fn stale_terminal_identity_is_rejected_without_sending_input() {
    let server = SessionServer::start();
    let mut mcp = McpChild::spawn_for(&server.base, &server.session);
    mcp.initialize();
    let launched = mcp.request(
        2,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "mcp-stale-identity",
                "argv": ["/bin/cat"],
                "cwd": server.base.to_string_lossy(),
            },
        }),
    );
    assert_eq!(launched["result"]["isError"], false, "{launched}");
    let data = &launched["result"]["structuredContent"]["data"];
    let cli_id = data["cli_id"].as_str().expect("cli id").to_string();
    let pane_id = data["pane_id"].as_str().expect("pane id").to_string();

    let index_path = find_file(&server.base.join("state"), &format!("{cli_id}.json"));
    let mut index: Value = serde_json::from_slice(&fs::read(&index_path).expect("read CLI index"))
        .expect("parse CLI index");
    index["identity"]["terminal_id"] = json!("terminal_stale_identity");
    fs::write(
        &index_path,
        serde_json::to_vec(&index).expect("encode CLI index"),
    )
    .expect("write stale CLI index");

    let sent = mcp.request(
        3,
        "tools/call",
        json!({
            "name": "herdr_send_input",
            "arguments": {
                "cli_id": &cli_id,
                "text": "must-not-be-sent\n",
            },
        }),
    );
    assert_eq!(sent["result"]["isError"], true, "{sent}");
    assert_eq!(
        sent["result"]["structuredContent"]["error"]["code"],
        "cli_identity_mismatch"
    );
    assert_eq!(sent["result"]["content"], json!([]));

    let read = mcp.request(
        4,
        "tools/call",
        json!({
            "name": "herdr_read_pane",
            "arguments": {
                "pane_id": pane_id,
                "source": "recent",
                "lines": 20,
                "format": "text",
            },
        }),
    );
    assert_eq!(read["result"]["isError"], false, "{read}");
    assert!(!read["result"]["structuredContent"]["data"]["read"]["text"]
        .as_str()
        .is_some_and(|text| text.contains("must-not-be-sent")));
    mcp.close_and_wait();
}

#[test]
fn stale_launch_pruning_requires_agent_list_confirmation() {
    let server = SessionServer::start();
    let mut mcp = McpChild::spawn_for(&server.base, &server.session);
    mcp.initialize();

    let first = mcp.request(
        2,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "mcp-stale-live",
                "argv": ["/bin/cat"],
                "cwd": server.base.to_string_lossy(),
            },
        }),
    );
    assert_eq!(first["result"]["isError"], false, "{first}");
    let first_data = &first["result"]["structuredContent"]["data"];
    let first_cli = first_data["cli_id"].as_str().expect("first cli id");
    let first_pane = first_data["pane_id"].as_str().expect("first pane id");
    let (first_marker, first_index) = age_cli_as_creating(&server.base, first_cli);

    let renamed = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args([
            "--session",
            &server.session,
            "agent",
            "rename",
            first_pane,
            &first_marker,
        ])
        .env("XDG_CONFIG_HOME", server.base.join("config"))
        .env("XDG_STATE_HOME", server.base.join("state"))
        .env_remove("HERDR_SOCKET_PATH")
        .env_remove("HERDR_CLIENT_SOCKET_PATH")
        .env_remove("HERDR_ENV")
        .output()
        .expect("rename live agent to launch marker");
    assert!(
        renamed.status.success(),
        "agent rename failed: stdout={} stderr={}",
        String::from_utf8_lossy(&renamed.stdout),
        String::from_utf8_lossy(&renamed.stderr)
    );

    let second = mcp.request(
        3,
        "tools/call",
        json!({
            "name": "herdr_launch_cli",
            "arguments": {
                "name": "mcp-stale-absent",
                "argv": ["/bin/cat"],
                "cwd": server.base.to_string_lossy(),
            },
        }),
    );
    assert_eq!(second["result"]["isError"], false, "{second}");
    assert!(first_index.exists(), "live marker record was pruned");
    let second_cli = second["result"]["structuredContent"]["data"]["cli_id"]
        .as_str()
        .expect("second cli id")
        .to_string();

    let reconciled_send = mcp.request(
        4,
        "tools/call",
        json!({
            "name": "herdr_send_input",
            "arguments": { "cli_id": first_cli, "text": "still-controlled\n" },
        }),
    );
    assert_eq!(
        reconciled_send["result"]["isError"], false,
        "{reconciled_send}"
    );

    let stopped_second = mcp.request(
        5,
        "tools/call",
        json!({
            "name": "herdr_stop_cli",
            "arguments": { "cli_id": &second_cli },
        }),
    );
    assert_eq!(stopped_second["result"]["isError"], false);
    let (_, second_index) = age_cli_as_creating(&server.base, &second_cli);
    let confirmed_absent = mcp.request(
        6,
        "tools/call",
        json!({
            "name": "herdr_stop_cli",
            "arguments": { "cli_id": &second_cli },
        }),
    );
    assert_eq!(confirmed_absent["result"]["isError"], false);
    assert_eq!(
        confirmed_absent["result"]["structuredContent"]["data"]["already_stopped"],
        true
    );
    assert!(!second_index.exists(), "confirmed-absent record remains");

    let stopped_first = mcp.request(
        7,
        "tools/call",
        json!({
            "name": "herdr_stop_cli",
            "arguments": { "cli_id": first_cli },
        }),
    );
    assert_eq!(stopped_first["result"]["isError"], false);
    mcp.close_and_wait();
}

#[test]
fn hidden_commands_stay_out_of_root_help_and_completions() {
    let help = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .arg("--help")
        .output()
        .expect("run root help");
    assert!(help.status.success());
    let help = String::from_utf8_lossy(&help.stdout);
    assert!(!help.contains("herdr mcp"));
    assert!(!help.contains("herdr desktop"));
    assert!(!help.contains("enqueue"));

    let completion = Command::new(env!("CARGO_BIN_EXE_herdr"))
        .args(["completion", "zsh"])
        .output()
        .expect("generate zsh completion");
    assert!(completion.status.success());
    let completion = String::from_utf8_lossy(&completion.stdout);
    assert!(!completion.contains("mcp"));
    assert!(!completion.contains("desktop"));
    assert!(!completion.contains("enqueue"));
}
