use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};

const ENTRY_NAME: &str = "herdr";
const DEFAULT_SESSION: &str = "default";
const SESSION_ENV: &str = "HERDR_SESSION";
const MANAGED_ENV: &str = "HERDR_MCP_MANAGED";
const FULL_CONTROL_ENV: &str = "HERDR_MCP_FULL_CONTROL";

pub fn install(client: &str, full_control: bool) -> io::Result<i32> {
    let Some(client) = parse_client(client) else {
        return Ok(2);
    };
    let context = InstallContext::from_process(full_control)?;
    let mut runner = SystemRunner;

    match install_with_runner(client, &context, &mut runner)? {
        InstallOutcome::Installed => {
            println!(
                "Installed Herdr MCP in {} ({}, session {}).",
                client.label(),
                context.access_label(),
                context.session
            );
            Ok(0)
        }
        InstallOutcome::Updated => {
            println!(
                "Updated Herdr MCP in {} ({}, session {}).",
                client.label(),
                context.access_label(),
                context.session
            );
            Ok(0)
        }
        InstallOutcome::Conflict => {
            eprintln!(
                "Refusing to replace {} MCP entry '{ENTRY_NAME}': it is not managed by Herdr.",
                client.label()
            );
            Ok(1)
        }
    }
}

pub fn uninstall(client: &str) -> io::Result<i32> {
    let Some(client) = parse_client(client) else {
        return Ok(2);
    };
    let mut runner = SystemRunner;

    match uninstall_with_runner(client, &mut runner)? {
        UninstallOutcome::Removed => {
            println!("Removed Herdr MCP from {}.", client.label());
            Ok(0)
        }
        UninstallOutcome::NotInstalled => {
            println!("Herdr MCP is not installed in {}.", client.label());
            Ok(0)
        }
        UninstallOutcome::Conflict => {
            eprintln!(
                "Refusing to remove {} MCP entry '{ENTRY_NAME}': it is not managed by Herdr.",
                client.label()
            );
            Ok(1)
        }
    }
}

pub fn status(client: Option<&str>) -> io::Result<i32> {
    let clients: Vec<Client> = match client {
        Some(client) => {
            let Some(client) = parse_client(client) else {
                return Ok(2);
            };
            vec![client]
        }
        None => Client::ALL.to_vec(),
    };

    let mut exit_code = 0;
    let mut runner = SystemRunner;
    for client in clients {
        match inspect(client, &mut runner) {
            Ok(EntryState::Managed(registration)) => {
                let session = registration.session.as_deref().unwrap_or("unknown");
                let access = if registration.full_control {
                    "full control"
                } else {
                    "restricted"
                };
                println!("{}: installed ({access}, session {session})", client);
            }
            Ok(EntryState::Missing) => {
                println!("{}: not installed", client);
                exit_code = 1;
            }
            Ok(EntryState::Foreign) => {
                println!(
                    "{}: conflict (entry '{ENTRY_NAME}' is not managed by Herdr)",
                    client
                );
                exit_code = 1;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                println!("{}: unavailable (client executable not found)", client);
                exit_code = 1;
            }
            Err(error) => {
                eprintln!("{}: unable to inspect MCP configuration: {error}", client);
                exit_code = 1;
            }
        }
    }

    Ok(exit_code)
}

fn parse_client(value: &str) -> Option<Client> {
    match Client::parse(value) {
        Some(client) => Some(client),
        None => {
            eprintln!(
                "Currently unsupported: MCP client '{value}'. Supported clients: codex, claude, hermes, openclaw."
            );
            None
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Client {
    Codex,
    Claude,
    Hermes,
    OpenClaw,
}

impl Client {
    const ALL: [Self; 4] = [Self::Codex, Self::Claude, Self::Hermes, Self::OpenClaw];

    fn parse(value: &str) -> Option<Self> {
        match value {
            "codex" => Some(Self::Codex),
            "claude" | "claude-code" => Some(Self::Claude),
            "hermes" => Some(Self::Hermes),
            "openclaw" => Some(Self::OpenClaw),
            _ => None,
        }
    }

    fn executable(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Hermes => "hermes",
            Self::OpenClaw => "openclaw",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Codex => "Codex",
            Self::Claude => "Claude Code",
            Self::Hermes => "Hermes",
            Self::OpenClaw => "OpenClaw",
        }
    }
}

impl fmt::Display for Client {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Hermes => "hermes",
            Self::OpenClaw => "openclaw",
        };
        formatter.write_str(value)
    }
}

#[derive(Debug)]
struct InstallContext {
    executable: PathBuf,
    session: String,
    full_control: bool,
}

impl InstallContext {
    #[cfg(unix)]
    fn from_process(full_control: bool) -> io::Result<Self> {
        let session = std::env::var(SESSION_ENV).unwrap_or_else(|_| DEFAULT_SESSION.to_string());
        if session.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{SESSION_ENV} cannot be empty"),
            ));
        }

        Ok(Self {
            executable: std::env::current_exe()?,
            session,
            full_control,
        })
    }

    #[cfg(not(unix))]
    fn from_process(_full_control: bool) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Herdr MCP integration is supported on macOS and Linux",
        ))
    }

    fn executable_string(&self) -> io::Result<String> {
        self.executable
            .to_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the Herdr executable path is not valid UTF-8",
                )
            })
    }

    fn environment(&self) -> Vec<(String, String)> {
        let mut environment = vec![
            (SESSION_ENV.to_string(), self.session.clone()),
            (MANAGED_ENV.to_string(), "1".to_string()),
        ];
        if self.full_control {
            environment.push((FULL_CONTROL_ENV.to_string(), "1".to_string()));
        }
        environment
    }

    fn access_label(&self) -> &'static str {
        if self.full_control {
            "full control"
        } else {
            "restricted"
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Registration {
    session: Option<String>,
    full_control: bool,
}

impl Registration {
    fn matches(&self, context: &InstallContext) -> bool {
        self.session.as_deref() == Some(context.session.as_str())
            && self.full_control == context.full_control
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EntryState {
    Missing,
    Managed(Registration),
    Foreign,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallOutcome {
    Installed,
    Updated,
    Conflict,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UninstallOutcome {
    Removed,
    NotInstalled,
    Conflict,
}

fn install_with_runner(
    client: Client,
    context: &InstallContext,
    runner: &mut dyn Runner,
) -> io::Result<InstallOutcome> {
    let existing = inspect(client, runner)?;
    let updating = match existing {
        EntryState::Missing => false,
        EntryState::Managed(_) => true,
        EntryState::Foreign => return Ok(InstallOutcome::Conflict),
    };

    if updating && client == Client::Claude {
        run_checked(
            runner,
            remove_invocation(client),
            "remove existing registration",
        )?;
    }

    let invocation = install_invocation(client, context, updating)?;
    run_checked(runner, invocation, "save registration")?;

    match inspect(client, runner)? {
        EntryState::Managed(registration) if registration.matches(context) => {
            if updating {
                Ok(InstallOutcome::Updated)
            } else {
                Ok(InstallOutcome::Installed)
            }
        }
        EntryState::Managed(_) => Err(io::Error::other(format!(
            "{} saved entry '{ENTRY_NAME}', but its session or access mode does not match",
            client.label()
        ))),
        EntryState::Missing => Err(io::Error::other(format!(
            "{} did not save MCP entry '{ENTRY_NAME}'",
            client.label()
        ))),
        EntryState::Foreign => Err(io::Error::other(format!(
            "{} saved MCP entry '{ENTRY_NAME}' without the Herdr ownership marker",
            client.label()
        ))),
    }
}

fn uninstall_with_runner(client: Client, runner: &mut dyn Runner) -> io::Result<UninstallOutcome> {
    match inspect(client, runner)? {
        EntryState::Missing => return Ok(UninstallOutcome::NotInstalled),
        EntryState::Foreign => return Ok(UninstallOutcome::Conflict),
        EntryState::Managed(_) => {}
    }

    run_checked(runner, remove_invocation(client), "remove registration")?;
    match inspect(client, runner)? {
        EntryState::Missing => Ok(UninstallOutcome::Removed),
        _ => Err(io::Error::other(format!(
            "{} did not remove MCP entry '{ENTRY_NAME}'",
            client.label()
        ))),
    }
}

fn inspect(client: Client, runner: &mut dyn Runner) -> io::Result<EntryState> {
    if client == Client::Hermes {
        return inspect_hermes(runner);
    }

    let invocation = query_invocation(client);
    let output = runner.run(&invocation)?;
    let text = output.combined_text();
    if !output.success {
        if reports_missing(&text) {
            return Ok(EntryState::Missing);
        }
        return Err(command_error(
            client,
            "inspect registration",
            &invocation,
            &output,
        ));
    }

    Ok(entry_state_from_text(&text))
}

fn inspect_hermes(runner: &mut dyn Runner) -> io::Result<EntryState> {
    let list = Invocation::new(Client::Hermes, ["mcp", "list"]);
    let output = runner.run(&list)?;
    if !output.success {
        return Err(command_error(
            Client::Hermes,
            "list registrations",
            &list,
            &output,
        ));
    }
    if !hermes_list_contains_entry(&output.combined_text()) {
        return Ok(EntryState::Missing);
    }

    let config = Invocation::new(Client::Hermes, ["config"]);
    let output = runner.run(&config)?;
    if !output.success {
        return Err(command_error(
            Client::Hermes,
            "inspect registration",
            &config,
            &output,
        ));
    }

    let text = output.combined_text();
    let Some(block) = hermes_entry_block(&text) else {
        return Ok(EntryState::Foreign);
    };
    Ok(entry_state_from_text(&block))
}

fn query_invocation(client: Client) -> Invocation {
    match client {
        Client::Codex => Invocation::new(client, ["mcp", "get", ENTRY_NAME, "--json"]),
        Client::Claude => Invocation::new(client, ["mcp", "get", ENTRY_NAME]),
        Client::OpenClaw => Invocation::new(client, ["mcp", "show", ENTRY_NAME, "--json"]),
        Client::Hermes => unreachable!("Hermes inspection uses its list and config commands"),
    }
}

fn install_invocation(
    client: Client,
    context: &InstallContext,
    updating: bool,
) -> io::Result<Invocation> {
    let executable = context.executable_string()?;
    let environment = context.environment();

    match client {
        Client::Codex => {
            let mut args = vec!["mcp".into(), "add".into(), ENTRY_NAME.into()];
            for (key, value) in environment {
                args.extend(["--env".into(), format!("{key}={value}")]);
            }
            args.extend(["--".into(), executable, "mcp".into(), "serve".into()]);
            Ok(Invocation::from_args(client, args))
        }
        Client::Claude => {
            let config = stdio_config_json(&executable, &environment);
            Ok(Invocation::new(
                client,
                ["mcp", "add-json", "--scope", "user", ENTRY_NAME, &config],
            ))
        }
        Client::Hermes => {
            let mut args = vec![
                "mcp".into(),
                "add".into(),
                ENTRY_NAME.into(),
                "--command".into(),
                executable,
                "--args".into(),
                "mcp".into(),
                "serve".into(),
                "--env".into(),
            ];
            args.extend(
                environment
                    .into_iter()
                    .map(|(key, value)| format!("{key}={value}")),
            );
            let stdin = if updating { "y\n\n" } else { "\n" };
            Ok(Invocation::from_args(client, args).with_stdin(stdin))
        }
        Client::OpenClaw => {
            let config = stdio_config_json(&executable, &environment);
            Ok(Invocation::new(client, ["mcp", "set", ENTRY_NAME, &config]))
        }
    }
}

fn remove_invocation(client: Client) -> Invocation {
    match client {
        Client::Codex => Invocation::new(client, ["mcp", "remove", ENTRY_NAME]),
        Client::Claude => Invocation::new(client, ["mcp", "remove", "--scope", "user", ENTRY_NAME]),
        Client::Hermes => Invocation::new(client, ["mcp", "remove", ENTRY_NAME]).with_stdin("\n"),
        Client::OpenClaw => Invocation::new(client, ["mcp", "unset", ENTRY_NAME]),
    }
}

fn stdio_config_json(executable: &str, environment: &[(String, String)]) -> String {
    let environment: BTreeMap<&str, &str> = environment
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    serde_json::json!({
        "type": "stdio",
        "command": executable,
        "args": ["mcp", "serve"],
        "env": environment,
    })
    .to_string()
}

fn run_checked(runner: &mut dyn Runner, invocation: Invocation, action: &str) -> io::Result<()> {
    let client = invocation.client;
    let output = runner.run(&invocation)?;
    if output.success {
        Ok(())
    } else {
        Err(command_error(client, action, &invocation, &output))
    }
}

fn command_error(
    client: Client,
    action: &str,
    invocation: &Invocation,
    output: &RunOutput,
) -> io::Error {
    let diagnostic = output.combined_text();
    let diagnostic = diagnostic.trim();
    let suffix = if diagnostic.is_empty() {
        format!("exit code {}", output.code.unwrap_or(1))
    } else {
        diagnostic.to_string()
    };
    io::Error::other(format!(
        "{} could not {action} with `{}`: {suffix}",
        client.label(),
        invocation.display()
    ))
}

fn entry_state_from_text(text: &str) -> EntryState {
    if extract_env_value(text, MANAGED_ENV).as_deref() != Some("1") {
        return EntryState::Foreign;
    }

    EntryState::Managed(Registration {
        session: extract_env_value(text, SESSION_ENV),
        full_control: extract_env_value(text, FULL_CONTROL_ENV).as_deref() == Some("1"),
    })
}

fn extract_env_value(text: &str, key: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Some(value) = find_json_string(&value, key) {
            return Some(value.to_string());
        }
    }

    for line in text.lines() {
        let Some(index) = line.find(key) else {
            continue;
        };
        if line[..index]
            .chars()
            .next_back()
            .is_some_and(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            continue;
        }

        let mut remainder = line[index + key.len()..].trim_start();
        remainder = remainder.trim_start_matches(['"', '\'', ':', '=', ' ']);
        if remainder.is_empty() {
            continue;
        }

        let value = if let Some(quote) =
            remainder.chars().next().filter(|c| *c == '"' || *c == '\'')
        {
            let quoted = &remainder[quote.len_utf8()..];
            quoted.split(quote).next().unwrap_or(quoted)
        } else {
            remainder
                .split(|character: char| {
                    character.is_whitespace() || matches!(character, ',' | '}' | ']' | '"' | '\'')
                })
                .next()
                .unwrap_or(remainder)
        };
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    None
}

fn find_json_string<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(value) = object.get(key).and_then(serde_json::Value::as_str) {
                return Some(value);
            }
            object
                .values()
                .find_map(|value| find_json_string(value, key))
        }
        serde_json::Value::Array(values) => {
            values.iter().find_map(|value| find_json_string(value, key))
        }
        _ => None,
    }
}

fn reports_missing(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    [
        "not found",
        "not configured",
        "does not exist",
        "no mcp server",
        "unknown mcp server",
    ]
    .iter()
    .any(|needle| text.contains(needle))
}

fn hermes_list_contains_entry(text: &str) -> bool {
    text.lines().any(|line| {
        line.split_whitespace().next().is_some_and(|value| {
            value.trim_matches(|c: char| !c.is_ascii_alphanumeric()) == ENTRY_NAME
        })
    })
}

fn hermes_entry_block(text: &str) -> Option<String> {
    let mut servers_indent = None;
    let mut entry_indent = None;
    let mut block = Vec::new();

    for line in text.lines() {
        let trimmed = line.trim();
        let indent = line.len() - line.trim_start().len();

        if servers_indent.is_none() {
            if yaml_key(trimmed) == Some("mcp_servers") {
                servers_indent = Some(indent);
            }
            continue;
        }

        let root_indent = servers_indent.expect("checked above");
        if entry_indent.is_none() {
            if !trimmed.is_empty() && indent <= root_indent {
                break;
            }
            if yaml_key(trimmed) == Some(ENTRY_NAME) {
                entry_indent = Some(indent);
                block.push(line);
            }
            continue;
        }

        let current_entry_indent = entry_indent.expect("checked above");
        if !trimmed.is_empty() && !trimmed.starts_with('#') && indent <= current_entry_indent {
            break;
        }
        block.push(line);
    }

    (!block.is_empty()).then(|| block.join("\n"))
}

fn yaml_key(line: &str) -> Option<&str> {
    let key = line.strip_suffix(':')?.trim();
    Some(key.trim_matches(['"', '\'']))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Invocation {
    client: Client,
    program: String,
    args: Vec<String>,
    stdin: Option<String>,
}

impl Invocation {
    fn new<const N: usize>(client: Client, args: [&str; N]) -> Self {
        Self::from_args(client, args.into_iter().map(ToOwned::to_owned).collect())
    }

    fn from_args(client: Client, args: Vec<String>) -> Self {
        Self {
            client,
            program: client.executable().to_string(),
            args,
            stdin: None,
        }
    }

    fn with_stdin(mut self, stdin: &str) -> Self {
        self.stdin = Some(stdin.to_string());
        self
    }

    fn display(&self) -> String {
        std::iter::once(self.program.as_str())
            .chain(self.args.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RunOutput {
    success: bool,
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl RunOutput {
    fn combined_text(&self) -> String {
        match (self.stdout.is_empty(), self.stderr.is_empty()) {
            (false, false) => format!("{}\n{}", self.stdout, self.stderr),
            (false, true) => self.stdout.clone(),
            (true, false) => self.stderr.clone(),
            (true, true) => String::new(),
        }
    }
}

trait Runner {
    fn run(&mut self, invocation: &Invocation) -> io::Result<RunOutput>;
}

struct SystemRunner;

impl Runner for SystemRunner {
    fn run(&mut self, invocation: &Invocation) -> io::Result<RunOutput> {
        let mut command = Command::new(&invocation.program);
        command
            .args(&invocation.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = if let Some(input) = &invocation.stdin {
            let mut child = command.stdin(Stdio::piped()).spawn()?;
            let mut stdin = child.stdin.take().ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "failed to open command stdin")
            })?;
            stdin.write_all(input.as_bytes())?;
            drop(stdin);
            child.wait_with_output()?
        } else {
            command.output()?
        };

        Ok(RunOutput {
            success: output.status.success(),
            code: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    fn context(full_control: bool) -> InstallContext {
        InstallContext {
            executable: PathBuf::from("/opt/herdr/bin/herdr"),
            session: "team-a".to_string(),
            full_control,
        }
    }

    fn success(stdout: impl Into<String>) -> RunOutput {
        RunOutput {
            success: true,
            code: Some(0),
            stdout: stdout.into(),
            stderr: String::new(),
        }
    }

    fn missing() -> RunOutput {
        RunOutput {
            success: false,
            code: Some(1),
            stdout: String::new(),
            stderr: "MCP server 'herdr' not found".to_string(),
        }
    }

    fn managed_json(full_control: bool) -> RunOutput {
        let mut env = serde_json::Map::from_iter([
            (
                SESSION_ENV.to_string(),
                serde_json::Value::String("team-a".to_string()),
            ),
            (
                MANAGED_ENV.to_string(),
                serde_json::Value::String("1".to_string()),
            ),
        ]);
        if full_control {
            env.insert(
                FULL_CONTROL_ENV.to_string(),
                serde_json::Value::String("1".to_string()),
            );
        }
        success(serde_json::json!({"name": ENTRY_NAME, "transport": {"env": env}}).to_string())
    }

    fn hermes_list(present: bool) -> RunOutput {
        if present {
            success(" MCP Servers:\n herdr            /opt/herdr/bin/herdr mcp serve       all          enabled")
        } else {
            success("No MCP servers configured.")
        }
    }

    fn hermes_config(full_control: bool) -> RunOutput {
        let full_control = if full_control {
            "\n      HERDR_MCP_FULL_CONTROL: '1'"
        } else {
            ""
        };
        success(format!(
            "model: test\nmcp_servers:\n  herdr:\n    command: /opt/herdr/bin/herdr\n    args: [mcp, serve]\n    env:\n      HERDR_SESSION: team-a\n      HERDR_MCP_MANAGED: '1'{full_control}\n  other:\n    command: other"
        ))
    }

    struct FakeRunner {
        outputs: VecDeque<RunOutput>,
        invocations: Vec<Invocation>,
    }

    impl FakeRunner {
        fn new(outputs: impl IntoIterator<Item = RunOutput>) -> Self {
            Self {
                outputs: outputs.into_iter().collect(),
                invocations: Vec::new(),
            }
        }
    }

    impl Runner for FakeRunner {
        fn run(&mut self, invocation: &Invocation) -> io::Result<RunOutput> {
            self.invocations.push(invocation.clone());
            Ok(self
                .outputs
                .pop_front()
                .expect("test did not provide a command result"))
        }
    }

    #[test]
    fn client_parser_accepts_only_supported_names_and_aliases() {
        assert_eq!(Client::parse("codex"), Some(Client::Codex));
        assert_eq!(Client::parse("claude"), Some(Client::Claude));
        assert_eq!(Client::parse("claude-code"), Some(Client::Claude));
        assert_eq!(Client::parse("hermes"), Some(Client::Hermes));
        assert_eq!(Client::parse("openclaw"), Some(Client::OpenClaw));
        assert_eq!(Client::parse("opencode"), None);
        assert_eq!(Client::parse("cursor"), None);
    }

    #[test]
    fn codex_install_uses_restricted_managed_environment_and_verifies() {
        let mut runner = FakeRunner::new([missing(), success("saved"), managed_json(false)]);

        let outcome = install_with_runner(Client::Codex, &context(false), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Installed);
        assert_eq!(runner.invocations.len(), 3);
        let install = &runner.invocations[1];
        assert_eq!(install.program, "codex");
        assert_eq!(&install.args[..3], ["mcp", "add", "herdr"]);
        assert!(install.args.contains(&"HERDR_SESSION=team-a".to_string()));
        assert!(install.args.contains(&"HERDR_MCP_MANAGED=1".to_string()));
        assert!(!install
            .args
            .iter()
            .any(|arg| arg.starts_with(FULL_CONTROL_ENV)));
        assert!(install.args.ends_with(&[
            "--".to_string(),
            "/opt/herdr/bin/herdr".to_string(),
            "mcp".to_string(),
            "serve".to_string(),
        ]));
    }

    #[test]
    fn codex_managed_entry_can_be_updated_to_full_control() {
        let mut runner =
            FakeRunner::new([managed_json(false), success("saved"), managed_json(true)]);

        let outcome = install_with_runner(Client::Codex, &context(true), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Updated);
        assert!(runner.invocations[1]
            .args
            .contains(&"HERDR_MCP_FULL_CONTROL=1".to_string()));
    }

    #[test]
    fn install_refuses_to_replace_a_foreign_entry() {
        let foreign = success(r#"{"name":"herdr","env":{"OWNER":"user"}}"#);
        let mut runner = FakeRunner::new([foreign]);

        let outcome = install_with_runner(Client::Codex, &context(false), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Conflict);
        assert_eq!(runner.invocations.len(), 1);
    }

    #[test]
    fn claude_update_removes_user_entry_then_uses_add_json() {
        let mut runner = FakeRunner::new([
            managed_json(false),
            success("removed"),
            success("added"),
            managed_json(true),
        ]);

        let outcome = install_with_runner(Client::Claude, &context(true), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Updated);
        assert_eq!(
            runner.invocations[1].args,
            ["mcp", "remove", "--scope", "user", "herdr"]
        );
        let add = &runner.invocations[2];
        assert_eq!(
            &add.args[..5],
            ["mcp", "add-json", "--scope", "user", "herdr"]
        );
        let config: serde_json::Value = serde_json::from_str(&add.args[5]).unwrap();
        assert_eq!(config["type"], "stdio");
        assert_eq!(config["command"], "/opt/herdr/bin/herdr");
        assert_eq!(config["args"], serde_json::json!(["mcp", "serve"]));
        assert_eq!(config["env"][SESSION_ENV], "team-a");
        assert_eq!(config["env"][MANAGED_ENV], "1");
        assert_eq!(config["env"][FULL_CONTROL_ENV], "1");
    }

    #[test]
    fn hermes_install_accepts_discovered_tools_and_verifies_config() {
        let mut runner = FakeRunner::new([
            hermes_list(false),
            success("saved"),
            hermes_list(true),
            hermes_config(false),
        ]);

        let outcome = install_with_runner(Client::Hermes, &context(false), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Installed);
        let install = &runner.invocations[1];
        assert_eq!(install.stdin.as_deref(), Some("\n"));
        assert_eq!(
            install.args,
            [
                "mcp",
                "add",
                "herdr",
                "--command",
                "/opt/herdr/bin/herdr",
                "--args",
                "mcp",
                "serve",
                "--env",
                "HERDR_SESSION=team-a",
                "HERDR_MCP_MANAGED=1",
            ]
        );
    }

    #[test]
    fn hermes_managed_update_confirms_replacement() {
        let mut runner = FakeRunner::new([
            hermes_list(true),
            hermes_config(false),
            success("saved"),
            hermes_list(true),
            hermes_config(true),
        ]);

        let outcome = install_with_runner(Client::Hermes, &context(true), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Updated);
        assert_eq!(runner.invocations[2].stdin.as_deref(), Some("y\n\n"));
        assert!(runner.invocations[2]
            .args
            .contains(&"HERDR_MCP_FULL_CONTROL=1".to_string()));
    }

    #[test]
    fn hermes_probe_that_does_not_save_is_an_install_failure() {
        let mut runner = FakeRunner::new([
            hermes_list(false),
            success("Connection failed; not saving."),
            hermes_list(false),
        ]);

        let error = install_with_runner(Client::Hermes, &context(false), &mut runner).unwrap_err();

        assert!(error.to_string().contains("did not save"));
    }

    #[test]
    fn openclaw_install_uses_set_with_owned_json() {
        let mut runner = FakeRunner::new([missing(), success("saved"), managed_json(false)]);

        let outcome = install_with_runner(Client::OpenClaw, &context(false), &mut runner).unwrap();

        assert_eq!(outcome, InstallOutcome::Installed);
        let set = &runner.invocations[1];
        assert_eq!(&set.args[..3], ["mcp", "set", "herdr"]);
        let config: serde_json::Value = serde_json::from_str(&set.args[3]).unwrap();
        assert_eq!(config["command"], "/opt/herdr/bin/herdr");
        assert_eq!(config["env"][MANAGED_ENV], "1");
        assert!(config["env"].get(FULL_CONTROL_ENV).is_none());
    }

    #[test]
    fn uninstall_removes_only_managed_openclaw_entry_and_verifies() {
        let mut runner = FakeRunner::new([managed_json(false), success("removed"), missing()]);

        let outcome = uninstall_with_runner(Client::OpenClaw, &mut runner).unwrap();

        assert_eq!(outcome, UninstallOutcome::Removed);
        assert_eq!(runner.invocations[1].args, ["mcp", "unset", "herdr"]);
    }

    #[test]
    fn uninstall_is_idempotent_and_refuses_foreign_entries() {
        let mut missing_runner = FakeRunner::new([missing()]);
        assert_eq!(
            uninstall_with_runner(Client::Codex, &mut missing_runner).unwrap(),
            UninstallOutcome::NotInstalled
        );

        let mut foreign_runner = FakeRunner::new([success(
            r#"{"name":"herdr","env":{"HERDR_MCP_MANAGED":"0"}}"#,
        )]);
        assert_eq!(
            uninstall_with_runner(Client::Codex, &mut foreign_runner).unwrap(),
            UninstallOutcome::Conflict
        );
        assert_eq!(foreign_runner.invocations.len(), 1);
    }

    #[test]
    fn text_environment_parser_handles_json_yaml_and_argument_forms() {
        assert_eq!(
            extract_env_value(r#"{"HERDR_SESSION":"json"}"#, SESSION_ENV).as_deref(),
            Some("json")
        );
        assert_eq!(
            extract_env_value("HERDR_SESSION: 'yaml'", SESSION_ENV).as_deref(),
            Some("yaml")
        );
        assert_eq!(
            extract_env_value("--env HERDR_SESSION=args OTHER=1", SESSION_ENV).as_deref(),
            Some("args")
        );
    }
}
