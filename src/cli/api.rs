const API_SCHEMA_JSON: &str = include_str!("../../docs/next/api/herdr-api.schema.json");

use crate::api::schema::{EmptyParams, Method, Request};

pub(super) fn run_api_command(args: &[String]) -> std::io::Result<i32> {
    let Some(subcommand) = args.first().map(String::as_str) else {
        print_api_help();
        return Ok(2);
    };

    match subcommand {
        "schema" => api_schema(&args[1..]),
        "snapshot" => api_snapshot(&args[1..]),
        "bridge" => api_bridge(&args[1..]),
        "help" | "--help" | "-h" => {
            print_api_help();
            Ok(0)
        }
        _ => {
            print_api_help();
            Ok(2)
        }
    }
}

fn api_bridge(args: &[String]) -> std::io::Result<i32> {
    let shell_context = match args {
        [] => false,
        [flag] if flag == "--shell-context" => true,
        _ => {
            eprintln!("usage: herdr api bridge [--shell-context]");
            return Ok(2);
        }
    };

    let mut request = String::new();
    std::io::BufRead::read_line(&mut std::io::stdin().lock(), &mut request)?;
    if request.trim().is_empty() {
        return Ok(0);
    }
    if shell_context {
        request = inject_terminal_delegate_shell_context(
            &request,
            &std::env::current_dir()?,
            std::env::vars(),
        )?;
    }

    let mut stream = crate::ipc::connect_local_stream(&crate::api::socket_path())?;
    std::io::Write::write_all(&mut stream, request.as_bytes())?;
    std::io::Write::flush(&mut stream)?;

    let mut stdout = std::io::stdout().lock();
    std::io::copy(&mut stream, &mut stdout)?;
    std::io::Write::flush(&mut stdout)?;
    Ok(0)
}

fn inject_terminal_delegate_shell_context(
    request: &str,
    cwd: &std::path::Path,
    env: impl IntoIterator<Item = (String, String)>,
) -> std::io::Result<String> {
    let mut value: serde_json::Value = serde_json::from_str(request)?;
    if value["method"].as_str() != Some("terminal.delegate.create") {
        return Ok(request.to_string());
    }
    let params = value
        .get_mut("params")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "terminal.delegate.create requires object params",
            )
        })?;
    if params.get("cwd").is_none_or(serde_json::Value::is_null) {
        params.insert(
            "cwd".into(),
            serde_json::Value::String(cwd.display().to_string()),
        );
    }
    let mut inherited = serde_json::Map::new();
    for (key, value) in env {
        if !key.starts_with("HERDR_") {
            inherited.insert(key, serde_json::Value::String(value));
        }
    }
    if let Some(explicit) = params.get("env").and_then(serde_json::Value::as_object) {
        inherited.extend(explicit.clone());
    }
    params.insert("env".into(), serde_json::Value::Object(inherited));
    let mut encoded = serde_json::to_string(&value)?;
    encoded.push('\n');
    Ok(encoded)
}

fn api_schema(args: &[String]) -> std::io::Result<i32> {
    match args {
        [] => {
            print!("{}", schema_summary_text()?);
        }
        [flag] if flag == "--json" => {
            print!("{API_SCHEMA_JSON}");
        }
        [flag, path] if flag == "--output" => {
            write_schema_file(std::path::Path::new(path))?;
            println!("wrote API schema to {path}");
        }
        [flag] if flag == "--output" => {
            eprintln!("missing value for --output");
            return Ok(2);
        }
        [flag] if matches!(flag.as_str(), "help" | "--help" | "-h") => {
            print_api_schema_help();
        }
        [other] if other.starts_with('-') => {
            eprintln!("unknown option: {other}");
            return Ok(2);
        }
        _ => {
            print_api_schema_help();
            return Ok(2);
        }
    }
    Ok(0)
}

fn api_snapshot(args: &[String]) -> std::io::Result<i32> {
    if !args.is_empty() {
        eprintln!("usage: herdr api snapshot");
        return Ok(2);
    }

    super::print_response(&super::send_request(&Request {
        id: "cli:api:snapshot".into(),
        method: Method::SessionSnapshot(EmptyParams::default()),
    })?)
}

fn write_schema_file(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::write(path, API_SCHEMA_JSON)
}

fn schema_summary_text() -> std::io::Result<String> {
    let value: serde_json::Value = serde_json::from_str(API_SCHEMA_JSON)?;
    let protocol = value
        .get("protocol")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| std::io::Error::other("API schema is missing protocol"))?;
    let schema_version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| std::io::Error::other("API schema is missing schema_version"))?;
    let mut schemas: Vec<&str> = value
        .get("schemas")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| std::io::Error::other("API schema is missing schemas"))?
        .keys()
        .map(String::as_str)
        .collect();
    schemas.sort();

    Ok(format!(
        "Herdr API schema\nprotocol: {}\nschema_version: {}\nschemas: {}\n\nUse `herdr api schema --json` to print the full schema.\nUse `herdr api schema --output PATH` to write it to a file.\n",
        protocol,
        schema_version,
        schemas.join(", ")
    ))
}

fn print_api_help() {
    eprintln!("herdr api commands:");
    eprintln!("  herdr api snapshot");
    eprintln!("  herdr api schema [--json | --output PATH]");
    eprintln!("  herdr api bridge");
}

fn print_api_schema_help() {
    eprintln!("usage: herdr api schema [--json | --output PATH]");
}

#[cfg(test)]
mod tests {
    #[test]
    fn schema_summary_text_stays_human_sized() {
        let text = super::schema_summary_text().unwrap();
        assert!(text.contains("Herdr API schema"));
        assert!(text.contains("Use `herdr api schema --json`"));
        assert!(text.len() < 400);
    }

    #[test]
    fn shell_context_injection_only_changes_fresh_terminal_delegation() {
        let request = r#"{"id":"create","method":"terminal.delegate.create","params":{"label":"remote","env":{"EXPLICIT":"yes"},"owner":{"peer_id":"a","pane_id":"p","route":["a"]}}}"#;
        let injected = super::inject_terminal_delegate_shell_context(
            request,
            std::path::Path::new("/remote/home"),
            [
                ("SSH_AUTH_SOCK".into(), "/tmp/ssh.sock".into()),
                ("HERDR_SOCKET_PATH".into(), "/tmp/wrong.sock".into()),
                ("EXPLICIT".into(), "inherited".into()),
            ],
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&injected).unwrap();
        assert_eq!(value["params"]["cwd"], "/remote/home");
        assert_eq!(value["params"]["env"]["SSH_AUTH_SOCK"], "/tmp/ssh.sock");
        assert_eq!(value["params"]["env"]["EXPLICIT"], "yes");
        assert!(value["params"]["env"].get("HERDR_SOCKET_PATH").is_none());

        let ping = "{\"id\":\"ping\",\"method\":\"ping\",\"params\":{}}\n";
        assert_eq!(
            super::inject_terminal_delegate_shell_context(
                ping,
                std::path::Path::new("/ignored"),
                std::iter::empty(),
            )
            .unwrap(),
            ping
        );
    }
}
