use std::ffi::OsStr;
use std::io;

const MCP_USAGE: &str = "usage:\n  herdr [--session NAME] mcp install <client> [--full-control]\n  herdr [--session NAME] mcp status [client]\n  herdr [--session NAME] mcp uninstall <client>";
const MCP_FULL_CONTROL_ENV_VAR: &str = "HERDR_MCP_FULL_CONTROL";
const MCP_FULL_CONTROL_ENV_ERROR: &str = "HERDR_MCP_FULL_CONTROL must be exactly 1 when set";

fn serve_access_mode(
    args: &[String],
    full_control_env: Option<&OsStr>,
) -> Result<Option<crate::desktop_mcp::AccessMode>, &'static str> {
    let explicit_full_control = match args {
        [command] if command == "serve" => false,
        [command, flag] if command == "serve" && flag == "--full-control" => true,
        _ => return Ok(None),
    };
    let environment_full_control = match full_control_env {
        None => false,
        Some(value) if value == OsStr::new("1") => true,
        Some(_) => return Err(MCP_FULL_CONTROL_ENV_ERROR),
    };
    Ok(Some(if explicit_full_control || environment_full_control {
        crate::desktop_mcp::AccessMode::FullControl
    } else {
        crate::desktop_mcp::AccessMode::Restricted
    }))
}

fn print_usage() {
    eprintln!("{MCP_USAGE}");
}

fn run_public_command(args: &[String]) -> io::Result<Option<i32>> {
    match args {
        [command, client] if command == "install" => {
            crate::mcp_install::install(client, false).map(Some)
        }
        [command, client, flag] if command == "install" && flag == "--full-control" => {
            crate::mcp_install::install(client, true).map(Some)
        }
        [command] if command == "status" => crate::mcp_install::status(None).map(Some),
        [command, client] if command == "status" => {
            crate::mcp_install::status(Some(client)).map(Some)
        }
        [command, client] if command == "uninstall" => {
            crate::mcp_install::uninstall(client).map(Some)
        }
        _ => Ok(None),
    }
}

pub(super) fn run_mcp_command(args: &[String]) -> io::Result<i32> {
    match serve_access_mode(args, std::env::var_os(MCP_FULL_CONTROL_ENV_VAR).as_deref()) {
        Ok(Some(access_mode)) => return crate::desktop_mcp::run_stdio(access_mode),
        Ok(None) => {}
        Err(message) => {
            eprintln!("error: {message}");
            return Ok(2);
        }
    }
    if let Some(exit_code) = run_public_command(args)? {
        return Ok(exit_code);
    }
    if matches!(args, [help] if matches!(help.as_str(), "help" | "--help" | "-h")) {
        print_usage();
        return Ok(0);
    }
    print_usage();
    Ok(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn serve_defaults_to_restricted_and_supports_two_explicit_full_control_grants() {
        assert_eq!(
            serve_access_mode(&args(&["serve"]), None),
            Ok(Some(crate::desktop_mcp::AccessMode::Restricted))
        );
        assert_eq!(
            serve_access_mode(&args(&["serve", "--full-control"]), None),
            Ok(Some(crate::desktop_mcp::AccessMode::FullControl))
        );
        assert_eq!(
            serve_access_mode(&args(&["serve"]), Some(OsStr::new("1"))),
            Ok(Some(crate::desktop_mcp::AccessMode::FullControl))
        );
    }

    #[test]
    fn serve_rejects_noncanonical_full_control_environment_values() {
        for value in ["", "0", "true", "yes", " 1"] {
            assert_eq!(
                serve_access_mode(&args(&["serve"]), Some(OsStr::new(value))),
                Err(MCP_FULL_CONTROL_ENV_ERROR)
            );
        }
        assert_eq!(
            serve_access_mode(
                &args(&["serve", "--full-control"]),
                Some(OsStr::new("true"))
            ),
            Err(MCP_FULL_CONTROL_ENV_ERROR)
        );
    }

    #[test]
    fn public_usage_does_not_disclose_the_bridge_serve_command() {
        assert!(MCP_USAGE.contains("mcp install"));
        assert!(MCP_USAGE.contains("mcp status"));
        assert!(MCP_USAGE.contains("mcp uninstall"));
        assert!(!MCP_USAGE.contains("serve"));
        assert_eq!(
            serve_access_mode(&args(&["serve", "--unknown"]), None),
            Ok(None)
        );
    }
}
