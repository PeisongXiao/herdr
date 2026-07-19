use std::io;

const MCP_USAGE: &str = "usage: herdr [--session NAME] mcp serve [--full-control]";

fn serve_access_mode(args: &[String]) -> Option<crate::desktop_mcp::AccessMode> {
    match args {
        [command] if command == "serve" => Some(crate::desktop_mcp::AccessMode::Restricted),
        [command, flag] if command == "serve" && flag == "--full-control" => {
            Some(crate::desktop_mcp::AccessMode::FullControl)
        }
        _ => None,
    }
}

pub(super) fn run_mcp_command(args: &[String]) -> io::Result<i32> {
    if let Some(access_mode) = serve_access_mode(args) {
        return crate::desktop_mcp::run_stdio(access_mode);
    }
    if matches!(args, [help] if matches!(help.as_str(), "help" | "--help" | "-h")) {
        eprintln!("{MCP_USAGE}");
        return Ok(0);
    }
    eprintln!("{MCP_USAGE}");
    Ok(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn serve_defaults_to_restricted_and_full_control_is_explicit() {
        assert_eq!(
            serve_access_mode(&args(&["serve"])),
            Some(crate::desktop_mcp::AccessMode::Restricted)
        );
        assert_eq!(
            serve_access_mode(&args(&["serve", "--full-control"])),
            Some(crate::desktop_mcp::AccessMode::FullControl)
        );
        assert_eq!(serve_access_mode(&args(&["serve", "--unknown"])), None);
    }
}
