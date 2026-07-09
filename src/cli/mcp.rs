use std::io;

const MCP_USAGE: &str = "usage: herdr [--session NAME] mcp serve";

pub(super) fn run_mcp_command(args: &[String]) -> io::Result<i32> {
    match args {
        [command] if command == "serve" => crate::desktop_mcp::run_stdio(),
        [help] if matches!(help.as_str(), "help" | "--help" | "-h") => {
            eprintln!("{MCP_USAGE}");
            Ok(0)
        }
        _ => {
            eprintln!("{MCP_USAGE}");
            Ok(2)
        }
    }
}
