use std::io::{self, Read};

use crate::desktop_queue::{enqueue_from_env, QueueErrorKind};

const MAX_MESSAGE_BYTES: u64 = 64 * 1024;
const USAGE: &str = "usage: herdr desktop enqueue [--kind info|progress|result|question|error] [--correlation-id ID] (--message TEXT|--stdin)";

pub(super) fn run_desktop_command(args: &[String]) -> io::Result<i32> {
    match args.first().map(String::as_str) {
        Some("enqueue") => run_enqueue(&args[1..]),
        _ => {
            eprintln!("{USAGE}");
            Ok(2)
        }
    }
}

fn run_enqueue(args: &[String]) -> io::Result<i32> {
    let mut kind = "info".to_string();
    let mut kind_provided = false;
    let mut correlation_id = None;
    let mut message = None;
    let mut read_stdin = false;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--kind" => {
                let Some(value) = args.get(index + 1) else {
                    return usage_error("--kind requires a value");
                };
                if kind_provided {
                    return usage_error("--kind may only be provided once");
                }
                kind_provided = true;
                kind = value.clone();
                index += 2;
            }
            "--correlation-id" => {
                let Some(value) = args.get(index + 1) else {
                    return usage_error("--correlation-id requires a value");
                };
                if correlation_id.is_some() {
                    return usage_error("--correlation-id may only be provided once");
                }
                correlation_id = Some(value.clone());
                index += 2;
            }
            "--message" => {
                let Some(value) = args.get(index + 1) else {
                    return usage_error("--message requires a value");
                };
                if message.replace(value.clone()).is_some() {
                    return usage_error("--message may only be provided once");
                }
                index += 2;
            }
            "--stdin" => {
                if read_stdin {
                    return usage_error("--stdin may only be provided once");
                }
                read_stdin = true;
                index += 1;
            }
            _ => return usage_error(&format!("unknown desktop enqueue option {}", args[index])),
        }
    }
    if read_stdin == message.is_some() {
        return usage_error("provide exactly one of --message or --stdin");
    }
    if read_stdin {
        let mut bytes = Vec::new();
        io::stdin()
            .take(MAX_MESSAGE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_MESSAGE_BYTES {
            return usage_error("stdin message exceeds 65536 bytes");
        }
        message = match String::from_utf8(bytes) {
            Ok(message) => Some(message),
            Err(_) => return usage_error("stdin message must be UTF-8"),
        };
    }
    let message = message.unwrap_or_default();
    match enqueue_from_env(&kind, correlation_id.as_deref(), message) {
        Ok(receipt) => {
            let encoded = serde_json::to_string(&receipt)
                .map_err(|err| io::Error::other(format!("encode enqueue receipt: {err}")))?;
            println!("{encoded}");
            Ok(0)
        }
        Err(err) => {
            eprintln!("desktop enqueue failed: {err}");
            Ok(match err.kind() {
                QueueErrorKind::Invalid => 2,
                QueueErrorKind::PermissionDenied
                | QueueErrorKind::Closed
                | QueueErrorKind::NotFound
                | QueueErrorKind::CliNotFound
                | QueueErrorKind::LeaseNotFound => 3,
                QueueErrorKind::Full => 4,
                QueueErrorKind::Corrupt | QueueErrorKind::LeaseExpired | QueueErrorKind::Io => 5,
            })
        }
    }
}

fn usage_error(message: &str) -> io::Result<i32> {
    eprintln!("{message}");
    eprintln!("{USAGE}");
    Ok(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_kind_is_rejected_before_enqueue() {
        let args = [
            "--kind".to_string(),
            "info".to_string(),
            "--kind".to_string(),
            "error".to_string(),
            "--message".to_string(),
            "hello".to_string(),
        ];
        assert_eq!(run_enqueue(&args).unwrap(), 2);
    }

    #[test]
    fn duplicate_correlation_id_is_rejected_before_enqueue() {
        let args = [
            "--correlation-id".to_string(),
            "first".to_string(),
            "--correlation-id".to_string(),
            "second".to_string(),
            "--message".to_string(),
            "hello".to_string(),
        ];
        assert_eq!(run_enqueue(&args).unwrap(), 2);
    }
}
