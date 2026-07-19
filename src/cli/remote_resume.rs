//! `herdr remote-resume`: list handed-off remote panes pending re-acquire
//! and retry resuming them with interactive SSH authentication.

use crate::api::schema::{Method, RemoteResumeParams, Request};
use crate::remote_resume::ResumeStore;

const USAGE: &str = "usage: herdr remote-resume [--list] [--peer PEER_ID]";

pub(super) fn run_remote_resume_command(args: &[String]) -> std::io::Result<i32> {
    let mut list_only = false;
    let mut peer_id: Option<String> = None;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--list" => {
                if list_only {
                    return usage_error("--list may only be provided once");
                }
                list_only = true;
                index += 1;
            }
            "--peer" => {
                let Some(value) = args.get(index + 1) else {
                    return usage_error("--peer requires a value");
                };
                if peer_id.is_some() {
                    return usage_error("--peer may only be provided once");
                }
                peer_id = Some(value.clone());
                index += 2;
            }
            "help" | "--help" | "-h" => {
                eprintln!("{USAGE}");
                return Ok(0);
            }
            other => return usage_error(&format!("unknown remote-resume option {other}")),
        }
    }

    let store = ResumeStore::for_active_session()?;
    let records: Vec<_> = store
        .records()
        .iter()
        .filter(|record| peer_id.as_deref().is_none_or(|peer| record.peer_id == peer))
        .cloned()
        .collect();

    if records.is_empty() {
        match peer_id.as_deref() {
            Some(peer) => println!("no handed-off remote panes are pending for peer {peer}"),
            None => println!("no handed-off remote panes are pending"),
        }
        return Ok(0);
    }

    if list_only {
        for record in &records {
            let name = record
                .agent
                .as_ref()
                .and_then(|agent| agent.name.clone())
                .unwrap_or_else(|| "shell".to_string());
            let session = record
                .ssh
                .session
                .as_deref()
                .map(|session| format!(" (remote session {session})"))
                .unwrap_or_default();
            let error = record
                .last_error
                .as_deref()
                .map(|err| format!(" — last attempt failed: {err}"))
                .unwrap_or_default();
            println!(
                "{}  {} on {}{}  pane {}  {}{}",
                record.peer_id,
                name,
                record.ssh.target,
                session,
                record.remote_pane_id,
                record.placement.public_tab_id,
                error
            );
        }
        return Ok(0);
    }

    // Group by peer: one interactive managed connection per peer, then the
    // server resumes each of that peer's records through it.
    let mut by_peer: std::collections::BTreeMap<String, Vec<crate::remote_resume::ResumeRecord>> =
        std::collections::BTreeMap::new();
    for record in records {
        by_peer
            .entry(record.peer_id.clone())
            .or_default()
            .push(record);
    }

    let mut failures = 0usize;
    for (peer, peer_records) in by_peer {
        let target = peer_records
            .first()
            .map(|record| record.ssh.target.clone())
            .unwrap_or_default();
        let ssh_args = peer_records
            .first()
            .map(|record| record.ssh.ssh_args.clone())
            .unwrap_or_default();
        eprintln!("resuming {} pane(s) on {target}...", peer_records.len());
        let managed = match crate::ssh_integration::prepare_managed_connection(&target, &ssh_args) {
            Ok(managed) => managed,
            Err(err) => {
                eprintln!("could not authenticate to {target}: {err}");
                failures += peer_records.len();
                continue;
            }
        };
        let managed_control_path = managed.map(|managed| managed.transfer());
        let response = super::send_request(&Request {
            id: "cli:remote:resume".into(),
            method: Method::RemoteResume(RemoteResumeParams {
                peer_id: Some(peer.clone()),
                managed_control_path,
            }),
        })?;
        failures += print_resume_outcomes(&response, &peer);
    }

    if failures > 0 {
        Ok(1)
    } else {
        Ok(0)
    }
}

fn print_resume_outcomes(response: &serde_json::Value, peer: &str) -> usize {
    if let Some(error) = response.get("error") {
        eprintln!("remote resume failed for {peer}: {error}");
        return 1;
    }
    let mut failures = 0usize;
    if let Some(outcomes) = response["result"]["outcomes"].as_array() {
        for outcome in outcomes {
            let terminal = outcome["remote_terminal_id"].as_str().unwrap_or("?");
            match outcome["error"].as_str() {
                Some(message) => {
                    failures += 1;
                    eprintln!("  {terminal}: {message}");
                }
                None => println!("  {terminal}: resumed"),
            }
        }
    }
    failures
}

fn usage_error(message: &str) -> std::io::Result<i32> {
    eprintln!("{message}");
    eprintln!("{USAGE}");
    Ok(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcomes_count_failures() {
        let response = serde_json::json!({
            "result": {
                "type": "remote_resume",
                "outcomes": [
                    { "remote_terminal_id": "term_a", "peer_id": "p", "error": null },
                    { "remote_terminal_id": "term_b", "peer_id": "p", "error": "auth failed" }
                ]
            }
        });
        assert_eq!(print_resume_outcomes(&response, "p"), 1);
    }

    #[test]
    fn error_response_counts_one_failure() {
        let response = serde_json::json!({ "error": { "code": "remote_resume_empty" } });
        assert_eq!(print_resume_outcomes(&response, "p"), 1);
    }
}
