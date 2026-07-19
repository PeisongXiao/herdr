//! Automatic remote-pane handoff on graceful server stop and re-acquire on
//! the next server start, driven by `[remote] auto_remote_handoff`.
//!
//! On stop, every owned remote presentation is handed back to its host with
//! `terminal.delegate.handoff` and a durable resume record is persisted per
//! pane. On start (or on `remote.resume`), the records drive a re-acquire
//! through the usual federation pipeline: capability checks, reverse peer
//! bridge, `terminal.delegate.claim`, and a local pane running the managed
//! SSH attach in the recorded workspace and tab.

use std::path::PathBuf;

use crate::api::schema::{
    self, AgentTransportInfo, PaneTarget, RemoteResumeOutcome, RemoteResumeParams, ResponseResult,
    SplitDirection, SuccessResponse,
};
use crate::events::{AppEvent, RemoteReacquireBatch, RemoteReacquireResult};
use crate::remote_resume::{ResumeAgent, ResumePlacement, ResumeRecord, ResumeSsh, ResumeStore};

use super::agents::remote_transport_agent;
use super::App;

impl App {
    /// Hand every owned remote presentation back to its host, persisting a
    /// resume record per pane so a later server can re-acquire it. Idempotent:
    /// handed-off terminals leave the owner maps, so a second call during
    /// shutdown is a no-op. Returns the number of panes handed off by this
    /// call. Panes that cannot be handed off stay in the owner maps and keep
    /// the destructive stop contract.
    pub(crate) fn auto_handoff_remote_presentations(&mut self) -> usize {
        if !self.auto_remote_handoff || self.remote_owner_presentations.is_empty() {
            return 0;
        }
        let mut store = match ResumeStore::for_active_session() {
            Ok(store) => store,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not open the remote resume store; remote panes keep the destructive stop contract"
                );
                return 0;
            }
        };
        let terminal_ids: Vec<_> = self.remote_owner_presentations.keys().cloned().collect();
        let mut handed_off = 0usize;
        for terminal_id in terminal_ids {
            let Some(delegation) = self.remote_owner_presentations.get(&terminal_id).cloned()
            else {
                continue;
            };
            // Agent starts link through remote_terminal_bridges, shim shells
            // through ssh_shell_bridges, and activation-based presentations
            // through remote_owner_peers.
            let peer_id = self
                .remote_terminal_bridges
                .get(&terminal_id)
                .map(|(peer_id, _)| peer_id.clone())
                .or_else(|| {
                    self.ssh_shell_bridges
                        .get(&terminal_id)
                        .and_then(|bridges| bridges.first().map(|(peer_id, _)| peer_id.clone()))
                })
                .or_else(|| self.remote_owner_peers.get(&terminal_id).cloned());
            let Some(peer_id) = peer_id else {
                tracing::warn!(
                    terminal = %terminal_id,
                    "no peer link found for a remote presentation; pane keeps the destructive stop contract"
                );
                continue;
            };
            let Some(peer) = self.state.peers.get(&peer_id).cloned() else {
                tracing::warn!(
                    terminal = %terminal_id,
                    peer = %peer_id,
                    "peer is not registered; pane keeps the destructive stop contract"
                );
                continue;
            };
            let Some(record) = self.resume_record_for_terminal(&terminal_id, &peer_id, &peer)
            else {
                tracing::warn!(
                    terminal = %terminal_id,
                    peer = %peer_id,
                    "could not build a resume record; pane keeps the destructive stop contract"
                );
                continue;
            };
            // Persist before handing off: if the process dies between the two,
            // the pane survives on its host and the record is the only way
            // back to it.
            if let Err(err) = store.upsert(record.clone()) {
                tracing::warn!(
                    error = %err,
                    terminal = %terminal_id,
                    "could not persist a remote resume record; pane keeps the destructive stop contract"
                );
                let _ = store.remove(&record.remote_terminal_id);
                continue;
            }
            match crate::remote_agent::handoff_delegated_terminal(&peer, &delegation) {
                Ok(()) => {
                    handed_off += 1;
                    tracing::info!(
                        terminal = %terminal_id,
                        peer = %peer_id,
                        "handed off remote pane to its host before server stop"
                    );
                    // The remote pane now lives on its host; the local attach
                    // pane is dead weight. Tear it down like a normal owner
                    // shutdown so no placeholder shell is ever serialized.
                    self.release_remote_terminal_bridge(&terminal_id);
                    self.state.terminals.remove(&terminal_id);
                    if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
                        runtime.shutdown();
                    }
                    let pane_id = self.state.workspaces.iter().find_map(|workspace| {
                        workspace.tabs.iter().find_map(|tab| {
                            tab.panes.iter().find_map(|(pane_id, pane)| {
                                (pane.attached_terminal_id == terminal_id).then_some(*pane_id)
                            })
                        })
                    });
                    if let Some(pane_id) = pane_id {
                        self.handle_internal_event(crate::events::AppEvent::PaneDied { pane_id });
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        terminal = %terminal_id,
                        peer = %peer_id,
                        "remote handoff before server stop failed; pane keeps the destructive stop contract"
                    );
                    let _ = store.remove(&record.remote_terminal_id);
                }
            }
        }
        handed_off
    }

    fn resume_record_for_terminal(
        &self,
        terminal_id: &crate::terminal::TerminalId,
        peer_id: &str,
        peer: &crate::api::schema::PeerInfo,
    ) -> Option<ResumeRecord> {
        let schema::PeerTransportInfo::Ssh {
            target,
            ssh_args,
            session,
            ..
        } = &peer.transport
        else {
            return None;
        };
        let delegation = self.remote_owner_presentations.get(terminal_id)?;
        let transport = self
            .state
            .terminals
            .get(terminal_id)
            .and_then(|terminal| terminal.remote_agent_transport.clone());
        let (remote_agent, remote_cwd) = match &transport {
            Some(AgentTransportInfo::Ssh {
                remote_agent,
                remote_cwd,
                ..
            }) => (remote_agent.clone(), remote_cwd.clone()),
            _ => (None, None),
        };
        let (ws_idx, tab_idx, pane_id) =
            self.state
                .workspaces
                .iter()
                .enumerate()
                .find_map(|(ws_idx, workspace)| {
                    workspace
                        .tabs
                        .iter()
                        .enumerate()
                        .find_map(|(tab_idx, tab)| {
                            tab.panes.iter().find_map(|(pane_id, pane)| {
                                (pane.attached_terminal_id == *terminal_id)
                                    .then_some((ws_idx, tab_idx, *pane_id))
                            })
                        })
                })?;
        let name = self.state.terminals.get(terminal_id).and_then(|terminal| {
            terminal
                .agent_name
                .clone()
                .or(terminal.manual_label.clone())
        });
        Some(ResumeRecord {
            schema: crate::remote_resume::RESUME_SCHEMA_VERSION,
            remote_terminal_id: delegation.terminal_id.clone(),
            remote_pane_id: delegation.pane_id.clone(),
            peer_id: peer_id.to_string(),
            ssh: ResumeSsh {
                target: target.clone(),
                ssh_args: ssh_args.clone(),
                session: session.clone(),
            },
            agent: Some(ResumeAgent {
                name,
                agent: remote_agent,
                cwd: remote_cwd,
            }),
            placement: ResumePlacement {
                workspace_id: self.public_workspace_id(ws_idx),
                public_tab_id: self.public_tab_id(ws_idx, tab_idx)?,
                public_pane_id: self.public_pane_id(ws_idx, pane_id),
                pane_index: None,
            },
            handed_off_at_unix_ms: crate::remote_resume::unix_ms(),
            last_error: None,
        })
    }

    /// Kick off background re-acquire attempts for every stored resume
    /// record. Called once after startup when the setting is enabled. Each
    /// peer's records resume sequentially on one worker thread; results
    /// arrive as `AppEvent::RemoteReacquireFinished`.
    pub(crate) fn spawn_remote_reacquires(&mut self) {
        if !self.auto_remote_handoff {
            return;
        }
        let store = match ResumeStore::for_active_session() {
            Ok(store) => store,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not open the remote resume store; skipping remote pane re-acquire"
                );
                return;
            }
        };
        if store.is_empty() {
            return;
        }
        let mut by_peer: std::collections::BTreeMap<String, Vec<ResumeRecord>> =
            std::collections::BTreeMap::new();
        for record in store.records() {
            by_peer
                .entry(record.peer_id.clone())
                .or_default()
                .push(record.clone());
        }
        let pending: Vec<String> = by_peer.keys().cloned().collect();
        tracing::info!(peers = ?pending, "re-acquiring handed-off remote panes");
        for (peer_id, records) in by_peer {
            self.spawn_remote_reacquire_worker(peer_id, records, None, None);
        }
    }

    /// Deferred `remote.resume` handler: spawn the re-acquire worker and
    /// respond when the batch finishes.
    pub(super) fn begin_remote_resume(
        &mut self,
        id: String,
        params: RemoteResumeParams,
        respond_to: std::sync::mpsc::Sender<String>,
    ) {
        match self.resume_records_for_request(&params) {
            Ok((peer_id, records)) => {
                self.spawn_remote_reacquire_worker(
                    peer_id,
                    records,
                    params.managed_control_path,
                    Some((id, respond_to)),
                );
            }
            Err(response) => {
                let _ = respond_to.send(response);
            }
        }
    }

    /// Synchronous `remote.resume` handler for the in-app (non-deferred)
    /// path. Blocks the app loop on SSH setup, matching the existing
    /// synchronous peer-connect fallback.
    pub(super) fn handle_remote_resume(
        &mut self,
        id: String,
        params: RemoteResumeParams,
    ) -> String {
        let (_peer_id, records) = match self.resume_records_for_request(&params) {
            Ok(resolved) => resolved,
            Err(response) => return response,
        };
        let results = run_reacquire_batch(records, params.managed_control_path);
        let outcomes = self.apply_reacquire_results(results);
        encode_success(id.clone(), ResponseResult::RemoteResume { outcomes }).unwrap_or_else(|| {
            encode_resume_error(
                &id,
                "internal_error",
                "failed to encode remote resume response",
            )
        })
    }

    fn resume_records_for_request(
        &self,
        params: &RemoteResumeParams,
    ) -> Result<(String, Vec<ResumeRecord>), String> {
        let store = ResumeStore::for_active_session().map_err(|err| {
            encode_resume_error(
                "remote_resume",
                "remote_resume_unavailable",
                format!("could not open the remote resume store: {err}"),
            )
        })?;
        let records: Vec<ResumeRecord> = match params.peer_id.as_deref() {
            Some(peer_id) => store
                .records()
                .iter()
                .filter(|record| record.peer_id == peer_id)
                .cloned()
                .collect(),
            None => store.records().to_vec(),
        };
        if records.is_empty() {
            return Err(encode_resume_error(
                "remote_resume",
                "remote_resume_empty",
                match params.peer_id.as_deref() {
                    Some(peer_id) => {
                        format!("no handed-off remote panes are pending for peer {peer_id}")
                    }
                    None => "no handed-off remote panes are pending".to_string(),
                },
            ));
        }
        let peer_id = records
            .first()
            .map(|record| record.peer_id.clone())
            .unwrap_or_default();
        if params.managed_control_path.is_some()
            && records.iter().any(|record| record.peer_id != peer_id)
        {
            return Err(encode_resume_error(
                "remote_resume",
                "invalid_request",
                "a managed control path belongs to one peer; resume one peer at a time with --peer",
            ));
        }
        Ok((peer_id, records))
    }

    fn spawn_remote_reacquire_worker(
        &self,
        peer_id: String,
        records: Vec<ResumeRecord>,
        managed_control_path: Option<String>,
        respond_to: Option<(String, std::sync::mpsc::Sender<String>)>,
    ) {
        let event_tx = self.event_tx.clone();
        let worker_respond_to = respond_to.clone();
        let spawn = std::thread::Builder::new()
            .name("herdr-remote-reacquire".into())
            .spawn(move || {
                let results = run_reacquire_batch(records, managed_control_path);
                let event = AppEvent::RemoteReacquireFinished(Box::new(RemoteReacquireBatch {
                    peer_id,
                    results,
                    respond_to: worker_respond_to,
                }));
                if let Err(err) = event_tx.blocking_send(event) {
                    if let AppEvent::RemoteReacquireFinished(mut batch) = err.0 {
                        for finished in &mut batch.results {
                            if let Ok(remote) = &mut finished.result {
                                crate::remote_agent::rollback_reacquire(remote);
                            }
                        }
                    }
                }
            });
        if let Err(err) = spawn {
            if let Some((id, respond_to)) = respond_to {
                let _ = respond_to.send(encode_resume_error(
                    &id,
                    "internal_error",
                    format!("could not start remote re-acquire: {err}"),
                ));
            }
        }
    }

    /// Apply a finished re-acquire batch on the app thread: place each pane,
    /// restore owner state, prune or annotate the resume records, and answer
    /// a waiting `remote.resume` request.
    pub(super) fn finish_remote_reacquire(&mut self, batch: RemoteReacquireBatch) {
        let RemoteReacquireBatch {
            peer_id,
            results,
            mut respond_to,
        } = batch;
        let outcomes = self.apply_reacquire_results(results);
        if let Some((id, respond_to)) = respond_to.take() {
            let _ = respond_to.send(
                encode_success(id, ResponseResult::RemoteResume { outcomes }).unwrap_or_else(
                    || "{\"jsonrpc\":\"2.0\",\"error\":{\"code\":\"internal_error\"}}".to_string(),
                ),
            );
        } else {
            let resumed = outcomes
                .iter()
                .filter(|outcome| outcome.error.is_none())
                .count();
            tracing::info!(
                peer = %peer_id,
                resumed,
                failed = outcomes.len() - resumed,
                "finished remote pane re-acquire batch"
            );
        }
    }

    fn apply_reacquire_results(
        &mut self,
        results: Vec<RemoteReacquireResult>,
    ) -> Vec<RemoteResumeOutcome> {
        let mut store = ResumeStore::for_active_session().ok();
        let mut outcomes = Vec::with_capacity(results.len());
        for mut finished in results {
            let record = finished.record.clone();
            let outcome = match &mut finished.result {
                Ok(remote) => match self.place_reacquired_pane(&record, remote) {
                    Ok(()) => {
                        if let Some(store) = store.as_mut() {
                            let _ = store.remove(&record.remote_terminal_id);
                        }
                        tracing::info!(
                            terminal = %record.remote_terminal_id,
                            peer = %record.peer_id,
                            "re-acquired handed-off remote pane"
                        );
                        RemoteResumeOutcome {
                            remote_terminal_id: record.remote_terminal_id.clone(),
                            peer_id: record.peer_id.clone(),
                            error: None,
                        }
                    }
                    Err(err) => {
                        crate::remote_agent::rollback_reacquire(remote);
                        if let Some(store) = store.as_mut() {
                            let _ =
                                store.set_last_error(&record.remote_terminal_id, Some(err.clone()));
                        }
                        RemoteResumeOutcome {
                            remote_terminal_id: record.remote_terminal_id.clone(),
                            peer_id: record.peer_id.clone(),
                            error: Some(err),
                        }
                    }
                },
                Err(message) => {
                    if let Some(store) = store.as_mut() {
                        if message.contains("not found") {
                            // The pane is gone on its host; the record can
                            // never resume again.
                            let _ = store.remove(&record.remote_terminal_id);
                            tracing::info!(
                                terminal = %record.remote_terminal_id,
                                peer = %record.peer_id,
                                "pruned a remote resume record whose pane is gone"
                            );
                        } else {
                            let _ = store
                                .set_last_error(&record.remote_terminal_id, Some(message.clone()));
                        }
                    }
                    tracing::warn!(
                        terminal = %record.remote_terminal_id,
                        peer = %record.peer_id,
                        error = %message,
                        "remote pane re-acquire failed"
                    );
                    RemoteResumeOutcome {
                        remote_terminal_id: record.remote_terminal_id.clone(),
                        peer_id: record.peer_id.clone(),
                        error: Some(message.clone()),
                    }
                }
            };
            outcomes.push(outcome);
        }
        outcomes
    }

    /// Spawn the local attach pane for a re-acquired remote pane and restore
    /// the owner-side presentation state, mirroring the tail of a remote
    /// agent start. On success the resume placeholder left by session restore
    /// in the recorded slot is closed.
    fn place_reacquired_pane(
        &mut self,
        record: &ResumeRecord,
        remote: &mut crate::remote_agent::RemoteAgentStart,
    ) -> Result<(), String> {
        let local_cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let (rows, cols) = self.state.estimate_pane_size();
        let attach_argv = remote.attach_argv.clone();
        let spawn_result =
            match self
                .parse_tab_id(&record.placement.public_tab_id)
                .map(|(ws_idx, tab_idx)| {
                    let target_pane = self.state.workspaces[ws_idx].tabs[tab_idx].layout.focused();
                    (ws_idx, target_pane)
                }) {
                Some((ws_idx, target_pane)) => self
                    .spawn_agent_split(
                        ws_idx,
                        target_pane,
                        SplitDirection::Right,
                        local_cwd,
                        &attach_argv,
                        Vec::new(),
                        false,
                    )
                    .map_err(|err| format!("{err:?}")),
                None if self.state.workspaces.is_empty() => self
                    .spawn_agent_workspace(local_cwd, rows, cols, &attach_argv, Vec::new(), false)
                    .map_err(|err| format!("{err:?}")),
                None => {
                    let ws_idx = self.state.active.unwrap_or(0);
                    let tab_idx = self.state.workspaces[ws_idx].active_tab;
                    let target_pane = self.state.workspaces[ws_idx].tabs[tab_idx].layout.focused();
                    self.spawn_agent_split(
                        ws_idx,
                        target_pane,
                        SplitDirection::Right,
                        local_cwd,
                        &attach_argv,
                        Vec::new(),
                        false,
                    )
                    .map_err(|err| format!("{err:?}"))
                }
            };
        let (ws_idx, _tab_idx, pane_id) = spawn_result?;
        let terminal_id = self
            .state
            .workspaces
            .get(ws_idx)
            .and_then(|ws| ws.terminal_id(pane_id))
            .cloned()
            .ok_or_else(|| "terminal disappeared after spawn".to_string())?;
        let terminal = self
            .state
            .terminals
            .get_mut(&terminal_id)
            .ok_or_else(|| "terminal disappeared after spawn".to_string())?;
        if let Some(name) = record.agent.as_ref().and_then(|agent| agent.name.clone()) {
            terminal.set_agent_name(name.clone());
            terminal.set_manual_label(name);
        }
        terminal.remote_agent_transport = Some(remote.transport.clone());
        self.remote_owner_presentations
            .insert(terminal_id.clone(), remote.delegation.clone());
        if let Some(peer) = remote.peer.clone() {
            if let Some(bridge) = remote.bridge.take() {
                let connection_id = self
                    .peer_bridges
                    .entry(peer.id.clone())
                    .or_default()
                    .push(bridge);
                self.remote_terminal_bridges
                    .insert(terminal_id.clone(), (peer.id.clone(), connection_id));
            }
            self.state.peers.insert(peer.id.clone(), peer.clone());
            self.start_peer_refresh(peer);
        }
        self.state.mark_session_dirty();

        for event in crate::remote_agent::events_from_agent_info(
            pane_id,
            &remote.agent,
            remote_transport_agent(&remote.transport),
        ) {
            self.handle_internal_event(event);
        }
        let mirror_cancel = crate::remote_agent::spawn_mirror(
            remote.transport.clone(),
            pane_id,
            self.event_tx.clone(),
        );
        self.remote_mirror_cancellations
            .insert(terminal_id.to_string(), mirror_cancel);

        self.close_reacquire_placeholder(record);
        Ok(())
    }

    /// Close the placeholder shell that session restore left in the recorded
    /// slot. Only closes the pane when it still looks like an untouched
    /// placeholder (no agent, no launch argv, no remote transport), never a
    /// pane the user has since reused.
    fn close_reacquire_placeholder(&mut self, record: &ResumeRecord) {
        let Some(public_pane_id) = record.placement.public_pane_id.clone() else {
            return;
        };
        let Some(pane_id) = self
            .state
            .workspaces
            .iter()
            .enumerate()
            .flat_map(|(ws_idx, workspace)| {
                workspace
                    .tabs
                    .iter()
                    .flat_map(|tab| tab.panes.keys().copied().collect::<Vec<_>>())
                    .map(move |pane_id| (ws_idx, pane_id))
            })
            .find(|(ws_idx, pane_id)| {
                self.public_pane_id(*ws_idx, *pane_id).as_deref() == Some(public_pane_id.as_str())
            })
            .map(|(_, pane_id)| pane_id)
        else {
            return;
        };
        let is_placeholder = self
            .find_pane(pane_id)
            .and_then(|(_, pane)| {
                self.state
                    .terminals
                    .get(&pane.attached_terminal_id)
                    .map(|terminal| {
                        terminal.remote_agent_transport.is_none()
                            && !terminal.is_agent_terminal()
                            && terminal.launch_argv.is_none()
                    })
            })
            .unwrap_or(false);
        if !is_placeholder {
            return;
        }
        let _ = self.close_pane(
            "remote-resume".to_string(),
            &PaneTarget {
                pane_id: public_pane_id,
            },
        );
    }
}

fn run_reacquire_batch(
    records: Vec<ResumeRecord>,
    managed_control_path: Option<String>,
) -> Vec<RemoteReacquireResult> {
    records
        .into_iter()
        .map(|record| {
            let result = crate::remote_agent::reacquire(&record, managed_control_path.clone())
                .map_err(|err| err.to_string());
            RemoteReacquireResult { record, result }
        })
        .collect()
}

fn encode_success(id: String, result: ResponseResult) -> Option<String> {
    serde_json::to_string(&SuccessResponse { id, result }).ok()
}

fn encode_resume_error(id: &str, code: &str, message: impl Into<String>) -> String {
    let response = schema::ErrorResponse {
        id: id.to_string(),
        error: schema::ErrorBody {
            code: code.to_string(),
            message: message.into(),
        },
    };
    serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_string())
}
