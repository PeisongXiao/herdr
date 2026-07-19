//! Automatic remote-pane handoff on graceful server stop and re-acquire on
//! the next server start, driven by `[remote] auto_remote_handoff`.
//!
//! On stop, every owned remote presentation is handed back to its host with
//! `terminal.delegate.handoff` and a durable resume record is persisted per
//! pane. On start (or on `remote.resume`), the records drive a re-acquire
//! through the usual federation pipeline: capability checks, reverse peer
//! bridge, `terminal.delegate.claim`, and a local pane running the managed
//! SSH attach in the recorded workspace and tab.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::schema::{
    self, AgentTransportInfo, PaneTarget, RemoteResumeOutcome, RemoteResumeParams, ResponseResult,
    SuccessResponse,
};
use crate::events::{
    AppEvent, RemoteOrphanInventoryResult, RemoteOrphanResolveOutcome, RemoteOrphanResolveResult,
    RemoteParkedTerminateResult, RemoteReacquireBatch, RemoteReacquireFailure,
    RemoteReacquireResult,
};
use crate::remote_resume::{
    RecoveryIdentityStore, ResumeAgent, ResumeLifecycle, ResumePlacement, ResumeRecord, ResumeSsh,
    ResumeStore, SecretToken,
};

use super::agents::remote_transport_agent;
use super::App;

const MAX_CONCURRENT_REMOTE_RESTORES: usize = 8;
const REMOTE_RESTORE_TIMEOUT: Duration = Duration::from_secs(120);
const REMOTE_RESTORE_INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const REMOTE_RESTORE_MAX_BACKOFF: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteRestorePanelAction {
    Retry,
    Close,
}

pub(crate) struct RemoteRestoreWorker {
    generation: u64,
    pane_id: crate::layout::PaneId,
    cancelled: Arc<AtomicBool>,
}

pub(crate) struct QueuedRemoteRestore {
    record: ResumeRecord,
    pane_id: crate::layout::PaneId,
    generation: u64,
    deadline: Instant,
    managed_control_path: Option<String>,
    request_token: Option<u64>,
    credentials: RemoteRestoreCredentials,
}

#[derive(Clone)]
enum RemoteRestoreCredentials {
    Parked {
        park_id: String,
        origin_id: String,
        resume_token: String,
    },
    LegacyVisibleHandoff,
}

struct ParkedTerminateCredentials {
    park_id: String,
    origin_id: String,
    discovery_token: String,
}

pub(crate) struct PendingRemoteResumeRequest {
    id: String,
    respond_to: std::sync::mpsc::Sender<String>,
    remaining: HashSet<String>,
    outcomes: Vec<RemoteResumeOutcome>,
}

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
            let park_id = match crate::remote_resume::generate_recovery_id() {
                Ok(park_id) => park_id,
                Err(err) => {
                    tracing::warn!(error = %err, terminal = %terminal_id, "could not generate a remote parking id");
                    continue;
                }
            };
            let resume_token = match SecretToken::generate() {
                Ok(token) => token,
                Err(err) => {
                    tracing::warn!(error = %err, terminal = %terminal_id, "could not generate a remote resume token");
                    continue;
                }
            };
            let mut identity = match RecoveryIdentityStore::open_global() {
                Ok(identity) => identity,
                Err(err) => {
                    tracing::warn!(error = %err, terminal = %terminal_id, "could not open remote recovery identity");
                    continue;
                }
            };
            let credentials = match identity.credentials_for_peer(&peer_id) {
                Ok(credentials) => credentials,
                Err(err) => {
                    tracing::warn!(error = %err, terminal = %terminal_id, "could not persist remote discovery credentials");
                    continue;
                }
            };
            // Persist the capability before sending the park request. A lost
            // reply leaves ParkingPending so startup can reconcile the typed
            // remote status instead of deleting the only recovery path.
            if let Err(err) =
                store.upsert_parking(record.clone(), park_id.clone(), resume_token.clone())
            {
                tracing::warn!(
                    error = %err,
                    terminal = %terminal_id,
                    "could not persist a remote resume record; pane keeps the destructive stop contract"
                );
                continue;
            }
            match crate::remote_agent::park_delegated_terminal(
                &peer,
                &delegation,
                &park_id,
                &credentials.origin_id,
                resume_token.expose_secret(),
                credentials.discovery_token.expose_secret(),
            ) {
                Ok(_) => {
                    handed_off += 1;
                    tracing::info!(
                        terminal = %terminal_id,
                        peer = %peer_id,
                        "handed off remote pane to its host before server stop"
                    );
                    // Preserve this exact layout slot as a no-PTY reservation.
                    // Session restore recognizes the explicit marker and does
                    // not infer reservation ownership from shell metadata.
                    self.release_remote_terminal_bridge(&terminal_id);
                    if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
                        runtime.shutdown();
                    }
                    if let Some(pane) = self
                        .state
                        .workspaces
                        .iter_mut()
                        .flat_map(|workspace| workspace.tabs.iter_mut())
                        .flat_map(|tab| tab.panes.values_mut())
                        .find(|pane| pane.attached_terminal_id == terminal_id)
                    {
                        pane.remote_restore_reservation = true;
                    }
                    if let Err(err) = store.mark_parked(&record.remote_terminal_id) {
                        tracing::warn!(
                            terminal = %record.remote_terminal_id,
                            error = %err,
                            "remote terminal is parked but its durable lifecycle is still pending"
                        );
                    }
                    self.state.mark_session_dirty();
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        terminal = %terminal_id,
                        peer = %peer_id,
                        "remote park reply was not confirmed; preserving its pending recovery ticket"
                    );
                    // The request may have committed remotely even when its
                    // response was lost. Reserve the slot and retain the
                    // ParkingPending record for startup reconciliation.
                    handed_off += 1;
                    self.release_remote_terminal_bridge(&terminal_id);
                    if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
                        runtime.shutdown();
                    }
                    if let Some(pane) = self
                        .state
                        .workspaces
                        .iter_mut()
                        .flat_map(|workspace| workspace.tabs.iter_mut())
                        .flat_map(|tab| tab.panes.values_mut())
                        .find(|pane| pane.attached_terminal_id == terminal_id)
                    {
                        pane.remote_restore_reservation = true;
                    }
                    self.state.mark_session_dirty();
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

    /// Kick off one independently timed restore job per persisted terminal.
    /// Admission is shared with other remote API work and capped at eight;
    /// every terminal receives its own enqueue-stamped 120-second deadline.
    pub(crate) fn spawn_remote_reacquires(&mut self) {
        if !self.auto_remote_handoff {
            return;
        }
        let mut store = match ResumeStore::for_active_session() {
            Ok(store) => store,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "could not open the remote resume store; skipping remote pane re-acquire"
                );
                return;
            }
        };
        let records = store.records().to_vec();
        let mut claimed_reservations = HashSet::new();
        for record in records {
            let lifecycle = store
                .recovery_state(&record.remote_terminal_id)
                .map(|state| state.lifecycle);
            if lifecycle == Some(ResumeLifecycle::TerminationPending) {
                self.close_persisted_restore_reservation(&record);
                match parked_terminate_credentials(&store, &record) {
                    Ok(Some(credentials)) => {
                        self.spawn_best_effort_parked_terminate(record, credentials)
                    }
                    Ok(None) => {
                        let _ = store.remove(&record.remote_terminal_id);
                    }
                    Err(err) => tracing::warn!(
                        terminal = %record.remote_terminal_id,
                        error = %err,
                        "could not resume pending parked-terminal termination"
                    ),
                }
                continue;
            }
            if lifecycle == Some(ResumeLifecycle::PlacedCleanupPending) {
                if self.record_has_explicit_reservation(&record) {
                    if let Err(err) = store.mark_parked(&record.remote_terminal_id) {
                        tracing::warn!(
                            terminal = %record.remote_terminal_id,
                            error = %err,
                            "could not return interrupted placement to parked state"
                        );
                        continue;
                    }
                } else {
                    if let Err(err) = store.remove(&record.remote_terminal_id) {
                        tracing::warn!(
                            terminal = %record.remote_terminal_id,
                            error = %err,
                            "could not finish remote restore record cleanup"
                        );
                    }
                    continue;
                }
            }
            if let Err(err) = self.enqueue_remote_restore(record.clone(), None, None) {
                tracing::warn!(
                    terminal = %record.remote_terminal_id,
                    peer = %record.peer_id,
                    error = %err,
                    "could not queue remote terminal restoration"
                );
            } else if let Some(pane_id) = self.panel_pane_for_record(&record) {
                claimed_reservations.insert(pane_id);
            }
        }
        self.reconcile_unclaimed_restore_reservations(&claimed_reservations);
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
            Ok(records) => {
                let token = self.next_remote_resume_request_token;
                self.next_remote_resume_request_token =
                    self.next_remote_resume_request_token.wrapping_add(1).max(1);
                let remaining = records
                    .iter()
                    .map(|record| record.remote_terminal_id.clone())
                    .collect();
                self.pending_remote_resume_requests.insert(
                    token,
                    PendingRemoteResumeRequest {
                        id,
                        respond_to,
                        remaining,
                        outcomes: Vec::new(),
                    },
                );
                for record in records {
                    if let Err(error) = self.enqueue_remote_restore(
                        record.clone(),
                        params.managed_control_path.clone(),
                        Some(token),
                    ) {
                        self.record_remote_resume_outcome(
                            token,
                            RemoteResumeOutcome {
                                remote_terminal_id: record.remote_terminal_id,
                                peer_id: record.peer_id,
                                error: Some(error),
                            },
                        );
                    }
                }
                self.finish_remote_resume_request_if_ready(token);
            }
            Err(error) => {
                let _ = respond_to.send(encode_resume_error_body(&id, error));
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
        let records = match self.resume_records_for_request(&params) {
            Ok(resolved) => resolved,
            Err(error) => return encode_resume_error_body(&id, error),
        };
        let mut ready = Vec::new();
        let mut outcomes = Vec::new();
        for record in records {
            match self.ensure_remote_restore_panel(&record) {
                Ok(_) => ready.push(record),
                Err(error) => outcomes.push(RemoteResumeOutcome {
                    remote_terminal_id: record.remote_terminal_id,
                    peer_id: record.peer_id,
                    error: Some(error),
                }),
            }
        }
        let results = run_reacquire_batch(ready, params.managed_control_path);
        outcomes.extend(
            self.apply_reacquire_results(results)
                .into_iter()
                .map(|(_, outcome)| outcome),
        );
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
    ) -> Result<Vec<ResumeRecord>, schema::ErrorBody> {
        let store = ResumeStore::for_active_session().map_err(|err| schema::ErrorBody {
            code: "remote_resume_unavailable".into(),
            message: format!("could not open the remote resume store: {err}"),
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
            return Err(schema::ErrorBody {
                code: "remote_resume_empty".into(),
                message: match params.peer_id.as_deref() {
                    Some(peer_id) => {
                        format!("no handed-off remote panes are pending for peer {peer_id}")
                    }
                    None => "no handed-off remote panes are pending".to_string(),
                },
            });
        }
        let peer_id = records
            .first()
            .map(|record| record.peer_id.clone())
            .unwrap_or_default();
        if params.managed_control_path.is_some()
            && records.iter().any(|record| record.peer_id != peer_id)
        {
            return Err(schema::ErrorBody {
                code: "invalid_request".into(),
                message: "a managed control path belongs to one peer; resume one peer at a time with --peer".into(),
            });
        }
        Ok(records)
    }

    fn enqueue_remote_restore(
        &mut self,
        record: ResumeRecord,
        managed_control_path: Option<String>,
        request_token: Option<u64>,
    ) -> Result<(), String> {
        if self
            .remote_restore_workers
            .contains_key(&record.remote_terminal_id)
            || self
                .remote_restore_queue
                .iter()
                .any(|queued| queued.record.remote_terminal_id == record.remote_terminal_id)
        {
            return Err("remote terminal restoration is already running".into());
        }
        let credentials = load_remote_restore_credentials(&record)?;
        if !self.record_has_explicit_reservation(&record) {
            self.migrate_legacy_restore_reservation(&record)?;
        }
        let pane_id = self.ensure_remote_restore_panel(&record)?;
        let generation = self.next_remote_restore_generation;
        self.next_remote_restore_generation =
            self.next_remote_restore_generation.wrapping_add(1).max(1);
        if let Some(panel) = self.state.remote_restore_panels.get_mut(&pane_id) {
            panel.status = crate::app::state::RemoteRestoreStatus::Restoring;
            panel.generation = generation;
            panel.timeout_notified = false;
        }
        self.remote_restore_queue.push_back(QueuedRemoteRestore {
            record,
            pane_id,
            generation,
            deadline: remote_restore_deadline(Instant::now()),
            managed_control_path,
            request_token,
            credentials,
        });
        self.pump_remote_restore_queue();
        Ok(())
    }

    fn ensure_remote_restore_panel(
        &mut self,
        record: &ResumeRecord,
    ) -> Result<crate::layout::PaneId, String> {
        let (ws_idx, tab_idx, pane_id) = self.exact_reservation_target(record)?;
        let pane = self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .get(&pane_id)
            .ok_or_else(|| "saved restore pane no longer exists".to_string())?;
        if !pane.remote_restore_reservation {
            return Err("saved pane is not a remote restore reservation".into());
        }
        self.state
            .remote_restore_panels
            .entry(pane_id)
            .or_insert_with(|| crate::app::state::RemoteRestorePanelState {
                remote_terminal_id: record.remote_terminal_id.clone(),
                peer_id: record.peer_id.clone(),
                status: crate::app::state::RemoteRestoreStatus::Restoring,
                generation: 0,
                timeout_notified: false,
            });
        Ok(pane_id)
    }

    fn migrate_legacy_restore_reservation(&mut self, record: &ResumeRecord) -> Result<(), String> {
        let (ws_idx, tab_idx, pane_id) = self.exact_reservation_target(record)?;
        let pane = self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .get(&pane_id)
            .ok_or_else(|| "legacy restore pane no longer exists".to_string())?;
        if pane.remote_restore_reservation {
            return Ok(());
        }
        let terminal_id = pane.attached_terminal_id.clone();
        let safe_legacy_shell = self
            .state
            .terminals
            .get(&terminal_id)
            .is_some_and(|terminal| {
                terminal.remote_agent_transport.is_none()
                    && terminal.launch_argv.is_none()
                    && !terminal.is_agent_terminal()
            });
        if !safe_legacy_shell {
            return Err(
                "legacy restore slot has been reused and cannot be converted safely".into(),
            );
        }
        if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
            runtime.shutdown();
        }
        self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .get_mut(&pane_id)
            .expect("legacy pane was validated before conversion")
            .remote_restore_reservation = true;
        self.state.mark_session_dirty();
        Ok(())
    }

    fn exact_reservation_target(
        &self,
        record: &ResumeRecord,
    ) -> Result<(usize, usize, crate::layout::PaneId), String> {
        let public_pane_id = record
            .placement
            .public_pane_id
            .as_deref()
            .ok_or_else(|| "resume record has no exact pane placement".to_string())?;
        let (ws_idx, tab_idx) = self
            .parse_tab_id(&record.placement.public_tab_id)
            .ok_or_else(|| "saved workspace or tab no longer exists".to_string())?;
        let pane_id = self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .keys()
            .copied()
            .find(|pane_id| {
                self.public_pane_id(ws_idx, *pane_id).as_deref() == Some(public_pane_id)
            })
            .ok_or_else(|| "saved pane no longer exists in its recorded tab".to_string())?;
        Ok((ws_idx, tab_idx, pane_id))
    }

    fn pump_remote_restore_queue(&mut self) {
        while self.remote_api_jobs_in_flight < MAX_CONCURRENT_REMOTE_RESTORES {
            let Some(queued) = self.remote_restore_queue.pop_front() else {
                break;
            };
            let cancelled = Arc::new(AtomicBool::new(false));
            let remote_terminal_id = queued.record.remote_terminal_id.clone();
            let peer_id = queued.record.peer_id.clone();
            let failure_peer_id = peer_id.clone();
            let failure_pane_id = queued.pane_id;
            let failure_request_token = queued.request_token;
            let generation = queued.generation;
            self.remote_api_jobs_in_flight += 1;
            self.remote_restore_workers.insert(
                remote_terminal_id.clone(),
                RemoteRestoreWorker {
                    generation,
                    pane_id: queued.pane_id,
                    cancelled: cancelled.clone(),
                },
            );
            let event_tx = self.event_tx.clone();
            let spawn = std::thread::Builder::new()
                .name("herdr-remote-restore".into())
                .spawn(move || {
                    let finished = run_remote_restore_worker(
                        queued.record,
                        queued.managed_control_path,
                        queued.deadline,
                        generation,
                        queued.request_token,
                        cancelled,
                        queued.credentials,
                    );
                    let event = AppEvent::RemoteReacquireFinished(Box::new(RemoteReacquireBatch {
                        peer_id,
                        results: vec![finished],
                        respond_to: None,
                    }));
                    if let Err(err) = event_tx.blocking_send(event) {
                        if let AppEvent::RemoteReacquireFinished(mut batch) = err.0 {
                            if let Some(Ok(remote)) =
                                batch.results.first_mut().map(|result| &mut result.result)
                            {
                                crate::remote_agent::rollback_reacquire(remote);
                            }
                        }
                    }
                });
            if let Err(err) = spawn {
                self.finish_remote_api_job();
                self.remote_restore_workers.remove(&remote_terminal_id);
                if let Some(panel) = self.state.remote_restore_panels.get_mut(&failure_pane_id) {
                    panel.status = crate::app::state::RemoteRestoreStatus::Retryable {
                        message: format!("could not start remote restore worker: {err}"),
                    };
                }
                if let Some(token) = failure_request_token {
                    self.record_remote_resume_outcome(
                        token,
                        RemoteResumeOutcome {
                            remote_terminal_id,
                            peer_id: failure_peer_id,
                            error: Some(format!("could not start remote restore worker: {err}")),
                        },
                    );
                    self.finish_remote_resume_request_if_ready(token);
                }
            }
        }
    }

    /// Expire queued restores independently of worker admission. This keeps a
    /// ninth (or later) unreachable terminal within the same per-terminal
    /// 120-second wall-clock contract as the first eight.
    pub(crate) fn expire_remote_restore_queue(&mut self, now: Instant) -> bool {
        let mut retained = std::collections::VecDeque::new();
        let mut expired = Vec::new();
        while let Some(queued) = self.remote_restore_queue.pop_front() {
            if now >= queued.deadline {
                expired.push(queued);
            } else {
                retained.push_back(queued);
            }
        }
        self.remote_restore_queue = retained;
        if expired.is_empty() {
            return false;
        }

        let message = "remote restoration did not complete within 120 seconds".to_string();
        let mut store = ResumeStore::for_active_session().ok();
        for queued in expired {
            if let Some(panel) = self.state.remote_restore_panels.get_mut(&queued.pane_id) {
                if panel.generation == queued.generation {
                    panel.status = crate::app::state::RemoteRestoreStatus::TimedOut {
                        message: message.clone(),
                    };
                }
            }
            if let Some(store) = store.as_mut() {
                let _ =
                    store.set_last_error(&queued.record.remote_terminal_id, Some(message.clone()));
            }
            self.notify_remote_restore_timeout(queued.pane_id, &queued.record);
            if let Some(token) = queued.request_token {
                self.record_remote_resume_outcome(
                    token,
                    RemoteResumeOutcome {
                        remote_terminal_id: queued.record.remote_terminal_id.clone(),
                        peer_id: queued.record.peer_id.clone(),
                        error: Some(message.clone()),
                    },
                );
                self.finish_remote_resume_request_if_ready(token);
            }
        }
        true
    }

    /// Apply a finished re-acquire batch on the app thread: place each pane,
    /// restore owner state, prune or annotate the resume records, and answer
    /// a waiting `remote.resume` request.
    pub(super) fn finish_remote_reacquire(&mut self, batch: RemoteReacquireBatch) {
        let RemoteReacquireBatch {
            peer_id,
            results,
            respond_to: _,
        } = batch;
        self.finish_remote_api_job();
        let outcomes = self.apply_reacquire_results(results);
        let resumed = outcomes
            .iter()
            .filter(|(_, outcome)| outcome.error.is_none())
            .count();
        for (request_token, outcome) in outcomes {
            if let Some(token) = request_token {
                self.record_remote_resume_outcome(token, outcome);
                self.finish_remote_resume_request_if_ready(token);
            }
        }
        tracing::info!(peer = %peer_id, resumed, "finished remote terminal restore attempt");
        self.pump_remote_restore_queue();
    }

    fn apply_reacquire_results(
        &mut self,
        results: Vec<RemoteReacquireResult>,
    ) -> Vec<(Option<u64>, RemoteResumeOutcome)> {
        let mut store = ResumeStore::for_active_session().ok();
        let mut outcomes = Vec::with_capacity(results.len());
        for mut finished in results {
            let record = finished.record.clone();
            let request_token = finished.request_token;
            let current = finished.generation == 0
                || self
                    .remote_restore_workers
                    .get(&record.remote_terminal_id)
                    .is_some_and(|worker| {
                        worker.generation == finished.generation
                            && self
                                .state
                                .remote_restore_panels
                                .get(&worker.pane_id)
                                .is_some_and(|panel| panel.generation == finished.generation)
                    });
            self.remote_restore_workers
                .remove(&record.remote_terminal_id);
            if !current {
                if let Ok(remote) = &mut finished.result {
                    crate::remote_agent::rollback_reacquire(remote);
                }
                outcomes.push((
                    request_token,
                    RemoteResumeOutcome {
                        remote_terminal_id: record.remote_terminal_id,
                        peer_id: record.peer_id,
                        error: Some("remote restoration was cancelled".into()),
                    },
                ));
                continue;
            }
            let outcome = match &mut finished.result {
                Ok(remote) => match self.place_reacquired_pane(&record, remote) {
                    Ok(()) => {
                        if let Some(store) = store.as_mut() {
                            if let Err(err) =
                                store.mark_placed_cleanup_pending(&record.remote_terminal_id)
                            {
                                tracing::warn!(
                                    terminal = %record.remote_terminal_id,
                                    error = %err,
                                    "remote terminal restored but record cleanup is pending"
                                );
                            }
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
                        if let Some(panel) = self
                            .panel_pane_for_record(&record)
                            .and_then(|pane_id| self.state.remote_restore_panels.get_mut(&pane_id))
                        {
                            panel.status = crate::app::state::RemoteRestoreStatus::Retryable {
                                message: err.clone(),
                            };
                        }
                        RemoteResumeOutcome {
                            remote_terminal_id: record.remote_terminal_id.clone(),
                            peer_id: record.peer_id.clone(),
                            error: Some(err),
                        }
                    }
                },
                Err(RemoteReacquireFailure::Cancelled) => RemoteResumeOutcome {
                    remote_terminal_id: record.remote_terminal_id.clone(),
                    peer_id: record.peer_id.clone(),
                    error: Some("remote restoration was cancelled".into()),
                },
                Err(RemoteReacquireFailure::TimedOut { message }) => {
                    if let Some(store) = store.as_mut() {
                        let _ =
                            store.set_last_error(&record.remote_terminal_id, Some(message.clone()));
                    }
                    if let Some(pane_id) = self.panel_pane_for_record(&record) {
                        if let Some(panel) = self.state.remote_restore_panels.get_mut(&pane_id) {
                            panel.status = crate::app::state::RemoteRestoreStatus::TimedOut {
                                message: message.clone(),
                            };
                        }
                        self.notify_remote_restore_timeout(pane_id, &record);
                    }
                    RemoteResumeOutcome {
                        remote_terminal_id: record.remote_terminal_id.clone(),
                        peer_id: record.peer_id.clone(),
                        error: Some(message.clone()),
                    }
                }
                Err(RemoteReacquireFailure::Retryable { message }) => {
                    if let Some(store) = store.as_mut() {
                        let _ =
                            store.set_last_error(&record.remote_terminal_id, Some(message.clone()));
                    }
                    if let Some(panel) = self
                        .panel_pane_for_record(&record)
                        .and_then(|pane_id| self.state.remote_restore_panels.get_mut(&pane_id))
                    {
                        panel.status = crate::app::state::RemoteRestoreStatus::Retryable {
                            message: message.clone(),
                        };
                    }
                    RemoteResumeOutcome {
                        remote_terminal_id: record.remote_terminal_id.clone(),
                        peer_id: record.peer_id.clone(),
                        error: Some(message.clone()),
                    }
                }
                Err(RemoteReacquireFailure::Ended { message }) => {
                    if let Some(store) = store.as_mut() {
                        let _ =
                            store.set_last_error(&record.remote_terminal_id, Some(message.clone()));
                    }
                    if let Some(panel) = self
                        .panel_pane_for_record(&record)
                        .and_then(|pane_id| self.state.remote_restore_panels.get_mut(&pane_id))
                    {
                        panel.status = crate::app::state::RemoteRestoreStatus::Ended {
                            message: message.clone(),
                        };
                    }
                    RemoteResumeOutcome {
                        remote_terminal_id: record.remote_terminal_id.clone(),
                        peer_id: record.peer_id.clone(),
                        error: Some(message.clone()),
                    }
                }
            };
            outcomes.push((request_token, outcome));
        }
        outcomes
    }

    /// Attach the remote runtime directly to the explicit reservation. This
    /// never creates a split, workspace, or active-tab fallback.
    fn place_reacquired_pane(
        &mut self,
        record: &ResumeRecord,
        remote: &mut crate::remote_agent::RemoteAgentStart,
    ) -> Result<(), String> {
        let (ws_idx, tab_idx, pane_id) = self.exact_reservation_target(record)?;
        if !self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .get(&pane_id)
            .is_some_and(|pane| pane.remote_restore_reservation)
        {
            return Err("saved pane is no longer a remote restore reservation".into());
        }
        let local_cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let (rows, cols) = self.state.estimate_pane_size();
        let attach_argv = remote.attach_argv.clone();
        let launch_env = crate::pane::PaneLaunchEnv::from_extra(Vec::new()).with_identity(
            self.public_workspace_id(ws_idx),
            record.placement.public_tab_id.clone(),
            record
                .placement
                .public_pane_id
                .clone()
                .ok_or_else(|| "resume record has no public pane id".to_string())?,
        );
        let runtime = crate::terminal::TerminalRuntime::spawn_argv_command(
            pane_id,
            rows,
            cols,
            local_cwd.clone(),
            &attach_argv,
            &launch_env,
            self.state.pane_scrollback_limit_bytes,
            self.state.host_terminal_theme,
            self.event_tx.clone(),
            self.render_notify.clone(),
            self.render_dirty.clone(),
        )
        .map_err(|err| format!("could not start remote attach runtime: {err}"))?;
        let old_terminal_id = self.state.workspaces[ws_idx].tabs[tab_idx].panes[&pane_id]
            .attached_terminal_id
            .clone();
        let terminal_id = crate::terminal::TerminalId::alloc();
        let mut terminal = crate::terminal::TerminalState::new(terminal_id.clone(), local_cwd)
            .with_launch_argv(attach_argv);
        if let Some(name) = record.agent.as_ref().and_then(|agent| agent.name.clone()) {
            terminal.set_agent_name(name.clone());
            terminal.set_manual_label(name);
        }
        terminal.remote_agent_transport = Some(remote.transport.clone());
        self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .get_mut(&pane_id)
            .expect("reservation was validated before runtime spawn")
            .attached_terminal_id = terminal_id.clone();
        self.state.workspaces[ws_idx].tabs[tab_idx]
            .panes
            .get_mut(&pane_id)
            .expect("reservation was validated before runtime spawn")
            .remote_restore_reservation = false;
        self.state.terminals.remove(&old_terminal_id);
        self.state.terminals.insert(terminal_id.clone(), terminal);
        self.terminal_runtimes.insert(terminal_id.clone(), runtime);
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
        self.state.remote_restore_panels.remove(&pane_id);
        self.schedule_session_save();
        Ok(())
    }

    fn panel_pane_for_record(&self, record: &ResumeRecord) -> Option<crate::layout::PaneId> {
        self.state
            .remote_restore_panels
            .iter()
            .find_map(|(pane_id, panel)| {
                (panel.remote_terminal_id == record.remote_terminal_id).then_some(*pane_id)
            })
    }

    fn record_has_explicit_reservation(&self, record: &ResumeRecord) -> bool {
        self.exact_reservation_target(record)
            .ok()
            .and_then(|(ws_idx, tab_idx, pane_id)| {
                self.state.workspaces[ws_idx].tabs[tab_idx]
                    .panes
                    .get(&pane_id)
            })
            .is_some_and(|pane| pane.remote_restore_reservation)
    }

    fn set_remote_restore_reservation(
        &mut self,
        pane_id: crate::layout::PaneId,
        reserved: bool,
    ) -> bool {
        for workspace in &mut self.state.workspaces {
            for tab in &mut workspace.tabs {
                if let Some(pane) = tab.panes.get_mut(&pane_id) {
                    pane.remote_restore_reservation = reserved;
                    return true;
                }
            }
        }
        false
    }

    fn close_persisted_restore_reservation(&mut self, record: &ResumeRecord) {
        let Ok((ws_idx, _, pane_id)) = self.exact_reservation_target(record) else {
            return;
        };
        if !self
            .find_pane(pane_id)
            .is_some_and(|(_, pane)| pane.remote_restore_reservation)
        {
            return;
        }
        if let Some(public_pane_id) = self.public_pane_id(ws_idx, pane_id) {
            let _ = self.close_pane(
                "remote-restore-termination-pending".into(),
                &PaneTarget {
                    pane_id: public_pane_id,
                },
            );
        }
    }

    fn reconcile_unclaimed_restore_reservations(
        &mut self,
        claimed: &HashSet<crate::layout::PaneId>,
    ) {
        let stale = self
            .state
            .workspaces
            .iter()
            .flat_map(|workspace| workspace.tabs.iter())
            .flat_map(|tab| tab.panes.iter())
            .filter_map(|(pane_id, pane)| {
                (pane.remote_restore_reservation && !claimed.contains(pane_id)).then_some(*pane_id)
            })
            .collect::<Vec<_>>();
        for pane_id in stale {
            let terminal_id = self
                .find_pane(pane_id)
                .map(|(_, pane)| pane.attached_terminal_id.clone());
            self.set_remote_restore_reservation(pane_id, false);
            if self.respawn_shell_for_launch_pane(pane_id) {
                self.state.mark_session_dirty();
                self.schedule_session_save();
                continue;
            }
            self.set_remote_restore_reservation(pane_id, true);
            self.state.remote_restore_panels.insert(
                pane_id,
                crate::app::state::RemoteRestorePanelState {
                    remote_terminal_id: terminal_id
                        .map(|terminal_id| terminal_id.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                    peer_id: "unknown".into(),
                    status: crate::app::state::RemoteRestoreStatus::Ended {
                        message: "the durable remote recovery record is unavailable".into(),
                    },
                    generation: 0,
                    timeout_notified: false,
                },
            );
        }
    }

    fn notify_remote_restore_timeout(
        &mut self,
        pane_id: crate::layout::PaneId,
        record: &ResumeRecord,
    ) {
        let Some(panel) = self.state.remote_restore_panels.get_mut(&pane_id) else {
            return;
        };
        if panel.timeout_notified {
            return;
        }
        panel.timeout_notified = true;
        let workspace_id = self
            .state
            .workspaces
            .iter()
            .position(|workspace| workspace.pane_state(pane_id).is_some())
            .map(|ws_idx| self.public_workspace_id(ws_idx));
        let previous_toast = self.state.toast.clone();
        let context = format!(
            "{} on {} did not restore within 120 seconds",
            record.remote_terminal_id, record.peer_id
        );
        let merged_context = self
            .state
            .toast
            .as_ref()
            .filter(|toast| toast.title == "remote restoration timed out")
            .map(|toast| format!("{}\n{}", toast.context, context))
            .unwrap_or(context);
        self.state.toast = Some(crate::app::state::ToastNotification {
            kind: crate::app::state::ToastKind::NeedsAttention,
            title: "remote restoration timed out".into(),
            context: merged_context,
            position: None,
            target: workspace_id.map(|workspace_id| crate::app::state::ToastTarget {
                workspace_id,
                pane_id,
            }),
        });
        self.sync_toast_deadline(previous_toast);
        if self.local_terminal_notifications {
            let body = format!("{} on {}", record.remote_terminal_id, record.peer_id);
            match self.state.toast_config.delivery {
                crate::config::ToastDelivery::Terminal => {
                    let _ = crate::terminal_notify::show_notification(
                        "remote restoration timed out",
                        Some(&body),
                    );
                }
                crate::config::ToastDelivery::System => {
                    let _ = crate::platform::show_desktop_notification(
                        "remote restoration timed out",
                        Some(&body),
                    );
                }
                _ => {}
            }
        }
    }

    pub(crate) fn handle_remote_restore_panel_action(
        &mut self,
        pane_id: crate::layout::PaneId,
        action: RemoteRestorePanelAction,
    ) {
        let Some(remote_terminal_id) = self
            .state
            .remote_restore_panels
            .get(&pane_id)
            .map(|panel| panel.remote_terminal_id.clone())
        else {
            return;
        };
        let Ok(store) = ResumeStore::for_active_session() else {
            return;
        };
        let record = store
            .records()
            .iter()
            .find(|record| record.remote_terminal_id == remote_terminal_id)
            .cloned();
        match action {
            RemoteRestorePanelAction::Retry => {
                let can_retry = self
                    .state
                    .remote_restore_panels
                    .get(&pane_id)
                    .is_some_and(|panel| panel.status.can_retry());
                if can_retry {
                    if let Some(record) = record {
                        if let Err(err) = self.enqueue_remote_restore(record, None, None) {
                            if let Some(panel) = self.state.remote_restore_panels.get_mut(&pane_id)
                            {
                                panel.status = crate::app::state::RemoteRestoreStatus::Retryable {
                                    message: err,
                                };
                            }
                        }
                    }
                }
            }
            RemoteRestorePanelAction::Close => {
                if self
                    .discard_remote_restore_by_id(&remote_terminal_id)
                    .is_err()
                {
                    return;
                }
                if let Some((ws_idx, _)) = self.find_pane(pane_id) {
                    if let Some(public_pane_id) = self.public_pane_id(ws_idx, pane_id) {
                        let _ = self.close_pane(
                            "remote-restore-close".into(),
                            &PaneTarget {
                                pane_id: public_pane_id,
                            },
                        );
                    }
                }
            }
        }
    }

    pub(crate) fn discard_remote_restore_for_pane(
        &mut self,
        pane_id: crate::layout::PaneId,
    ) -> Result<bool, String> {
        let Some(remote_terminal_id) = self
            .state
            .remote_restore_panels
            .get(&pane_id)
            .map(|panel| panel.remote_terminal_id.clone())
        else {
            return Ok(false);
        };
        self.discard_remote_restore_by_id(&remote_terminal_id)?;
        Ok(true)
    }

    fn discard_remote_restore_by_id(&mut self, remote_terminal_id: &str) -> Result<(), String> {
        let mut store = ResumeStore::for_active_session()
            .map_err(|err| format!("could not open remote resume store: {err}"))?;
        let record = store.find(remote_terminal_id).cloned();
        let terminate_credentials = record
            .as_ref()
            .map(|record| parked_terminate_credentials(&store, record))
            .transpose()
            .map_err(|err| format!("could not load termination credentials: {err}"))?
            .flatten();
        if terminate_credentials.is_some() {
            store
                .mark_termination_pending(remote_terminal_id)
                .map_err(|err| format!("could not retain termination ticket: {err}"))?;
        } else {
            store
                .remove(remote_terminal_id)
                .map_err(|err| format!("could not discard restore record: {err}"))?;
        }
        if let Some(worker) = self.remote_restore_workers.remove(remote_terminal_id) {
            worker.cancelled.store(true, Ordering::Release);
        }
        let mut cancelled_request_tokens = Vec::new();
        self.remote_restore_queue.retain(|queued| {
            if queued.record.remote_terminal_id == remote_terminal_id {
                if let Some(token) = queued.request_token {
                    cancelled_request_tokens.push((token, queued.record.peer_id.clone()));
                }
                false
            } else {
                true
            }
        });
        for (token, peer_id) in cancelled_request_tokens {
            self.record_remote_resume_outcome(
                token,
                RemoteResumeOutcome {
                    remote_terminal_id: remote_terminal_id.to_string(),
                    peer_id,
                    error: Some("remote restoration was cancelled".into()),
                },
            );
            self.finish_remote_resume_request_if_ready(token);
        }
        self.state
            .remote_restore_panels
            .retain(|_, panel| panel.remote_terminal_id != remote_terminal_id);
        if let (Some(record), Some(credentials)) = (record, terminate_credentials) {
            self.spawn_best_effort_parked_terminate(record, credentials);
        }
        Ok(())
    }

    pub(crate) fn terminal_recovery_list(&self) -> Vec<schema::TerminalRecoveryInfo> {
        let store = ResumeStore::for_active_session().ok();
        let mut terminal_ids = std::collections::BTreeSet::new();
        if let Some(store) = store.as_ref() {
            terminal_ids.extend(
                store
                    .records()
                    .iter()
                    .map(|record| record.remote_terminal_id.clone()),
            );
        }
        terminal_ids.extend(
            self.state
                .remote_restore_panels
                .values()
                .map(|panel| panel.remote_terminal_id.clone()),
        );
        terminal_ids.extend(
            self.remote_restore_queue
                .iter()
                .map(|queued| queued.record.remote_terminal_id.clone()),
        );
        terminal_ids.extend(self.remote_restore_workers.keys().cloned());

        let now = Instant::now();
        terminal_ids
            .into_iter()
            .filter_map(|terminal_id| {
                self.terminal_recovery_info(&terminal_id, store.as_ref(), now)
            })
            .collect()
    }

    pub(crate) fn terminal_recovery_status(
        &self,
        remote_terminal_id: &str,
    ) -> Option<schema::TerminalRecoveryInfo> {
        let store = ResumeStore::for_active_session().ok();
        self.terminal_recovery_info(remote_terminal_id, store.as_ref(), Instant::now())
    }

    fn terminal_recovery_info(
        &self,
        remote_terminal_id: &str,
        store: Option<&ResumeStore>,
        now: Instant,
    ) -> Option<schema::TerminalRecoveryInfo> {
        let record = store.and_then(|store| store.find(remote_terminal_id));
        let recovery_state = store.and_then(|store| store.recovery_state(remote_terminal_id));
        let queued = self
            .remote_restore_queue
            .iter()
            .find(|queued| queued.record.remote_terminal_id == remote_terminal_id);
        let panel = self
            .state
            .remote_restore_panels
            .values()
            .find(|panel| panel.remote_terminal_id == remote_terminal_id);
        let worker_active = self.remote_restore_workers.contains_key(remote_terminal_id);
        if record.is_none() && queued.is_none() && panel.is_none() && !worker_active {
            return None;
        }

        let peer_id = queued
            .map(|queued| queued.record.peer_id.clone())
            .or_else(|| panel.map(|panel| panel.peer_id.clone()))
            .or_else(|| record.map(|record| record.peer_id.clone()))
            .unwrap_or_default();
        let record_message = record.and_then(|record| record.last_error.clone());
        let (status, message) = match recovery_state.map(|state| state.lifecycle) {
            Some(ResumeLifecycle::TerminationPending) => (
                schema::TerminalRecoveryStatus::Discarding,
                record_message.clone(),
            ),
            Some(ResumeLifecycle::PlacedCleanupPending) => (
                schema::TerminalRecoveryStatus::CleanupPending,
                record_message.clone(),
            ),
            _ if queued.is_some_and(|queued| now >= queued.deadline) => (
                schema::TerminalRecoveryStatus::TimedOut,
                Some("remote restoration did not complete within 120 seconds".into()),
            ),
            _ if queued.is_some() => (
                schema::TerminalRecoveryStatus::Queued,
                record_message.clone(),
            ),
            _ if worker_active => (
                schema::TerminalRecoveryStatus::Restoring,
                record_message.clone(),
            ),
            _ => match panel.map(|panel| &panel.status) {
                Some(crate::app::state::RemoteRestoreStatus::Restoring) => (
                    schema::TerminalRecoveryStatus::Restoring,
                    record_message.clone(),
                ),
                Some(crate::app::state::RemoteRestoreStatus::TimedOut { message }) => (
                    schema::TerminalRecoveryStatus::TimedOut,
                    Some(message.clone()),
                ),
                Some(crate::app::state::RemoteRestoreStatus::Retryable { message }) => (
                    schema::TerminalRecoveryStatus::Retryable,
                    Some(message.clone()),
                ),
                Some(crate::app::state::RemoteRestoreStatus::Ended { message }) => {
                    (schema::TerminalRecoveryStatus::Ended, Some(message.clone()))
                }
                None => (
                    schema::TerminalRecoveryStatus::Pending,
                    record_message.clone(),
                ),
            },
        };
        Some(schema::TerminalRecoveryInfo {
            remote_terminal_id: remote_terminal_id.to_string(),
            peer_id,
            status,
            message,
        })
    }

    pub(crate) fn retry_terminal_recovery(
        &mut self,
        remote_terminal_id: &str,
    ) -> Result<schema::TerminalRecoveryInfo, schema::ErrorBody> {
        if self.remote_restore_workers.contains_key(remote_terminal_id)
            || self
                .remote_restore_queue
                .iter()
                .any(|queued| queued.record.remote_terminal_id == remote_terminal_id)
        {
            return Err(schema::ErrorBody {
                code: "terminal_recovery_active".into(),
                message: format!("terminal recovery {remote_terminal_id} is already active"),
            });
        }
        let current = self
            .terminal_recovery_status(remote_terminal_id)
            .ok_or_else(|| schema::ErrorBody {
                code: "terminal_recovery_not_found".into(),
                message: format!("terminal recovery {remote_terminal_id} was not found"),
            })?;
        if !matches!(
            current.status,
            schema::TerminalRecoveryStatus::TimedOut | schema::TerminalRecoveryStatus::Retryable
        ) {
            return Err(schema::ErrorBody {
                code: "terminal_recovery_not_retryable".into(),
                message: format!("terminal recovery {remote_terminal_id} is not retryable"),
            });
        }
        let record = ResumeStore::for_active_session()
            .map_err(|err| schema::ErrorBody {
                code: "terminal_recovery_store_unavailable".into(),
                message: format!("could not open remote recovery store: {err}"),
            })?
            .find(remote_terminal_id)
            .cloned()
            .ok_or_else(|| schema::ErrorBody {
                code: "terminal_recovery_not_found".into(),
                message: format!("terminal recovery {remote_terminal_id} was not found"),
            })?;
        self.enqueue_remote_restore(record, None, None)
            .map_err(|message| schema::ErrorBody {
                code: "terminal_recovery_retry_failed".into(),
                message,
            })?;
        self.terminal_recovery_status(remote_terminal_id)
            .ok_or_else(|| schema::ErrorBody {
                code: "terminal_recovery_not_found".into(),
                message: format!("terminal recovery {remote_terminal_id} disappeared"),
            })
    }

    pub(crate) fn discard_terminal_recovery(
        &mut self,
        remote_terminal_id: &str,
    ) -> Result<(), schema::ErrorBody> {
        if self.terminal_recovery_status(remote_terminal_id).is_none() {
            return Err(schema::ErrorBody {
                code: "terminal_recovery_not_found".into(),
                message: format!("terminal recovery {remote_terminal_id} was not found"),
            });
        }
        self.discard_remote_restore_by_id(remote_terminal_id)
            .map_err(|message| schema::ErrorBody {
                code: "terminal_recovery_discard_failed".into(),
                message,
            })
    }

    fn spawn_best_effort_parked_terminate(
        &mut self,
        record: ResumeRecord,
        credentials: ParkedTerminateCredentials,
    ) {
        self.remote_api_jobs_in_flight += 1;
        let event_tx = self.event_tx.clone();
        let terminal_id = record.remote_terminal_id.clone();
        let cancelled = Arc::new(AtomicBool::new(false));
        if let Err(err) = std::thread::Builder::new()
            .name("herdr-remote-terminate".into())
            .spawn(move || {
                let result = crate::remote_agent::terminate_parked_until(
                    &record,
                    None,
                    &credentials.park_id,
                    &credentials.origin_id,
                    &credentials.discovery_token,
                    Instant::now() + Duration::from_secs(15),
                    &cancelled,
                )
                .map_err(|err| err.to_string());
                let _ = event_tx.blocking_send(AppEvent::RemoteParkedTerminateFinished(
                    RemoteParkedTerminateResult {
                        remote_terminal_id: terminal_id,
                        result,
                    },
                ));
            })
        {
            self.finish_remote_api_job();
            tracing::warn!(error = %err, "could not start parked terminal termination");
        }
    }

    pub(crate) fn finish_remote_parked_terminate(&mut self, result: RemoteParkedTerminateResult) {
        self.finish_remote_api_job();
        match result.result {
            Ok(()) => {
                if let Ok(mut store) = ResumeStore::for_active_session() {
                    let _ = store.remove(&result.remote_terminal_id);
                }
            }
            Err(err) => {
                if let Ok(mut store) = ResumeStore::for_active_session() {
                    let _ = store.set_last_error(&result.remote_terminal_id, Some(err.clone()));
                }
                tracing::warn!(
                    terminal = %result.remote_terminal_id,
                    error = %err,
                    "parked terminal termination remains pending"
                );
            }
        }
        self.pump_remote_restore_queue();
    }

    fn record_remote_resume_outcome(&mut self, token: u64, outcome: RemoteResumeOutcome) {
        if let Some(request) = self.pending_remote_resume_requests.get_mut(&token) {
            request.remaining.remove(&outcome.remote_terminal_id);
            request.outcomes.push(outcome);
        }
    }

    fn finish_remote_resume_request_if_ready(&mut self, token: u64) {
        let ready = self
            .pending_remote_resume_requests
            .get(&token)
            .is_some_and(|request| request.remaining.is_empty());
        if !ready {
            return;
        }
        let Some(request) = self.pending_remote_resume_requests.remove(&token) else {
            return;
        };
        let response = encode_success(
            request.id.clone(),
            ResponseResult::RemoteResume {
                outcomes: request.outcomes,
            },
        )
        .unwrap_or_else(|| {
            encode_resume_error(
                &request.id,
                "internal_error",
                "failed to encode remote resume response",
            )
        });
        let _ = request.respond_to.send(response);
    }

    pub(crate) fn show_orphan_review(
        &mut self,
        entries: Vec<crate::app::state::OrphanReviewEntry>,
    ) {
        if entries.is_empty() {
            return;
        }
        if let Some(review) = self.state.orphan_review.as_mut() {
            for entry in entries {
                if !review.entries.iter().any(|existing| {
                    existing.park_id == entry.park_id && existing.source == entry.source
                }) {
                    review.entries.push(entry);
                }
            }
            review
                .entries
                .sort_by(|left, right| left.park_id.cmp(&right.park_id));
            review.selected = review.selected.min(review.entries.len().saturating_sub(1));
        } else {
            self.state.orphan_review = Some(crate::app::state::OrphanReviewState {
                entries,
                selected: 0,
                pending_action: None,
                error: None,
            });
        }
    }

    pub(crate) fn show_local_parked_terminal_review(&mut self) {
        let entries = self
            .parked_terminal_inventory()
            .into_iter()
            .map(|parked| crate::app::state::OrphanReviewEntry {
                park_id: parked.park_id,
                terminal_id: parked.terminal_id,
                pane_id: parked.pane_id,
                source: crate::app::state::OrphanReviewSource::LocalServer,
            })
            .collect();
        self.show_orphan_review(entries);
    }

    pub(crate) fn discover_remote_parked_terminals(&mut self, peer: crate::api::schema::PeerInfo) {
        if self
            .remote_orphan_inventory_cancellations
            .contains_key(&peer.id)
            || self.remote_api_jobs_in_flight >= MAX_CONCURRENT_REMOTE_RESTORES
        {
            return;
        }
        let mut identity = match RecoveryIdentityStore::open_global() {
            Ok(identity) => identity,
            Err(err) => {
                tracing::warn!(error = %err, peer = %peer.id, "could not open recovery identity for orphan discovery");
                return;
            }
        };
        let credentials = match identity.credentials_for_peer(&peer.id) {
            Ok(credentials) => credentials,
            Err(err) => {
                tracing::warn!(error = %err, peer = %peer.id, "could not load orphan discovery credential");
                return;
            }
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.remote_orphan_inventory_cancellations
            .insert(peer.id.clone(), cancelled.clone());
        self.remote_api_jobs_in_flight += 1;
        let peer_id = peer.id.clone();
        let worker_peer_id = peer_id.clone();
        let event_tx = self.event_tx.clone();
        if let Err(err) = std::thread::Builder::new()
            .name("herdr-remote-orphan-list".into())
            .spawn(move || {
                let result = crate::remote_agent::list_parked_until(
                    &peer,
                    &credentials.origin_id,
                    credentials.discovery_token.expose_secret(),
                    Instant::now() + Duration::from_secs(15),
                    &cancelled,
                )
                .map_err(|err| err.to_string());
                let _ = event_tx.blocking_send(AppEvent::RemoteOrphanInventoryFinished(
                    RemoteOrphanInventoryResult {
                        peer_id: worker_peer_id,
                        result,
                    },
                ));
            })
        {
            self.finish_remote_api_job();
            self.remote_orphan_inventory_cancellations.remove(&peer_id);
            tracing::warn!(error = %err, "could not start remote orphan inventory worker");
        }
    }

    pub(crate) fn finish_remote_orphan_inventory(&mut self, result: RemoteOrphanInventoryResult) {
        self.finish_remote_api_job();
        self.remote_orphan_inventory_cancellations
            .remove(&result.peer_id);
        match result.result {
            Ok(parked) => {
                let known_park_ids: HashSet<String> = ResumeStore::for_active_session()
                    .ok()
                    .map(|store| {
                        store
                            .records()
                            .iter()
                            .filter_map(|record| {
                                store
                                    .recovery_state(&record.remote_terminal_id)
                                    .and_then(|state| state.park_id.clone())
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let entries = parked
                    .into_iter()
                    .filter(|parked| !known_park_ids.contains(&parked.park_id))
                    .map(|parked| crate::app::state::OrphanReviewEntry {
                        park_id: parked.park_id,
                        terminal_id: parked.terminal_id,
                        pane_id: parked.pane_id,
                        source: crate::app::state::OrphanReviewSource::RemotePeer {
                            peer_id: result.peer_id.clone(),
                        },
                    })
                    .collect();
                self.show_orphan_review(entries);
            }
            Err(err) => tracing::debug!(
                peer = %result.peer_id,
                error = %err,
                "remote parked-terminal inventory was unavailable"
            ),
        }
        self.pump_remote_restore_queue();
    }

    pub(crate) fn handle_orphan_review_action(
        &mut self,
        action: crate::app::state::OrphanReviewAction,
    ) {
        let Some(entry) = self
            .state
            .orphan_review
            .as_ref()
            .and_then(|review| review.entries.get(review.selected).cloned())
        else {
            return;
        };
        if let Some(review) = self.state.orphan_review.as_mut() {
            review.pending_action = Some((entry.park_id.clone(), action));
            review.error = None;
        }
        match entry.source.clone() {
            crate::app::state::OrphanReviewSource::LocalServer => {
                let result = match action {
                    crate::app::state::OrphanReviewAction::Retain => self
                        .retain_parked_terminal_admin(&entry.park_id)
                        .map(|_| ()),
                    crate::app::state::OrphanReviewAction::Terminate => {
                        self.terminate_parked_terminal_admin(&entry.park_id)
                    }
                    crate::app::state::OrphanReviewAction::Promote => {
                        self.promote_parked_terminal_admin(&entry.park_id).map(|_| {
                            if let Some(ws_idx) = self.state.workspaces.len().checked_sub(1) {
                                self.state.active = Some(ws_idx);
                                self.state.selected = ws_idx;
                            }
                        })
                    }
                };
                match result {
                    Ok(()) => self.finish_orphan_review_entry(&entry),
                    Err(error) => self.fail_orphan_review_action(error.message),
                }
            }
            crate::app::state::OrphanReviewSource::RemotePeer { peer_id } => {
                self.spawn_remote_orphan_action(entry, peer_id, action);
            }
        }
    }

    fn spawn_remote_orphan_action(
        &mut self,
        entry: crate::app::state::OrphanReviewEntry,
        peer_id: String,
        action: crate::app::state::OrphanReviewAction,
    ) {
        if self.remote_api_jobs_in_flight >= MAX_CONCURRENT_REMOTE_RESTORES
            || self
                .remote_orphan_action_cancellations
                .contains_key(&entry.park_id)
        {
            self.fail_orphan_review_action("remote operation capacity is busy".into());
            return;
        }
        let Some(peer) = self.state.peers.get(&peer_id).cloned() else {
            self.fail_orphan_review_action("remote peer is no longer connected".into());
            return;
        };
        let mut identity = match RecoveryIdentityStore::open_global() {
            Ok(identity) => identity,
            Err(err) => {
                self.fail_orphan_review_action(err.to_string());
                return;
            }
        };
        let credentials = match identity.credentials_for_peer(&peer_id) {
            Ok(credentials) => credentials,
            Err(err) => {
                self.fail_orphan_review_action(err.to_string());
                return;
            }
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        self.remote_orphan_action_cancellations
            .insert(entry.park_id.clone(), cancelled.clone());
        self.remote_api_jobs_in_flight += 1;
        let event_tx = self.event_tx.clone();
        let worker_entry = entry.clone();
        let worker_parked = crate::api::schema::TerminalParkedInfo {
            park_id: entry.park_id.clone(),
            terminal_id: entry.terminal_id.clone(),
            pane_id: entry.pane_id.clone(),
            origin_id: credentials.origin_id.clone(),
            status: crate::api::schema::TerminalDelegationStatus::Parked,
            resuming: false,
        };
        if let Err(err) = std::thread::Builder::new()
            .name("herdr-remote-orphan-resolve".into())
            .spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(30);
                let result = if action == crate::app::state::OrphanReviewAction::Promote {
                    crate::remote_agent::reacquire_discovered_until(
                        &peer,
                        &worker_parked,
                        &credentials.origin_id,
                        credentials.discovery_token.expose_secret(),
                        deadline,
                        &cancelled,
                    )
                    .map(|remote| RemoteOrphanResolveOutcome::Promoted(Box::new(remote)))
                } else {
                    let remote_action = match action {
                        crate::app::state::OrphanReviewAction::Retain => {
                            crate::api::schema::TerminalParkedResolveAction::Retain
                        }
                        crate::app::state::OrphanReviewAction::Terminate => {
                            crate::api::schema::TerminalParkedResolveAction::Terminate
                        }
                        crate::app::state::OrphanReviewAction::Promote => unreachable!(),
                    };
                    crate::remote_agent::resolve_parked_until(
                        &peer,
                        &worker_entry.park_id,
                        &credentials.origin_id,
                        credentials.discovery_token.expose_secret(),
                        remote_action,
                        None,
                        deadline,
                        &cancelled,
                    )
                    .map(|_| RemoteOrphanResolveOutcome::Resolved)
                }
                .map_err(|err| err.to_string());
                let event = AppEvent::RemoteOrphanResolveFinished(RemoteOrphanResolveResult {
                    entry: worker_entry,
                    result,
                });
                if let Err(err) = event_tx.blocking_send(event) {
                    if let AppEvent::RemoteOrphanResolveFinished(mut result) = err.0 {
                        if let Ok(RemoteOrphanResolveOutcome::Promoted(remote)) = &mut result.result
                        {
                            crate::remote_agent::rollback_reacquire(remote);
                        }
                    }
                }
            })
        {
            self.finish_remote_api_job();
            self.remote_orphan_action_cancellations
                .remove(&entry.park_id);
            self.fail_orphan_review_action(format!("could not start orphan action: {err}"));
        }
    }

    pub(crate) fn finish_remote_orphan_resolve(&mut self, mut result: RemoteOrphanResolveResult) {
        self.finish_remote_api_job();
        self.remote_orphan_action_cancellations
            .remove(&result.entry.park_id);
        let outcome = match &mut result.result {
            Ok(RemoteOrphanResolveOutcome::Resolved) => Ok(()),
            Ok(RemoteOrphanResolveOutcome::Promoted(remote)) => self
                .place_discovered_remote_workspace(remote)
                .inspect_err(|_| crate::remote_agent::rollback_reacquire(remote)),
            Err(error) => Err(error.clone()),
        };
        match outcome {
            Ok(()) => self.finish_orphan_review_entry(&result.entry),
            Err(error) => self.fail_orphan_review_action(error),
        }
        self.pump_remote_restore_queue();
    }

    fn place_discovered_remote_workspace(
        &mut self,
        remote: &mut crate::remote_agent::RemoteAgentStart,
    ) -> Result<(), String> {
        let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let (rows, cols) = self.state.estimate_pane_size();
        let (ws_idx, _, pane_id) = self
            .spawn_agent_workspace(cwd, rows, cols, &remote.attach_argv, Vec::new(), true)
            .map_err(|err| format!("{err:?}"))?;
        self.state.workspaces[ws_idx].custom_name = Some("Recovered remote terminal".into());
        let terminal_id = self.state.workspaces[ws_idx]
            .terminal_id(pane_id)
            .cloned()
            .ok_or_else(|| "recovered terminal disappeared after spawn".to_string())?;
        let terminal = self
            .state
            .terminals
            .get_mut(&terminal_id)
            .ok_or_else(|| "recovered terminal state disappeared after spawn".to_string())?;
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
        self.state.mark_session_dirty();
        self.schedule_session_save();
        Ok(())
    }

    fn finish_orphan_review_entry(&mut self, entry: &crate::app::state::OrphanReviewEntry) {
        let Some(review) = self.state.orphan_review.as_mut() else {
            return;
        };
        review.entries.retain(|candidate| {
            candidate.park_id != entry.park_id || candidate.source != entry.source
        });
        review.pending_action = None;
        review.error = None;
        review.selected = review.selected.min(review.entries.len().saturating_sub(1));
        if review.entries.is_empty() {
            self.state.orphan_review = None;
        }
    }

    fn fail_orphan_review_action(&mut self, message: String) {
        if let Some(review) = self.state.orphan_review.as_mut() {
            review.pending_action = None;
            review.error = Some(message);
        }
    }
}

fn run_reacquire_batch(
    records: Vec<ResumeRecord>,
    managed_control_path: Option<String>,
) -> Vec<RemoteReacquireResult> {
    let mut results = Vec::with_capacity(records.len());
    let mut queued = records
        .into_iter()
        .map(|record| {
            let deadline = remote_restore_deadline(Instant::now());
            let credentials = load_remote_restore_credentials(&record);
            (record, deadline, credentials)
        })
        .collect::<std::collections::VecDeque<_>>();
    while !queued.is_empty() {
        let batch = (0..MAX_CONCURRENT_REMOTE_RESTORES)
            .filter_map(|_| queued.pop_front())
            .collect::<Vec<_>>();
        let mut runnable = Vec::new();
        for (record, deadline, credentials) in batch {
            match credentials {
                Ok(credentials) => runnable.push((record, deadline, credentials)),
                Err(message) => results.push(RemoteReacquireResult {
                    record,
                    generation: 0,
                    request_token: None,
                    result: Err(RemoteReacquireFailure::Retryable { message }),
                }),
            }
        }
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(runnable.len());
            for (record, deadline, credentials) in runnable {
                let control_path = managed_control_path.clone();
                handles.push(scope.spawn(move || {
                    run_remote_restore_worker(
                        record,
                        control_path,
                        deadline,
                        0,
                        None,
                        Arc::new(AtomicBool::new(false)),
                        credentials,
                    )
                }));
            }
            for handle in handles {
                if let Ok(result) = handle.join() {
                    results.push(result);
                }
            }
        });
    }
    results
}

fn run_remote_restore_worker(
    record: ResumeRecord,
    managed_control_path: Option<String>,
    deadline: Instant,
    generation: u64,
    request_token: Option<u64>,
    cancelled: Arc<AtomicBool>,
    credentials: RemoteRestoreCredentials,
) -> RemoteReacquireResult {
    let mut backoff = REMOTE_RESTORE_INITIAL_BACKOFF;
    let mut last_transport_error = "remote machine is unreachable".to_string();
    let result = loop {
        if cancelled.load(Ordering::Acquire) {
            break Err(RemoteReacquireFailure::Cancelled);
        }
        if Instant::now() >= deadline {
            break Err(RemoteReacquireFailure::TimedOut {
                message: remote_restore_timeout_message(&last_transport_error),
            });
        }
        let attempt = match &credentials {
            RemoteRestoreCredentials::Parked {
                park_id,
                origin_id,
                resume_token,
            } => crate::remote_agent::reacquire_until(
                &record,
                managed_control_path.clone(),
                park_id,
                origin_id,
                resume_token,
                deadline,
                &cancelled,
            ),
            RemoteRestoreCredentials::LegacyVisibleHandoff => {
                crate::remote_agent::reacquire(&record, managed_control_path.clone())
            }
        };
        match attempt {
            Ok(remote) => break Ok(remote),
            Err(crate::remote_agent::RemoteParkedError::Gone { message, .. }) => {
                break Err(RemoteReacquireFailure::Ended { message });
            }
            Err(crate::remote_agent::RemoteParkedError::Unauthorized { message, .. })
            | Err(crate::remote_agent::RemoteParkedError::Rejected { message, .. }) => {
                break Err(RemoteReacquireFailure::Retryable { message });
            }
            Err(crate::remote_agent::RemoteParkedError::InvalidResponse(message)) => {
                break Err(RemoteReacquireFailure::Retryable { message });
            }
            Err(crate::remote_agent::RemoteParkedError::Busy { message, .. }) => {
                last_transport_error = message;
            }
            Err(crate::remote_agent::RemoteParkedError::Transport(err)) => {
                last_transport_error = err.to_string();
            }
        }
        let now = Instant::now();
        if now >= deadline {
            break Err(RemoteReacquireFailure::TimedOut {
                message: remote_restore_timeout_message(&last_transport_error),
            });
        }
        let delay = backoff.min(deadline.saturating_duration_since(now));
        if sleep_with_cancellation(delay, &cancelled) {
            break Err(RemoteReacquireFailure::Cancelled);
        }
        backoff = backoff.saturating_mul(2).min(REMOTE_RESTORE_MAX_BACKOFF);
    };
    RemoteReacquireResult {
        record,
        generation,
        request_token,
        result,
    }
}

fn remote_restore_deadline(enqueued_at: Instant) -> Instant {
    enqueued_at + REMOTE_RESTORE_TIMEOUT
}

fn remote_restore_timeout_message(last_transport_error: &str) -> String {
    format!("remote restoration did not complete within 120 seconds: {last_transport_error}")
}

fn load_remote_restore_credentials(
    record: &ResumeRecord,
) -> Result<RemoteRestoreCredentials, String> {
    let store = ResumeStore::for_active_session()
        .map_err(|err| format!("could not open remote resume store: {err}"))?;
    let state = store
        .recovery_state(&record.remote_terminal_id)
        .ok_or_else(|| "remote resume record has no recovery state".to_string())?;
    if state.lifecycle == ResumeLifecycle::LegacyVisibleHandoff {
        return Ok(RemoteRestoreCredentials::LegacyVisibleHandoff);
    }
    let park_id = state
        .park_id
        .clone()
        .ok_or_else(|| "remote resume record has no park id".to_string())?;
    let resume_token = state
        .resume_token
        .as_ref()
        .map(|token| token.expose_secret().to_string())
        .ok_or_else(|| "remote resume record has no resume token".to_string())?;
    let identity = RecoveryIdentityStore::open_global()
        .map_err(|err| format!("could not open remote recovery identity: {err}"))?;
    Ok(RemoteRestoreCredentials::Parked {
        park_id,
        origin_id: identity.origin_id().to_string(),
        resume_token,
    })
}

fn parked_terminate_credentials(
    store: &ResumeStore,
    record: &ResumeRecord,
) -> Result<Option<ParkedTerminateCredentials>, String> {
    let Some(state) = store.recovery_state(&record.remote_terminal_id) else {
        return Ok(None);
    };
    let Some(park_id) = state.park_id.clone() else {
        return Ok(None);
    };
    let mut identity = RecoveryIdentityStore::open_global()
        .map_err(|err| format!("could not open remote recovery identity: {err}"))?;
    let credentials = identity
        .credentials_for_peer(&record.peer_id)
        .map_err(|err| format!("could not load remote discovery credentials: {err}"))?;
    Ok(Some(ParkedTerminateCredentials {
        park_id,
        origin_id: credentials.origin_id,
        discovery_token: credentials.discovery_token.expose_secret().to_string(),
    }))
}

fn sleep_with_cancellation(duration: Duration, cancelled: &AtomicBool) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if cancelled.load(Ordering::Acquire) {
            return true;
        }
        std::thread::sleep(
            Duration::from_millis(50).min(deadline.saturating_duration_since(Instant::now())),
        );
    }
    cancelled.load(Ordering::Acquire)
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

fn encode_resume_error_body(id: &str, error: schema::ErrorBody) -> String {
    serde_json::to_string(&schema::ErrorResponse {
        id: id.to_string(),
        error,
    })
    .unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recovery_test_record(
        app: &App,
        pane_id: crate::layout::PaneId,
        index: usize,
    ) -> ResumeRecord {
        ResumeRecord {
            schema: crate::remote_resume::RESUME_SCHEMA_VERSION,
            remote_terminal_id: format!("remote-terminal-{index}"),
            remote_pane_id: format!("remote-pane-{index}"),
            peer_id: "peer.example".into(),
            ssh: ResumeSsh {
                target: "unreachable.invalid".into(),
                ssh_args: Vec::new(),
                session: None,
            },
            agent: None,
            placement: ResumePlacement {
                workspace_id: app.public_workspace_id(0),
                public_tab_id: app.public_tab_id(0, 0).expect("public tab id"),
                public_pane_id: app.public_pane_id(0, pane_id),
                pane_index: None,
            },
            handed_off_at_unix_ms: 0,
            last_error: None,
        }
    }

    fn recovery_test_app() -> (App, crate::layout::PaneId) {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![crate::workspace::Workspace::test_new("recovery-test")];
        app.state.active = Some(0);
        app.state.selected = 0;
        app.state.ensure_test_terminals();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        app.state.workspaces[0].tabs[0]
            .panes
            .get_mut(&pane_id)
            .expect("root pane")
            .remote_restore_reservation = true;
        (app, pane_id)
    }

    #[test]
    fn resume_errors_preserve_the_callers_request_id() {
        let encoded = encode_resume_error_body(
            "caller-request-42",
            schema::ErrorBody {
                code: "remote_resume_empty".into(),
                message: "nothing to restore".into(),
            },
        );
        let response: schema::ErrorResponse = serde_json::from_str(&encoded).unwrap();
        assert_eq!(response.id, "caller-request-42");
        assert_eq!(response.error.code, "remote_resume_empty");
    }

    #[test]
    fn retry_sleep_observes_per_terminal_cancellation() {
        let cancelled = AtomicBool::new(true);
        let started = Instant::now();
        assert!(sleep_with_cancellation(Duration::from_secs(30), &cancelled));
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn nine_enqueued_restores_expire_independently_and_aggregate_notifications() {
        let (mut app, pane_id) = recovery_test_app();
        let enqueued_at = Instant::now() - REMOTE_RESTORE_TIMEOUT - Duration::from_millis(1);
        let deadline = remote_restore_deadline(enqueued_at);

        for index in 0..9 {
            let record = recovery_test_record(&app, pane_id, index);
            let finished = run_remote_restore_worker(
                record.clone(),
                None,
                deadline,
                index as u64 + 1,
                None,
                Arc::new(AtomicBool::new(false)),
                RemoteRestoreCredentials::LegacyVisibleHandoff,
            );
            let Err(RemoteReacquireFailure::TimedOut { message }) = finished.result else {
                panic!("expired restore should report a timeout");
            };
            assert!(message.contains("did not complete within 120 seconds"));

            app.state.remote_restore_panels.insert(
                pane_id,
                crate::app::state::RemoteRestorePanelState {
                    remote_terminal_id: record.remote_terminal_id.clone(),
                    peer_id: record.peer_id.clone(),
                    status: crate::app::state::RemoteRestoreStatus::TimedOut {
                        message: "unreachable".into(),
                    },
                    generation: index as u64 + 1,
                    timeout_notified: false,
                },
            );
            app.notify_remote_restore_timeout(pane_id, &record);
        }

        let context = &app.state.toast.as_ref().expect("timeout toast").context;
        for index in 0..9 {
            assert!(
                context.contains(&format!("remote-terminal-{index}")),
                "timeout notification omitted terminal {index}: {context}"
            );
        }
    }

    #[test]
    fn placed_cleanup_pending_is_retained_until_reservation_clears() {
        let (mut app, pane_id) = recovery_test_app();
        let record = recovery_test_record(&app, pane_id, 0);
        let unique = crate::remote_resume::generate_recovery_id().expect("test id");
        let path = std::env::temp_dir().join(format!(
            "herdr-placed-cleanup-{}-{unique}.json",
            std::process::id()
        ));
        let token: SecretToken =
            serde_json::from_str("\"test-resume-token\"").expect("resume token");
        let mut store = ResumeStore::open(path.clone()).expect("test resume store");
        store
            .upsert_parking(record.clone(), "park-test".into(), token)
            .expect("parking record");
        store
            .mark_placed_cleanup_pending(&record.remote_terminal_id)
            .expect("cleanup pending");

        assert!(app.record_has_explicit_reservation(&record));
        if app.record_has_explicit_reservation(&record) {
            store
                .mark_parked(&record.remote_terminal_id)
                .expect("retain interrupted placement");
        } else {
            store
                .remove(&record.remote_terminal_id)
                .expect("remove completed placement");
        }
        assert!(store.find(&record.remote_terminal_id).is_some());

        app.state.workspaces[0].tabs[0]
            .panes
            .get_mut(&pane_id)
            .expect("root pane")
            .remote_restore_reservation = false;
        if app.record_has_explicit_reservation(&record) {
            store
                .mark_parked(&record.remote_terminal_id)
                .expect("retain interrupted placement");
        } else {
            store
                .remove(&record.remote_terminal_id)
                .expect("remove completed placement");
        }
        assert!(store.find(&record.remote_terminal_id).is_none());

        drop(store);
        let _ = std::fs::remove_file(path);
    }
}
