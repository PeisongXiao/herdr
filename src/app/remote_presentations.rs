use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::api::schema::{
    ErrorBody, TerminalDelegateClaimParams, TerminalDelegateCreateParams, TerminalDelegationClaim,
    TerminalDelegationInfo, TerminalDelegationStatus,
};
use crate::layout::PaneId;
use crate::terminal::TerminalId;
use crate::workspace::MovedPane;

use super::App;

const PENDING_DELEGATION_TTL: Duration = Duration::from_secs(30);
const COMPLETED_DELEGATION_TTL: Duration = Duration::from_secs(30);

pub(crate) struct PendingTerminalDelegation {
    pub(crate) info: TerminalDelegationInfo,
    source: PendingDelegationSource,
    created_at: Instant,
}

enum PendingDelegationSource {
    New {
        moved: MovedPane,
        public_pane_id: String,
        public_workspace_id: String,
        public_tab_id: String,
    },
    Existing {
        pane_id: PaneId,
        public_pane_id: String,
        public_workspace_id: String,
        public_tab_id: String,
        terminate_on_expire: bool,
    },
    Takeover {
        previous_delegation_id: String,
    },
}

pub(crate) struct DelegatedPane {
    pub(crate) info: TerminalDelegationInfo,
    pub(crate) moved: MovedPane,
    pub(crate) public_pane_id: String,
    pub(crate) public_workspace_id: String,
    pub(crate) public_tab_id: String,
}

pub(crate) struct CompletedTerminalDelegation {
    pub(crate) info: TerminalDelegationInfo,
    completed_at: Instant,
}

#[derive(Default)]
pub(crate) struct RemotePresentationState {
    pub(crate) pending: HashMap<String, PendingTerminalDelegation>,
    pub(crate) active: HashMap<String, DelegatedPane>,
    pub(crate) completed: HashMap<String, CompletedTerminalDelegation>,
}

impl App {
    pub(crate) fn prepare_delegated_terminal(
        &mut self,
        params: TerminalDelegateCreateParams,
    ) -> Result<TerminalDelegationInfo, ErrorBody> {
        validate_owner_route(&params.owner, &crate::remote_agent::local_peer_id())?;
        let extra_env = super::api::env::normalize_launch_env(params.env)
            .map_err(|(code, message)| ErrorBody { code, message })?;
        let cwd = params
            .cwd
            .map(PathBuf::from)
            .unwrap_or_else(|| self.resolve_new_terminal_cwd(None));
        let (rows, cols) = self.state.estimate_pane_size();
        let shell = self.state.default_shell.clone();
        let (mut workspace, mut terminal, runtime) =
            crate::workspace::Workspace::new_with_extra_env(
                cwd,
                rows,
                cols,
                self.state.pane_scrollback_limit_bytes,
                self.state.host_terminal_theme,
                crate::pane::PaneShellConfig::new(&shell, self.state.shell_mode),
                self.event_tx.clone(),
                self.render_notify.clone(),
                self.render_dirty.clone(),
                extra_env,
            )
            .map_err(|err| ErrorBody {
                code: "terminal_delegate_create_failed".into(),
                message: err.to_string(),
            })?;
        if let Some(label) = params.label.filter(|label| !label.is_empty()) {
            terminal.set_manual_label(label);
        }
        let pane_id = workspace.tabs[0].root_pane;
        let public_pane_id = crate::workspace::public_pane_id_for_number(&workspace.id, 1);
        let public_workspace_id = workspace.id.clone();
        let public_tab_id = crate::workspace::public_tab_id_for_number(&workspace.id, 1);
        let Some(taken) = workspace.take_pane_for_move(pane_id) else {
            runtime.shutdown();
            return Err(ErrorBody {
                code: "terminal_delegate_create_failed".into(),
                message: "new remote terminal could not be detached for delegation".into(),
            });
        };
        let terminal_id = terminal.id.clone();
        self.terminal_runtimes.insert(terminal_id.clone(), runtime);
        self.state.terminals.insert(terminal_id.clone(), terminal);
        let info = new_delegation_info(
            &terminal_id,
            &public_pane_id,
            params.owner,
            crate::remote_agent::local_peer_id(),
        );
        self.remote_presentations.pending.insert(
            info.delegation_id.clone(),
            PendingTerminalDelegation {
                info: info.clone(),
                source: PendingDelegationSource::New {
                    moved: taken.moved,
                    public_pane_id,
                    public_workspace_id,
                    public_tab_id,
                },
                created_at: Instant::now(),
            },
        );
        Ok(info)
    }

    pub(crate) fn prepare_existing_terminal_delegation(
        &mut self,
        params: TerminalDelegateClaimParams,
    ) -> Result<TerminalDelegationInfo, ErrorBody> {
        let origin_peer_id = crate::remote_agent::local_peer_id();
        validate_owner_route(&params.owner, &origin_peer_id)?;
        let aliased_pane_id = self
            .state
            .public_pane_id_aliases
            .get(&params.target)
            .copied();
        if let Some((delegation_id, delegated)) =
            self.remote_presentations
                .active
                .iter()
                .find(|(_, delegated)| {
                    delegated_matches_target(delegated, &params.target, aliased_pane_id)
                        || self
                            .state
                            .terminals
                            .get(&delegated.moved.pane_state.attached_terminal_id)
                            .is_some_and(|terminal| {
                                terminal.agent_name.as_deref() == Some(params.target.as_str())
                                    || terminal.effective_agent_label()
                                        == Some(params.target.as_str())
                            })
                })
        {
            if !params.takeover {
                return Err(ErrorBody {
                    code: "terminal_already_delegated".into(),
                    message: format!(
                        "terminal {} is already presented by {}; retry with --takeover",
                        delegated.info.terminal_id, delegated.info.owner.peer_id
                    ),
                });
            }
            let terminal_id = &delegated.moved.pane_state.attached_terminal_id;
            if self.terminal_has_downstream_presentation(terminal_id) {
                return Err(ErrorBody {
                    code: "nested_terminal_takeover_unsupported".into(),
                    message: "this pane currently owns a deeper remote presentation; hand that presentation back or close it before taking over the pane".into(),
                });
            }
            let info = new_delegation_info(
                terminal_id,
                &delegated.public_pane_id,
                params.owner,
                origin_peer_id,
            );
            self.remote_presentations.pending.insert(
                info.delegation_id.clone(),
                PendingTerminalDelegation {
                    info: info.clone(),
                    source: PendingDelegationSource::Takeover {
                        previous_delegation_id: delegation_id.clone(),
                    },
                    created_at: Instant::now(),
                },
            );
            return Ok(info);
        }

        let resolved = self
            .resolve_terminal_target(&params.target)
            .map_err(|_| ErrorBody {
                code: "terminal_not_found".into(),
                message: format!("terminal target {} not found", params.target),
            })?;
        let terminal_id = self.state.workspaces[resolved.ws_idx]
            .terminal_id(resolved.pane_id)
            .cloned()
            .ok_or_else(|| ErrorBody {
                code: "terminal_not_found".into(),
                message: format!("terminal target {} not found", params.target),
            })?;

        let public_pane_id = self
            .public_pane_id(resolved.ws_idx, resolved.pane_id)
            .ok_or_else(|| ErrorBody {
                code: "terminal_not_found".into(),
                message: format!("terminal target {} has no public pane id", params.target),
            })?;
        let public_workspace_id = self.public_workspace_id(resolved.ws_idx);
        let public_tab_id = self
            .public_tab_id(resolved.ws_idx, resolved.tab_idx)
            .ok_or_else(|| ErrorBody {
                code: "terminal_not_found".into(),
                message: format!("terminal target {} has no public tab id", params.target),
            })?;
        let info = new_delegation_info(&terminal_id, &public_pane_id, params.owner, origin_peer_id);
        self.remote_presentations.pending.insert(
            info.delegation_id.clone(),
            PendingTerminalDelegation {
                info: info.clone(),
                source: PendingDelegationSource::Existing {
                    pane_id: resolved.pane_id,
                    public_pane_id,
                    public_workspace_id,
                    public_tab_id,
                    terminate_on_expire: params.terminate_on_expire,
                },
                created_at: Instant::now(),
            },
        );
        Ok(info)
    }

    pub(crate) fn commit_terminal_delegation(
        &mut self,
        claim: &TerminalDelegationClaim,
        requested_terminal_id: &str,
    ) -> Result<TerminalId, String> {
        let Some(pending) = self
            .remote_presentations
            .pending
            .remove(&claim.delegation_id)
        else {
            return Err("terminal delegation was not found or has expired".into());
        };
        if pending.info.epoch != claim.epoch || pending.info.terminal_id != requested_terminal_id {
            self.remote_presentations
                .pending
                .insert(claim.delegation_id.clone(), pending);
            return Err("terminal delegation claim does not match the prepared terminal".into());
        }
        let source_available = match &pending.source {
            PendingDelegationSource::New { moved, .. } => {
                let terminal_id = &moved.pane_state.attached_terminal_id;
                self.state.terminals.contains_key(terminal_id)
                    && self.terminal_runtimes.get(terminal_id).is_some()
            }
            PendingDelegationSource::Existing { pane_id, .. } => self
                .state
                .workspaces
                .iter()
                .find_map(|workspace| workspace.pane_state(*pane_id))
                .is_some_and(|pane| {
                    self.state
                        .terminals
                        .contains_key(&pane.attached_terminal_id)
                        && self
                            .terminal_runtimes
                            .get(&pane.attached_terminal_id)
                            .is_some()
                }),
            PendingDelegationSource::Takeover {
                previous_delegation_id,
            } => self
                .remote_presentations
                .active
                .get(previous_delegation_id)
                .is_some_and(|delegated| delegated.info.terminal_id == requested_terminal_id),
        };
        if !source_available {
            self.fail_pending_delegation(pending);
            return Err("terminal delegation source is no longer running".into());
        }
        let takeover_has_downstream_presentation = match &pending.source {
            PendingDelegationSource::Takeover {
                previous_delegation_id,
            } => self
                .remote_presentations
                .active
                .get(previous_delegation_id)
                .is_some_and(|delegated| {
                    self.terminal_has_downstream_presentation(
                        &delegated.moved.pane_state.attached_terminal_id,
                    )
                }),
            PendingDelegationSource::New { .. } | PendingDelegationSource::Existing { .. } => false,
        };
        if takeover_has_downstream_presentation {
            self.fail_pending_delegation(pending);
            return Err("this pane started owning a deeper remote presentation before takeover committed; hand that presentation back or close it before retrying".into());
        }

        let (moved, public_pane_id, public_workspace_id, public_tab_id) = match pending.source {
            PendingDelegationSource::New {
                moved,
                public_pane_id,
                public_workspace_id,
                public_tab_id,
            } => (moved, public_pane_id, public_workspace_id, public_tab_id),
            PendingDelegationSource::Existing {
                pane_id,
                public_pane_id,
                public_workspace_id,
                public_tab_id,
                terminate_on_expire: _,
            } => {
                let Some(moved) = self.take_active_pane_for_delegation(pane_id) else {
                    return Err("source pane disappeared before delegation committed".into());
                };
                (moved, public_pane_id, public_workspace_id, public_tab_id)
            }
            PendingDelegationSource::Takeover {
                previous_delegation_id,
            } => {
                let Some(mut previous) = self
                    .remote_presentations
                    .active
                    .remove(&previous_delegation_id)
                else {
                    return Err("terminal presentation changed before takeover committed".into());
                };
                if previous.info.terminal_id != requested_terminal_id {
                    self.remote_presentations
                        .active
                        .insert(previous_delegation_id, previous);
                    return Err("takeover target no longer matches the prepared terminal".into());
                }
                previous.info.status = TerminalDelegationStatus::TakenOver;
                self.remote_presentations.completed.insert(
                    previous_delegation_id,
                    CompletedTerminalDelegation {
                        info: previous.info,
                        completed_at: Instant::now(),
                    },
                );
                (
                    previous.moved,
                    previous.public_pane_id,
                    previous.public_workspace_id,
                    previous.public_tab_id,
                )
            }
        };
        let terminal_id = moved.pane_state.attached_terminal_id.clone();
        let pane_id = moved.pane_id;
        let mut info = pending.info;
        info.status = TerminalDelegationStatus::Active;
        let activation = info.clone();
        self.remote_presentations.active.insert(
            claim.delegation_id.clone(),
            DelegatedPane {
                info,
                moved,
                public_pane_id,
                public_workspace_id,
                public_tab_id,
            },
        );
        self.state
            .delegated_terminal_ids
            .insert(pane_id, terminal_id.clone());
        self.state.mark_session_dirty();
        self.notify_previous_presentation_owner(&activation);
        Ok(terminal_id)
    }

    fn terminal_has_downstream_presentation(&self, terminal_id: &TerminalId) -> bool {
        self.remote_owner_presentations.contains_key(terminal_id)
            || self.ssh_shell_bridges.contains_key(terminal_id)
            || self.remote_terminal_bridges.contains_key(terminal_id)
            || self.pending_owner_operations.contains_key(terminal_id)
            || self
                .pending_owner_activations
                .values()
                .any(|pending| &pending.terminal_id == terminal_id)
    }

    fn notify_previous_presentation_owner(&self, info: &TerminalDelegationInfo) {
        let Some(previous_peer_id) = info.owner.route.iter().rev().nth(1) else {
            return;
        };
        let Some(peer) = self.state.peers.get(previous_peer_id).cloned() else {
            return;
        };
        let claim = TerminalDelegationClaim {
            delegation_id: info.delegation_id.clone(),
            epoch: info.epoch,
        };
        std::thread::spawn(move || {
            let request = crate::api::schema::Request {
                id: "peer:presentation:activate".into(),
                method: crate::api::schema::Method::PeerPresentationActivate(claim),
            };
            if let Err(message) = super::peer_agents::peer_request_value(&peer, &request) {
                tracing::debug!(peer = %peer.id, %message, "could not acknowledge remote presentation activation");
            }
        });
    }

    pub(crate) fn terminal_delegation_info(
        &self,
        delegation_id: &str,
        epoch: u64,
    ) -> Option<TerminalDelegationInfo> {
        let matches = |info: &TerminalDelegationInfo| info.epoch == epoch;
        self.remote_presentations
            .pending
            .get(delegation_id)
            .map(|pending| &pending.info)
            .or_else(|| {
                self.remote_presentations
                    .active
                    .get(delegation_id)
                    .map(|active| &active.info)
            })
            .or_else(|| {
                self.remote_presentations
                    .completed
                    .get(delegation_id)
                    .map(|completed| &completed.info)
            })
            .filter(|info| matches(info))
            .cloned()
    }

    pub(crate) fn handoff_delegated_pane(
        &mut self,
        public_pane_id: &str,
    ) -> Result<TerminalDelegationInfo, ErrorBody> {
        let Some(delegation_id) =
            self.remote_presentations
                .active
                .iter()
                .find_map(|(id, delegated)| {
                    (delegated.public_pane_id == public_pane_id
                        || self
                            .state
                            .public_pane_id_aliases
                            .get(public_pane_id)
                            .is_some_and(|pane_id| *pane_id == delegated.moved.pane_id))
                    .then(|| id.clone())
                })
        else {
            return Err(ErrorBody {
                code: "pane_not_remotely_presented".into(),
                message: "remote-handoff only works in the currently delegated remote pane".into(),
            });
        };
        let handoff_has_downstream_presentation = self
            .remote_presentations
            .active
            .get(&delegation_id)
            .is_some_and(|delegated| {
                self.terminal_has_downstream_presentation(
                    &delegated.moved.pane_state.attached_terminal_id,
                )
            });
        if handoff_has_downstream_presentation {
            return Err(ErrorBody {
                code: "nested_terminal_handoff_unsupported".into(),
                message: "this pane currently owns a deeper remote presentation; hand off or close the innermost pane first".into(),
            });
        }
        let Some(mut delegated) = self.remote_presentations.active.remove(&delegation_id) else {
            return Err(ErrorBody {
                code: "terminal_delegation_not_found".into(),
                message: "terminal delegation disappeared during handoff".into(),
            });
        };
        self.state
            .delegated_terminal_ids
            .remove(&delegated.moved.pane_id);
        let terminal_id = delegated.moved.pane_state.attached_terminal_id.clone();
        let identity_cwd = self
            .state
            .terminals
            .get(&terminal_id)
            .map(|terminal| terminal.cwd.clone())
            .unwrap_or_else(|| PathBuf::from("/"));
        let tab_label = self
            .state
            .terminals
            .get(&terminal_id)
            .and_then(|terminal| terminal.manual_label.clone());
        let pane_id = delegated.moved.pane_id;
        let workspace = crate::workspace::Workspace::from_existing_pane(
            None,
            tab_label,
            identity_cwd,
            delegated.moved,
            self.event_tx.clone(),
            self.render_notify.clone(),
            self.render_dirty.clone(),
        );
        self.state.workspaces.push(workspace);
        let ws_idx = self.state.workspaces.len() - 1;
        self.state
            .public_pane_id_aliases
            .insert(delegated.public_pane_id.clone(), pane_id);
        if self.state.active.is_none() {
            self.state.active = Some(ws_idx);
            self.state.selected = ws_idx;
        }
        delegated.info.status = TerminalDelegationStatus::HandedOff;
        let info = delegated.info.clone();
        self.fail_pending_takeovers_for(&delegation_id);
        self.remote_presentations.completed.insert(
            delegation_id,
            CompletedTerminalDelegation {
                info: delegated.info,
                completed_at: Instant::now(),
            },
        );
        self.state.mark_session_dirty();
        self.schedule_session_save();
        Ok(info)
    }

    pub(crate) fn terminate_terminal_delegation(
        &mut self,
        claim: &TerminalDelegationClaim,
    ) -> bool {
        if let Some(pending) = self
            .remote_presentations
            .pending
            .remove(&claim.delegation_id)
        {
            if pending.info.epoch != claim.epoch {
                self.remote_presentations
                    .pending
                    .insert(claim.delegation_id.clone(), pending);
                return false;
            }
            self.fail_pending_delegation(pending);
            return true;
        }
        let Some(mut delegated) = self
            .remote_presentations
            .active
            .remove(&claim.delegation_id)
        else {
            return false;
        };
        if delegated.info.epoch != claim.epoch {
            self.remote_presentations
                .active
                .insert(claim.delegation_id.clone(), delegated);
            return false;
        }
        let terminal_id = delegated.moved.pane_state.attached_terminal_id.clone();
        self.release_remote_terminal_bridge(&terminal_id);
        self.state
            .delegated_terminal_ids
            .remove(&delegated.moved.pane_id);
        self.state
            .pane_id_aliases
            .retain(|_, pane_id| *pane_id != delegated.moved.pane_id);
        self.state
            .public_pane_id_aliases
            .retain(|_, pane_id| *pane_id != delegated.moved.pane_id);
        delegated.info.status = TerminalDelegationStatus::Terminated;
        self.state.terminals.remove(&terminal_id);
        if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
            runtime.shutdown();
        }
        self.remote_presentations.completed.insert(
            claim.delegation_id.clone(),
            CompletedTerminalDelegation {
                info: delegated.info,
                completed_at: Instant::now(),
            },
        );
        self.fail_pending_takeovers_for(&claim.delegation_id);
        self.state.mark_session_dirty();
        true
    }

    pub(crate) fn active_remote_presentation_count(&self) -> usize {
        self.remote_presentations.active.len()
    }

    #[cfg(unix)]
    pub(crate) fn has_remote_presentation_activity(&self) -> bool {
        !self.remote_presentations.pending.is_empty()
            || !self.remote_presentations.active.is_empty()
            || !self.remote_owner_presentations.is_empty()
            || !self.pending_owner_activations.is_empty()
            || !self.pending_owner_operations.is_empty()
    }

    pub(crate) fn shutdown_remote_presentation_count(&self) -> usize {
        let mut terminal_ids = std::collections::HashSet::new();
        terminal_ids.extend(self.remote_owner_presentations.keys());
        terminal_ids.extend(
            self.remote_presentations
                .active
                .values()
                .map(|delegated| &delegated.moved.pane_state.attached_terminal_id),
        );
        terminal_ids.len()
    }

    pub(crate) fn terminate_all_remote_presentations(&mut self) -> usize {
        let claims = self
            .remote_presentations
            .active
            .values()
            .map(|delegated| TerminalDelegationClaim {
                delegation_id: delegated.info.delegation_id.clone(),
                epoch: delegated.info.epoch,
            })
            .collect::<Vec<_>>();
        let count = self.active_remote_presentation_count();
        for claim in claims {
            self.terminate_terminal_delegation(&claim);
        }

        let pending = std::mem::take(&mut self.remote_presentations.pending);
        for (_, pending) in pending {
            self.fail_pending_delegation(pending);
        }
        count
    }

    pub(crate) fn terminate_remote_presentations_for_route_peer(&mut self, peer_id: &str) -> usize {
        let claims = self
            .remote_presentations
            .active
            .values()
            .filter(|delegated| {
                delegated
                    .info
                    .owner
                    .route
                    .iter()
                    .any(|peer| peer == peer_id)
            })
            .map(|delegated| TerminalDelegationClaim {
                delegation_id: delegated.info.delegation_id.clone(),
                epoch: delegated.info.epoch,
            })
            .collect::<Vec<_>>();
        let pending_ids = self
            .remote_presentations
            .pending
            .iter()
            .filter(|(_, pending)| pending.info.owner.route.iter().any(|peer| peer == peer_id))
            .map(|(delegation_id, _)| delegation_id.clone())
            .collect::<Vec<_>>();
        let count = claims.len();
        for claim in claims {
            self.terminate_terminal_delegation(&claim);
        }
        for delegation_id in pending_ids {
            if let Some(pending) = self.remote_presentations.pending.remove(&delegation_id) {
                self.fail_pending_delegation(pending);
            }
        }
        count
    }

    pub(crate) fn terminal_is_remotely_presented(&self, terminal_id: &str) -> bool {
        self.remote_presentations
            .active
            .values()
            .any(|delegated| delegated.info.terminal_id == terminal_id)
    }

    pub(crate) fn delegated_pane_for_public_id(
        &self,
        public_pane_id: &str,
    ) -> Option<&DelegatedPane> {
        let aliased_pane_id = self
            .state
            .public_pane_id_aliases
            .get(public_pane_id)
            .copied();
        self.remote_presentations.active.values().find(|delegated| {
            delegated.public_pane_id == public_pane_id
                || aliased_pane_id == Some(delegated.moved.pane_id)
        })
    }

    pub(crate) fn delegated_agent_info_for_target(
        &self,
        target: &str,
    ) -> Option<crate::api::schema::AgentInfo> {
        let aliased_pane_id = self.state.public_pane_id_aliases.get(target).copied();
        let delegated = self
            .remote_presentations
            .active
            .values()
            .find(|delegated| {
                if delegated_matches_target(delegated, target, aliased_pane_id) {
                    return true;
                }
                self.state
                    .terminals
                    .get(&delegated.moved.pane_state.attached_terminal_id)
                    .is_some_and(|terminal| {
                        terminal.agent_name.as_deref() == Some(target)
                            || terminal.effective_agent_label() == Some(target)
                    })
            })?;
        let terminal_id = &delegated.moved.pane_state.attached_terminal_id;
        let terminal = self.state.terminals.get(terminal_id)?;
        if !terminal.is_agent_terminal() {
            return None;
        }
        let presentation = terminal.effective_presentation();
        let foreground_cwd = self
            .terminal_runtimes
            .get(terminal_id)
            .and_then(crate::terminal::TerminalRuntime::foreground_cwd)
            .map(|cwd| cwd.display().to_string());
        Some(crate::api::schema::AgentInfo {
            terminal_id: terminal_id.to_string(),
            peer: None,
            qualified_target: None,
            presentation: Some(crate::api::schema::AgentPresentationInfo {
                origin_peer_id: delegated.info.origin_peer_id.clone(),
                owner_peer_id: delegated.info.owner.peer_id.clone(),
                route: delegated.info.owner.route.clone(),
            }),
            name: terminal.agent_name.clone(),
            agent: terminal.effective_agent_label().map(str::to_string),
            title: presentation.title,
            display_agent: presentation.display_agent,
            agent_status: super::api_helpers::pane_agent_status(
                terminal.state,
                delegated.moved.pane_state.seen,
            ),
            screen_detection_skipped: terminal.full_lifecycle_hook_authority_active(),
            custom_status: presentation.custom_status,
            state_labels: presentation.state_labels,
            agent_session: super::creation::terminal_agent_session_info(terminal),
            transport: terminal.remote_agent_transport.clone(),
            mirror_of_terminal_id: None,
            attach: None,
            workspace_id: delegated.public_workspace_id.clone(),
            tab_id: delegated.public_tab_id.clone(),
            pane_id: delegated.public_pane_id.clone(),
            focused: false,
            cwd: Some(terminal.cwd.display().to_string()),
            foreground_cwd,
            revision: terminal.revision,
        })
    }

    pub(crate) fn resolve_internal_report_pane(&self, pane_id: &str) -> Option<PaneId> {
        self.parse_pane_id(pane_id)
            .map(|(_, pane_id)| pane_id)
            .or_else(|| {
                self.delegated_pane_for_public_id(pane_id)
                    .map(|delegated| delegated.moved.pane_id)
            })
    }

    pub(crate) fn expire_remote_presentations(&mut self, now: Instant) -> bool {
        let pending_ids: Vec<String> = self
            .remote_presentations
            .pending
            .iter()
            .filter_map(|(id, pending)| {
                if now.duration_since(pending.created_at) >= PENDING_DELEGATION_TTL {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        let mut changed = false;
        for id in pending_ids {
            if let Some(pending) = self.remote_presentations.pending.remove(&id) {
                self.fail_pending_delegation_at(pending, now);
                changed = true;
            }
        }
        self.remote_presentations.completed.retain(|_, completed| {
            now.duration_since(completed.completed_at) < COMPLETED_DELEGATION_TTL
        });
        changed
    }

    pub(crate) fn handle_pending_delegation_pane_exit(&mut self, pane_id: PaneId) -> bool {
        let Some(delegation_id) =
            self.remote_presentations
                .pending
                .iter()
                .find_map(|(delegation_id, pending)| match &pending.source {
                    PendingDelegationSource::New { moved, .. } if moved.pane_id == pane_id => {
                        Some(delegation_id.clone())
                    }
                    PendingDelegationSource::Existing {
                        pane_id: pending_pane_id,
                        ..
                    } if *pending_pane_id == pane_id => Some(delegation_id.clone()),
                    _ => None,
                })
        else {
            return false;
        };
        let Some(pending) = self.remote_presentations.pending.remove(&delegation_id) else {
            return false;
        };
        let was_hidden = matches!(&pending.source, PendingDelegationSource::New { .. });
        self.fail_pending_delegation(pending);
        was_hidden
    }

    fn fail_pending_takeovers_for(&mut self, previous_delegation_id: &str) {
        let pending_ids = self
            .remote_presentations
            .pending
            .iter()
            .filter_map(|(delegation_id, pending)| match &pending.source {
                PendingDelegationSource::Takeover {
                    previous_delegation_id: candidate,
                } if candidate == previous_delegation_id => Some(delegation_id.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        for delegation_id in pending_ids {
            if let Some(pending) = self.remote_presentations.pending.remove(&delegation_id) {
                self.fail_pending_delegation(pending);
            }
        }
    }

    fn fail_pending_delegation(&mut self, pending: PendingTerminalDelegation) {
        self.fail_pending_delegation_at(pending, Instant::now());
    }

    fn fail_pending_delegation_at(
        &mut self,
        mut pending: PendingTerminalDelegation,
        completed_at: Instant,
    ) {
        match pending.source {
            PendingDelegationSource::New { moved, .. } => {
                let terminal_id = moved.pane_state.attached_terminal_id;
                self.state.terminals.remove(&terminal_id);
                if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
                    runtime.shutdown();
                }
            }
            PendingDelegationSource::Existing {
                pane_id,
                terminate_on_expire: true,
                ..
            } => {
                let terminal_id = self.state.workspaces.iter().find_map(|workspace| {
                    workspace
                        .pane_state(pane_id)
                        .map(|pane| pane.attached_terminal_id.clone())
                });
                if let Some(terminal_id) = terminal_id {
                    if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
                        runtime.shutdown();
                    }
                }
            }
            PendingDelegationSource::Existing { .. } | PendingDelegationSource::Takeover { .. } => {
            }
        }
        pending.info.status = TerminalDelegationStatus::Failed;
        self.remote_presentations.completed.insert(
            pending.info.delegation_id.clone(),
            CompletedTerminalDelegation {
                info: pending.info,
                completed_at,
            },
        );
        self.state.mark_session_dirty();
    }

    fn take_active_pane_for_delegation(&mut self, pane_id: PaneId) -> Option<MovedPane> {
        let ws_idx = self
            .state
            .workspaces
            .iter()
            .position(|workspace| workspace.pane_state(pane_id).is_some())?;
        let taken = self.state.workspaces[ws_idx].take_pane_for_move(pane_id)?;
        self.clear_hidden_pane_presentation_state(ws_idx, pane_id);
        self.state.workspaces[ws_idx].unregister_moved_pane(pane_id);
        if taken.workspace_empty {
            self.state.workspaces.remove(ws_idx);
            if self.state.workspaces.is_empty() {
                self.state.active = None;
                self.state.selected = 0;
                if self.state.mode == crate::app::state::Mode::Terminal {
                    self.state.mode = crate::app::state::Mode::Navigate;
                }
            } else {
                if let Some(active) = self.state.active {
                    self.state.active = Some(if active == ws_idx {
                        ws_idx.min(self.state.workspaces.len() - 1)
                    } else if active > ws_idx {
                        active - 1
                    } else {
                        active
                    });
                }
                self.state.selected = if self.state.selected == ws_idx {
                    ws_idx.min(self.state.workspaces.len() - 1)
                } else if self.state.selected > ws_idx {
                    self.state.selected - 1
                } else {
                    self.state.selected
                };
            }
        }
        self.last_focus = self.state.active.and_then(|idx| {
            self.state
                .workspaces
                .get(idx)
                .and_then(|workspace| workspace.focused_pane_id().map(|pane| (idx, pane)))
        });
        Some(taken.moved)
    }

    fn clear_hidden_pane_presentation_state(&mut self, ws_idx: usize, pane_id: PaneId) {
        let workspace_id = self.state.workspaces[ws_idx].id.clone();
        self.state.pending_agent_notifications.remove(&pane_id);
        if self
            .state
            .previous_pane_focus
            .as_ref()
            .is_some_and(|focus| focus.pane_id == pane_id)
        {
            self.state.previous_pane_focus = None;
        }
        if self.state.rename_pane_target == Some(pane_id) {
            self.state.rename_pane_target = None;
        }
        if self
            .state
            .selection
            .as_ref()
            .is_some_and(|selection| selection.pane_id == pane_id)
        {
            self.state.selection = None;
            self.state.selection_autoscroll = None;
        }
        if self
            .state
            .copy_mode
            .as_ref()
            .is_some_and(|copy_mode| copy_mode.pane_id == pane_id)
        {
            self.state.copy_mode = None;
        }
        if self
            .state
            .toast
            .as_ref()
            .and_then(|toast| toast.target.as_ref())
            .is_some_and(|target| target.workspace_id == workspace_id && target.pane_id == pane_id)
        {
            self.state.toast = None;
        }
        self.state.context_menu = None;
        self.state.drag = None;
        self.state.workspace_press = None;
        self.state.tab_press = None;
    }
}

fn delegated_matches_target(
    delegated: &DelegatedPane,
    target: &str,
    aliased_pane_id: Option<PaneId>,
) -> bool {
    delegated.info.terminal_id == target
        || delegated.info.pane_id == target
        || delegated.public_pane_id == target
        || aliased_pane_id == Some(delegated.moved.pane_id)
}

fn validate_owner_route(
    owner: &crate::api::schema::TerminalPresentationOwner,
    origin_peer_id: &str,
) -> Result<(), ErrorBody> {
    if owner.peer_id.is_empty() || owner.pane_id.is_empty() {
        return Err(ErrorBody {
            code: "invalid_terminal_presentation_owner".into(),
            message: "terminal presentation owner requires non-empty peer and pane ids".into(),
        });
    }
    if owner.route.first() != Some(&owner.peer_id) {
        return Err(ErrorBody {
            code: "invalid_terminal_presentation_route".into(),
            message: "terminal presentation route must begin with its owner peer id".into(),
        });
    }
    let mut unique = std::collections::HashSet::new();
    if owner
        .route
        .iter()
        .any(|peer_id| !unique.insert(peer_id.as_str()))
    {
        return Err(ErrorBody {
            code: "invalid_terminal_presentation_route".into(),
            message: "terminal presentation route must not repeat a host".into(),
        });
    }
    if owner.route.len() >= 32 {
        return Err(ErrorBody {
            code: "terminal_presentation_route_too_deep".into(),
            message: "terminal presentation route exceeds the 32-host safety limit".into(),
        });
    }
    if owner.route.iter().any(|peer_id| peer_id == origin_peer_id) {
        return Err(ErrorBody {
            code: "terminal_presentation_cycle".into(),
            message: format!(
                "terminal presentation route would revisit Herdr peer {origin_peer_id}"
            ),
        });
    }
    Ok(())
}

fn new_delegation_info(
    terminal_id: &TerminalId,
    public_pane_id: &str,
    mut owner: crate::api::schema::TerminalPresentationOwner,
    origin_peer_id: String,
) -> TerminalDelegationInfo {
    static NEXT_DELEGATION: AtomicU64 = AtomicU64::new(1);
    let sequence = NEXT_DELEGATION.fetch_add(1, Ordering::Relaxed);
    if owner.route.last() != Some(&origin_peer_id) {
        owner.route.push(origin_peer_id.clone());
    }
    TerminalDelegationInfo {
        delegation_id: format!(
            "delegation-{}-{sequence:x}",
            terminal_id.to_string().trim_start_matches("term_")
        ),
        epoch: 1,
        terminal_id: terminal_id.to_string(),
        pane_id: public_pane_id.to_string(),
        origin_peer_id,
        owner,
        status: TerminalDelegationStatus::Pending,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{TerminalDelegationStatus, TerminalPresentationOwner};
    use crate::workspace::Workspace;

    fn test_app() -> (App, String, String) {
        static TEST_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> =
            std::sync::OnceLock::new();
        let runtime = TEST_RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime")
        });
        let _runtime_guard = runtime.enter();
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        let workspace = Workspace::test_new("delegation");
        let pane_id = workspace.tabs[0].root_pane;
        let public_pane_id = crate::workspace::public_pane_id_for_number(&workspace.id, 1);
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        let terminal_key = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        app.terminal_runtimes.insert(
            terminal_key,
            crate::terminal::TerminalRuntime::test_with_screen_bytes(80, 24, b""),
        );
        app.state.active = Some(0);
        app.state.selected = 0;
        app.last_focus = Some((0, pane_id));
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .to_string();
        app.state
            .terminals
            .values_mut()
            .next()
            .expect("terminal state")
            .set_agent_name("remote-agent".into());
        (app, terminal_id, public_pane_id)
    }

    fn owner(peer_id: &str, pane_id: &str, route: &[&str]) -> TerminalPresentationOwner {
        TerminalPresentationOwner {
            peer_id: peer_id.into(),
            pane_id: pane_id.into(),
            route: route.iter().map(|peer| (*peer).to_string()).collect(),
        }
    }

    fn prepare_existing(
        app: &mut App,
        terminal_id: &str,
        owner: TerminalPresentationOwner,
        takeover: bool,
    ) -> TerminalDelegationInfo {
        app.prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
            target: terminal_id.into(),
            owner,
            takeover,
            terminate_on_expire: false,
        })
        .expect("prepare delegation")
    }

    fn claim(info: &TerminalDelegationInfo) -> TerminalDelegationClaim {
        TerminalDelegationClaim {
            delegation_id: info.delegation_id.clone(),
            epoch: info.epoch,
        }
    }

    #[test]
    fn delegation_hides_then_handoff_rehomes_the_same_pane() {
        let (mut app, terminal_id, public_pane_id) = test_app();
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "w1:p1", &["owner-a"]),
            false,
        );

        assert_eq!(
            app.commit_terminal_delegation(&claim(&prepared), &terminal_id)
                .expect("commit delegation")
                .to_string(),
            terminal_id
        );
        assert!(app.state.workspaces.is_empty());
        assert_eq!(app.active_remote_presentation_count(), 1);
        assert_eq!(
            app.delegated_agent_info_for_target(&public_pane_id)
                .expect("hidden agent metadata")
                .presentation
                .expect("presentation")
                .owner_peer_id,
            "owner-a"
        );
        app.state.assert_invariants_for_test();

        let handed_off = app
            .handoff_delegated_pane(&public_pane_id)
            .expect("handoff");
        assert_eq!(handed_off.status, TerminalDelegationStatus::HandedOff);
        assert_eq!(app.active_remote_presentation_count(), 0);
        assert_eq!(app.state.workspaces.len(), 1);
        assert_eq!(app.state.active, Some(0));
        assert_eq!(
            app.state.workspaces[0]
                .terminal_id(app.state.workspaces[0].tabs[0].root_pane)
                .expect("rehomed terminal")
                .to_string(),
            terminal_id
        );
        app.state.assert_invariants_for_test();
    }

    #[test]
    fn takeover_replaces_the_owner_without_duplicating_the_pane() {
        let (mut app, terminal_id, _) = test_app();
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("first commit");

        let error = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: "remote-agent".into(),
                owner: owner("owner-b", "pane-b", &["owner-b"]),
                takeover: false,
                terminate_on_expire: false,
            })
            .expect_err("implicit takeover must fail");
        assert_eq!(error.code, "terminal_already_delegated");

        let second = prepare_existing(
            &mut app,
            "remote-agent",
            owner("owner-b", "pane-b", &["owner-b"]),
            true,
        );
        app.commit_terminal_delegation(&claim(&second), &terminal_id)
            .expect("takeover commit");

        assert_eq!(app.active_remote_presentation_count(), 1);
        assert_eq!(
            app.terminal_delegation_info(&first.delegation_id, first.epoch)
                .expect("old status")
                .status,
            TerminalDelegationStatus::TakenOver
        );
        assert_eq!(
            app.terminal_delegation_info(&second.delegation_id, second.epoch)
                .expect("new status")
                .owner
                .peer_id,
            "owner-b"
        );
        app.state.assert_invariants_for_test();
    }

    #[test]
    fn owner_route_cycle_is_rejected_before_the_pane_moves() {
        let (mut app, terminal_id, _) = test_app();
        let local_peer_id = crate::remote_agent::local_peer_id();
        let error = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: terminal_id,
                owner: TerminalPresentationOwner {
                    peer_id: "owner-a".into(),
                    pane_id: "pane-a".into(),
                    route: vec!["owner-a".into(), local_peer_id],
                },
                takeover: false,
                terminate_on_expire: false,
            })
            .expect_err("cycle must fail");

        assert_eq!(error.code, "terminal_presentation_cycle");
        assert_eq!(app.state.workspaces.len(), 1);
        assert_eq!(app.active_remote_presentation_count(), 0);
        app.state.assert_invariants_for_test();
    }

    #[test]
    fn owner_route_must_start_with_owner_and_have_unique_hops() {
        let (mut app, terminal_id, _) = test_app();
        let missing_owner = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: terminal_id.clone(),
                owner: owner("owner-a", "pane-a", &["host-b"]),
                takeover: false,
                terminate_on_expire: false,
            })
            .expect_err("route must begin with owner");
        assert_eq!(missing_owner.code, "invalid_terminal_presentation_route");

        let duplicate = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: terminal_id,
                owner: owner("owner-a", "pane-a", &["owner-a", "host-b", "host-b"]),
                takeover: false,
                terminate_on_expire: false,
            })
            .expect_err("route hops must be unique");
        assert_eq!(duplicate.code, "invalid_terminal_presentation_route");
    }

    #[test]
    fn pending_source_exit_invalidates_claim_before_commit() {
        let (mut app, terminal_id, _) = test_app();
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;

        assert!(!app.handle_pending_delegation_pane_exit(pane_id));
        assert_eq!(
            app.terminal_delegation_info(&prepared.delegation_id, prepared.epoch)
                .expect("failed status")
                .status,
            TerminalDelegationStatus::Failed
        );
        assert!(app
            .commit_terminal_delegation(&claim(&prepared), &terminal_id)
            .is_err());
        assert_eq!(app.state.workspaces.len(), 1);
    }

    #[test]
    fn pending_existing_claim_rejects_a_pane_whose_runtime_already_stopped() {
        let (mut app, terminal_id, _) = test_app();
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        app.terminal_runtimes.remove(&terminal_key);

        let error = app
            .commit_terminal_delegation(&claim(&prepared), &terminal_id)
            .expect_err("dead runtime must not activate");

        assert!(error.contains("no longer running"));
        assert_eq!(
            app.terminal_delegation_info(&prepared.delegation_id, prepared.epoch)
                .expect("failed status")
                .status,
            TerminalDelegationStatus::Failed
        );
    }

    #[test]
    fn handoff_accepts_the_immutable_pane_id_from_before_a_workspace_move() {
        let (mut app, terminal_id, _) = test_app();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let old_public_id = "old-workspace:pane".to_string();
        app.state
            .public_pane_id_aliases
            .insert(old_public_id.clone(), pane_id);
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&prepared), &terminal_id)
            .expect("commit delegation");

        assert!(app
            .state
            .public_pane_id_aliases
            .contains_key(&old_public_id));
        assert!(app.delegated_pane_for_public_id(&old_public_id).is_some());
        app.handoff_delegated_pane(&old_public_id)
            .expect("handoff through historical pane id");
        assert_eq!(app.state.workspaces.len(), 1);
        app.state.assert_invariants_for_test();
    }

    #[test]
    fn takeover_accepts_a_historical_public_pane_alias() {
        let (mut app, terminal_id, _) = test_app();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let historical_pane_id = "old-workspace:pane".to_string();
        app.state
            .public_pane_id_aliases
            .insert(historical_pane_id.clone(), pane_id);
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("first commit");

        let second = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: historical_pane_id,
                owner: owner("owner-b", "pane-b", &["owner-b"]),
                takeover: true,
                terminate_on_expire: false,
            })
            .expect("prepare takeover through historical pane id");
        app.commit_terminal_delegation(&claim(&second), &terminal_id)
            .expect("commit takeover");

        assert_eq!(app.active_remote_presentation_count(), 1);
        assert_eq!(
            app.terminal_delegation_info(&first.delegation_id, first.epoch)
                .expect("previous status")
                .status,
            TerminalDelegationStatus::TakenOver
        );
        assert_eq!(
            app.terminal_delegation_info(&second.delegation_id, second.epoch)
                .expect("new status")
                .status,
            TerminalDelegationStatus::Active
        );
        app.handoff_delegated_pane("old-workspace:pane")
            .expect("handoff through historical pane id after takeover");
        app.state.assert_invariants_for_test();
    }

    #[test]
    fn takeover_rejects_a_pane_that_owns_a_deeper_remote_presentation() {
        let (mut app, terminal_id, _) = test_app();
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("first commit");
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        app.remote_owner_presentations.insert(
            terminal_key,
            TerminalDelegationInfo {
                delegation_id: "deeper".into(),
                epoch: 1,
                terminal_id: "remote-terminal".into(),
                pane_id: "remote-pane".into(),
                origin_peer_id: "origin-c".into(),
                owner: owner("owner-a", "pane-a", &["owner-a"]),
                status: TerminalDelegationStatus::Active,
            },
        );

        let error = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: terminal_id,
                owner: owner("owner-d", "pane-d", &["owner-d"]),
                takeover: true,
                terminate_on_expire: false,
            })
            .expect_err("nested takeover must be rejected safely");

        assert_eq!(error.code, "nested_terminal_takeover_unsupported");
        assert_eq!(app.active_remote_presentation_count(), 1);
    }

    #[test]
    fn takeover_rejects_a_pane_with_a_pending_deeper_presentation() {
        let (mut app, terminal_id, _) = test_app();
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("first commit");
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        app.pending_owner_activations.insert(
            "downstream".into(),
            crate::app::PendingOwnerActivation {
                terminal_id: terminal_key,
                peer_id: "peer-c".into(),
                connection_id: "connection-c".into(),
                peer: crate::api::schema::PeerInfo {
                    id: "peer-c".into(),
                    label: "peer-c".into(),
                    status: crate::api::schema::PeerStatus::Connected,
                    transport: crate::api::schema::PeerTransportInfo::Ssh {
                        target: "peer-c".into(),
                        ssh_args: Vec::new(),
                        managed_control_path: None,
                        session: None,
                    },
                },
                delegation: TerminalDelegationInfo {
                    delegation_id: "downstream".into(),
                    epoch: 1,
                    terminal_id: "remote-terminal".into(),
                    pane_id: "remote-pane".into(),
                    origin_peer_id: "peer-c".into(),
                    owner: owner("owner-a", "pane-a", &["owner-a"]),
                    status: TerminalDelegationStatus::Pending,
                },
                release_bridge_on_owner_close: false,
                cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        let error = app
            .prepare_existing_terminal_delegation(TerminalDelegateClaimParams {
                target: terminal_id,
                owner: owner("owner-d", "pane-d", &["owner-d"]),
                takeover: true,
                terminate_on_expire: false,
            })
            .expect_err("pending nested takeover must be rejected safely");

        assert_eq!(error.code, "nested_terminal_takeover_unsupported");
    }

    #[test]
    fn takeover_commit_rechecks_for_a_new_downstream_presentation() {
        let (mut app, terminal_id, _) = test_app();
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("first commit");
        let second = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-b", "pane-b", &["owner-b"]),
            true,
        );
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        let mut downstream = first.clone();
        downstream.delegation_id = "downstream".into();
        downstream.terminal_id = "remote-terminal".into();
        app.remote_owner_presentations
            .insert(terminal_key, downstream);

        let error = app
            .commit_terminal_delegation(&claim(&second), &terminal_id)
            .expect_err("new downstream ownership must block takeover commit");

        assert!(error.contains("started owning a deeper remote presentation"));
        assert_eq!(app.active_remote_presentation_count(), 1);
        assert_eq!(
            app.terminal_delegation_info(&first.delegation_id, first.epoch)
                .expect("previous owner remains active")
                .status,
            TerminalDelegationStatus::Active
        );
        assert_eq!(
            app.terminal_delegation_info(&second.delegation_id, second.epoch)
                .expect("failed takeover status")
                .status,
            TerminalDelegationStatus::Failed
        );
    }

    #[test]
    fn takeover_commit_rejects_an_in_flight_downstream_owner_operation() {
        let (mut app, terminal_id, _) = test_app();
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("first commit");
        let second = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-b", "pane-b", &["owner-b"]),
            true,
        );
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        app.pending_owner_operations.insert(terminal_key, 1);

        let error = app
            .commit_terminal_delegation(&claim(&second), &terminal_id)
            .expect_err("in-flight downstream ownership must block takeover commit");

        assert!(error.contains("started owning a deeper remote presentation"));
        assert_eq!(app.active_remote_presentation_count(), 1);
        assert_eq!(
            app.terminal_delegation_info(&first.delegation_id, first.epoch)
                .expect("previous owner remains active")
                .status,
            TerminalDelegationStatus::Active
        );
    }

    #[test]
    fn handoff_rejects_an_in_flight_downstream_owner_operation() {
        let (mut app, terminal_id, public_pane_id) = test_app();
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&prepared), &terminal_id)
            .expect("commit delegation");
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        app.pending_owner_operations.insert(terminal_key, 1);

        let error = app
            .handoff_delegated_pane(&public_pane_id)
            .expect_err("in-flight downstream ownership must block handoff");

        assert_eq!(error.code, "nested_terminal_handoff_unsupported");
        assert_eq!(app.active_remote_presentation_count(), 1);
        assert!(app.state.workspaces.is_empty());
    }

    #[test]
    fn terminating_a_hidden_owner_clears_its_deeper_bridge_maps() {
        let (mut app, terminal_id, _) = test_app();
        let first = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&first), &terminal_id)
            .expect("commit delegation");
        let terminal_key = app
            .state
            .terminals
            .keys()
            .find(|candidate| candidate.to_string() == terminal_id)
            .expect("terminal key")
            .clone();
        app.ssh_shell_bridges.insert(
            terminal_key.clone(),
            vec![("peer-c".into(), "connection-c".into())],
        );
        app.remote_owner_presentations
            .insert(terminal_key.clone(), first.clone());
        let pending_cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        app.pending_owner_activations.insert(
            "pending-downstream".into(),
            crate::app::PendingOwnerActivation {
                terminal_id: terminal_key.clone(),
                peer_id: "peer-d".into(),
                connection_id: "connection-d".into(),
                peer: crate::api::schema::PeerInfo {
                    id: "peer-d".into(),
                    label: "peer-d".into(),
                    status: crate::api::schema::PeerStatus::Connected,
                    transport: crate::api::schema::PeerTransportInfo::Ssh {
                        target: "peer-d".into(),
                        ssh_args: Vec::new(),
                        managed_control_path: None,
                        session: None,
                    },
                },
                delegation: TerminalDelegationInfo {
                    delegation_id: "pending-downstream".into(),
                    epoch: 1,
                    terminal_id: "remote-terminal-d".into(),
                    pane_id: "remote-pane-d".into(),
                    origin_peer_id: "peer-d".into(),
                    owner: owner("owner-a", "pane-a", &["owner-a"]),
                    status: TerminalDelegationStatus::Pending,
                },
                release_bridge_on_owner_close: false,
                cancelled: std::sync::Arc::clone(&pending_cancelled),
            },
        );

        assert!(app.terminate_terminal_delegation(&claim(&first)));

        assert!(!app.ssh_shell_bridges.contains_key(&terminal_key));
        assert!(!app.remote_owner_presentations.contains_key(&terminal_key));
        assert!(app.pending_owner_activations.is_empty());
        assert!(pending_cancelled.load(std::sync::atomic::Ordering::Acquire));
        assert!(!app.state.terminals.contains_key(&terminal_key));
    }

    #[test]
    fn losing_any_route_peer_terminates_the_hidden_presentation() {
        let (mut app, terminal_id, _) = test_app();
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a", "middle-b"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&prepared), &terminal_id)
            .expect("commit delegation");

        assert_eq!(
            app.terminate_remote_presentations_for_route_peer("middle-b"),
            1
        );
        assert_eq!(app.active_remote_presentation_count(), 0);
        assert_eq!(
            app.terminal_delegation_info(&prepared.delegation_id, prepared.epoch)
                .expect("terminal status")
                .status,
            TerminalDelegationStatus::Terminated
        );
    }

    #[test]
    fn pending_new_shell_exit_removes_hidden_terminal_state() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let runtime_guard = runtime.enter();
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        let prepared = app
            .prepare_delegated_terminal(TerminalDelegateCreateParams {
                cwd: Some(std::env::temp_dir().display().to_string()),
                label: Some("pending".into()),
                env: std::collections::HashMap::new(),
                owner: owner("owner-a", "pane-a", &["owner-a"]),
            })
            .expect("prepare fresh shell");
        let pane_id = match &app
            .remote_presentations
            .pending
            .get(&prepared.delegation_id)
            .expect("pending")
            .source
        {
            PendingDelegationSource::New { moved, .. } => moved.pane_id,
            _ => panic!("expected fresh pending shell"),
        };

        assert!(app.handle_pending_delegation_pane_exit(pane_id));
        assert!(!app
            .state
            .terminals
            .values()
            .any(|terminal| terminal.id.to_string() == prepared.terminal_id));
        assert_eq!(
            app.terminal_delegation_info(&prepared.delegation_id, prepared.epoch)
                .expect("failed status")
                .status,
            TerminalDelegationStatus::Failed
        );
        drop(app);
        drop(runtime_guard);
        runtime.shutdown_timeout(Duration::from_millis(100));
    }

    #[test]
    fn server_stop_reports_before_shutdown_terminates_presentations() {
        let (mut app, terminal_id, _) = test_app();
        let prepared = prepare_existing(
            &mut app,
            &terminal_id,
            owner("owner-a", "pane-a", &["owner-a"]),
            false,
        );
        app.commit_terminal_delegation(&claim(&prepared), &terminal_id)
            .expect("commit delegation");

        let response = app.handle_api_request(crate::api::schema::Request {
            id: "stop".into(),
            method: crate::api::schema::Method::ServerStop(
                crate::api::schema::EmptyParams::default(),
            ),
        });
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["result"]["type"], "ok");
        assert_eq!(response["result"]["terminated_remote_presentations"], 1);
        assert_eq!(app.active_remote_presentation_count(), 1);

        assert_eq!(app.terminate_all_remote_presentations(), 1);
        assert_eq!(app.active_remote_presentation_count(), 0);
        assert!(!app
            .state
            .terminals
            .values()
            .any(|terminal| terminal.id.to_string() == terminal_id));
    }

    #[test]
    fn server_stop_reports_owner_side_remote_presentations() {
        let (mut app, _, _) = test_app();
        let terminal_id = app.state.terminals.keys().next().expect("terminal").clone();
        let delegation = new_delegation_info(
            &terminal_id,
            "remote:p1",
            owner("owner-a", "pane-a", &["owner-a"]),
            "origin-b".into(),
        );
        app.remote_owner_presentations
            .insert(terminal_id, delegation);

        let response = app.handle_api_request(crate::api::schema::Request {
            id: "stop".into(),
            method: crate::api::schema::Method::ServerStop(
                crate::api::schema::EmptyParams::default(),
            ),
        });
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["result"]["type"], "ok");
        assert_eq!(response["result"]["terminated_remote_presentations"], 1);
    }
}
