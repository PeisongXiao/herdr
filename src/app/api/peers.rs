use std::time::{Duration, Instant};

use crate::api::schema::{
    ErrorBody, Method, PeerConnectSshParams, PeerDisconnectSshParams, PeerInfo,
    PeerKeepaliveSshParams, PeerRegisterParams, PeerStatus, PeerTarget, PeerTransportInfo,
    PingParams, Request, ResponseResult,
};
use crate::app::App;

use super::responses::{encode_error, encode_error_body, encode_success};

const SSH_SHELL_BRIDGE_LEASE: Duration = Duration::from_secs(15);
const INCOMING_PEER_LEASE: Duration = Duration::from_secs(15);
const REMOTE_OWNER_LEASE: Duration = Duration::from_secs(15);

impl App {
    pub(super) fn resolve_ssh_owner_terminal(
        &self,
        owner_pane_id: Option<&str>,
    ) -> Result<Option<crate::terminal::TerminalId>, ErrorBody> {
        let Some(owner_pane_id) = owner_pane_id else {
            return Ok(None);
        };
        if let Some(delegated) = self.delegated_pane_for_public_id(owner_pane_id) {
            return Ok(Some(
                delegated.moved.pane_state.attached_terminal_id.clone(),
            ));
        }
        let Some((ws_idx, pane_id)) = self.parse_pane_id(owner_pane_id) else {
            return Err(ErrorBody {
                code: "invalid_ssh_owner".into(),
                message: format!("SSH owner pane {owner_pane_id} was not found"),
            });
        };
        self.state
            .workspaces
            .get(ws_idx)
            .and_then(|workspace| workspace.terminal_id(pane_id))
            .cloned()
            .map(Some)
            .ok_or_else(|| ErrorBody {
                code: "invalid_ssh_owner".into(),
                message: format!("SSH owner pane {owner_pane_id} has no terminal"),
            })
    }

    pub(super) fn begin_peer_connect_ssh(
        &mut self,
        id: String,
        mut params: PeerConnectSshParams,
        respond_to: std::sync::mpsc::Sender<String>,
    ) {
        let owner_terminal_id =
            match self.resolve_ssh_owner_terminal(params.owner_pane_id.as_deref()) {
                Ok(owner_terminal_id) => owner_terminal_id,
                Err(error) => {
                    let _ = respond_to.send(encode_error_body(id, error));
                    return;
                }
            };
        params.owner = owner_terminal_id
            .as_ref()
            .and_then(|terminal_id| {
                self.remote_presentations
                    .active
                    .values()
                    .find(|delegated| {
                        &delegated.moved.pane_state.attached_terminal_id == terminal_id
                    })
                    .map(|delegated| delegated.info.owner.clone())
            })
            .or_else(|| {
                params.owner_pane_id.as_ref().map(|pane_id| {
                    let peer_id = crate::remote_agent::local_peer_id();
                    crate::api::schema::TerminalPresentationOwner {
                        peer_id: peer_id.clone(),
                        pane_id: pane_id.clone(),
                        route: vec![peer_id],
                    }
                })
            });
        let peer_id = crate::remote_agent::peer_id_for_connect_params(&params);
        let generation = self
            .peer_lifecycle_generations
            .get(&peer_id)
            .copied()
            .unwrap_or_default();
        if !self.try_begin_remote_api_job(&id, &respond_to) {
            return;
        }
        self.begin_pending_owner_operation(owner_terminal_id.as_ref());
        let event_tx = self.event_tx.clone();
        let worker_id = id.clone();
        let worker_owner_terminal_id = owner_terminal_id.clone();
        let worker_respond_to = respond_to.clone();
        let spawn = std::thread::Builder::new()
            .name("herdr-peer-connect".into())
            .spawn(move || {
                let result =
                    crate::remote_agent::connect_shell(&params).map_err(|err| err.to_string());
                let event = crate::events::AppEvent::PeerConnectSshFinished(Box::new(
                    crate::events::PeerConnectSshResult {
                        id: worker_id,
                        peer_id,
                        generation,
                        owner_terminal_id: worker_owner_terminal_id,
                        result,
                        respond_to: worker_respond_to,
                    },
                ));
                if let Err(err) = event_tx.blocking_send(event) {
                    if let crate::events::AppEvent::PeerConnectSshFinished(mut finished) = err.0 {
                        if let Ok(connected) = &mut finished.result {
                            connected.bridge.stop(true);
                        }
                    }
                }
            });
        if let Err(err) = spawn {
            self.finish_pending_owner_operation(owner_terminal_id.as_ref());
            self.finish_remote_api_job();
            let _ = respond_to.send(encode_error_body(
                id,
                ErrorBody {
                    code: "ssh_peer_connect_failed".into(),
                    message: format!("could not start SSH peer setup: {err}"),
                },
            ));
        }
    }

    pub(super) fn handle_peer_connect_ssh_finished(
        &mut self,
        finished: crate::events::PeerConnectSshResult,
    ) {
        let crate::events::PeerConnectSshResult {
            id,
            peer_id,
            generation,
            owner_terminal_id,
            result,
            respond_to,
        } = finished;
        self.finish_pending_owner_operation(owner_terminal_id.as_ref());
        let connected = match result {
            Ok(connected) => connected,
            Err(message) => {
                let _ = respond_to.send(encode_error_body(
                    id,
                    ErrorBody {
                        code: "ssh_peer_connect_failed".into(),
                        message,
                    },
                ));
                return;
            }
        };
        let current_generation = self
            .peer_lifecycle_generations
            .get(&peer_id)
            .copied()
            .unwrap_or_default();
        if connected.peer.id != peer_id || current_generation != generation {
            self.stop_uncommitted_peer_bridge(&peer_id, connected.bridge);
            let _ = respond_to.send(encode_error(
                id,
                "peer_connect_cancelled",
                format!("SSH peer connection for {peer_id} was cancelled by peer removal"),
            ));
            return;
        }
        if owner_terminal_id
            .as_ref()
            .is_some_and(|terminal_id| !self.state.terminals.contains_key(terminal_id))
        {
            self.stop_uncommitted_peer_bridge(&peer_id, connected.bridge);
            let _ = respond_to.send(encode_error_body(
                id,
                ErrorBody {
                    code: "invalid_ssh_owner".into(),
                    message: "SSH owner pane closed while the remote connection was starting"
                        .into(),
                },
            ));
            return;
        }
        let peer = connected.peer.clone();
        let delegation = connected.delegation.clone();
        let attach = connected.attach.clone();
        let connection_id = self
            .peer_bridges
            .entry(peer.id.clone())
            .or_default()
            .push(connected.bridge);
        self.ssh_shell_bridge_deadlines.insert(
            (peer.id.clone(), connection_id.clone()),
            Instant::now() + SSH_SHELL_BRIDGE_LEASE,
        );
        if let Some(terminal_id) = owner_terminal_id {
            self.register_pending_owner_activation(
                terminal_id,
                peer.clone(),
                connection_id.clone(),
                delegation.clone(),
                attach,
                true,
            );
        }
        self.state.peers.insert(peer.id.clone(), peer.clone());
        self.start_peer_refresh(peer.clone());
        #[cfg(unix)]
        self.discover_remote_parked_terminals(peer.clone());
        self.state.mark_session_dirty();
        let response = encode_success(
            id,
            ResponseResult::PeerSshConnected {
                peer: peer.clone(),
                connection_id: connection_id.clone(),
                attach: connected.attach,
            },
        );
        if respond_to.send(response).is_err() {
            self.cancel_pending_owner_activation(&delegation.delegation_id);
            self.release_peer_bridge(&peer.id, &connection_id);
        }
    }

    pub(super) fn handle_peer_register(
        &mut self,
        id: String,
        params: PeerRegisterParams,
    ) -> String {
        let mut peer = params.peer;
        if !valid_peer_id(&peer.id) {
            return encode_error_body(
                id,
                ErrorBody {
                    code: "invalid_peer_id".into(),
                    message: "peer id may contain only ASCII letters, digits, dot, underscore, and hyphen".into(),
                },
            );
        }
        peer.status = PeerStatus::Connected;
        self.state.peers.insert(peer.id.clone(), peer.clone());
        self.start_peer_refresh(peer.clone());
        self.renew_incoming_peer_lease(&peer.id, Instant::now());
        self.state.mark_session_dirty();
        encode_success(id, ResponseResult::PeerInfo { peer })
    }

    pub(super) fn handle_peer_connect_ssh(
        &mut self,
        id: String,
        mut params: PeerConnectSshParams,
    ) -> String {
        let owner_terminal_id =
            match self.resolve_ssh_owner_terminal(params.owner_pane_id.as_deref()) {
                Ok(owner_terminal_id) => owner_terminal_id,
                Err(error) => return encode_error_body(id, error),
            };
        params.owner = owner_terminal_id
            .as_ref()
            .and_then(|terminal_id| {
                self.remote_presentations
                    .active
                    .values()
                    .find(|delegated| {
                        &delegated.moved.pane_state.attached_terminal_id == terminal_id
                    })
                    .map(|delegated| delegated.info.owner.clone())
            })
            .or_else(|| {
                params.owner_pane_id.as_ref().map(|pane_id| {
                    let peer_id = crate::remote_agent::local_peer_id();
                    crate::api::schema::TerminalPresentationOwner {
                        peer_id: peer_id.clone(),
                        pane_id: pane_id.clone(),
                        route: vec![peer_id],
                    }
                })
            });
        let connected = match crate::remote_agent::connect_shell(&params) {
            Ok(connected) => connected,
            Err(err) => {
                return encode_error_body(
                    id,
                    ErrorBody {
                        code: "ssh_peer_connect_failed".into(),
                        message: err.to_string(),
                    },
                );
            }
        };
        let peer = connected.peer.clone();
        let delegation = connected.delegation.clone();
        let attach = connected.attach.clone();
        let connection_id = self
            .peer_bridges
            .entry(peer.id.clone())
            .or_default()
            .push(connected.bridge);
        self.ssh_shell_bridge_deadlines.insert(
            (peer.id.clone(), connection_id.clone()),
            Instant::now() + SSH_SHELL_BRIDGE_LEASE,
        );
        if let Some(terminal_id) = owner_terminal_id {
            self.register_pending_owner_activation(
                terminal_id,
                peer.clone(),
                connection_id.clone(),
                delegation,
                attach,
                true,
            );
        }
        self.state.peers.insert(peer.id.clone(), peer.clone());
        self.start_peer_refresh(peer.clone());
        #[cfg(unix)]
        self.discover_remote_parked_terminals(peer.clone());
        self.state.mark_session_dirty();
        encode_success(
            id,
            ResponseResult::PeerSshConnected {
                peer,
                connection_id,
                attach: connected.attach,
            },
        )
    }

    pub(super) fn handle_peer_disconnect_ssh(
        &mut self,
        id: String,
        params: PeerDisconnectSshParams,
    ) -> String {
        if let Some(claim) = params.activated_delegation.as_ref() {
            let matches = self
                .pending_owner_activations
                .get(&claim.delegation_id)
                .is_some_and(|pending| pending.delegation.epoch == claim.epoch);
            if matches {
                self.finish_pending_owner_activation(&claim.delegation_id, true);
            }
        }
        self.release_peer_bridge(&params.peer_id, &params.connection_id);
        encode_success(
            id,
            ResponseResult::Ok {
                terminated_remote_presentations: None,
                handed_off_remote_presentations: None,
            },
        )
    }

    pub(super) fn handle_peer_keepalive_ssh(
        &mut self,
        id: String,
        params: PeerKeepaliveSshParams,
    ) -> String {
        let connected = self
            .peer_bridges
            .get(&params.peer_id)
            .is_some_and(|bridges| bridges.contains(&params.connection_id));
        if !connected {
            return encode_error_body(
                id,
                ErrorBody {
                    code: "peer_connection_not_found".into(),
                    message: format!(
                        "SSH peer connection {} for {} was not found",
                        params.connection_id, params.peer_id
                    ),
                },
            );
        }
        self.ssh_shell_bridge_deadlines.insert(
            (params.peer_id, params.connection_id),
            Instant::now() + SSH_SHELL_BRIDGE_LEASE,
        );
        encode_success(
            id,
            ResponseResult::Ok {
                terminated_remote_presentations: None,
                handed_off_remote_presentations: None,
            },
        )
    }

    pub(super) fn handle_peer_presentation_activate(
        &mut self,
        id: String,
        claim: crate::api::schema::TerminalDelegationClaim,
    ) -> String {
        let matches = self
            .pending_owner_activations
            .get(&claim.delegation_id)
            .is_some_and(|pending| pending.delegation.epoch == claim.epoch);
        if !matches {
            return encode_error_body(
                id,
                ErrorBody {
                    code: "presentation_activation_not_found".into(),
                    message: "the prepared owner presentation was not found or no longer matches"
                        .into(),
                },
            );
        }
        self.finish_pending_owner_activation(&claim.delegation_id, true);
        encode_success(
            id,
            ResponseResult::Ok {
                terminated_remote_presentations: None,
                handed_off_remote_presentations: None,
            },
        )
    }

    pub(super) fn handle_peer_unregister(&mut self, id: String, target: PeerTarget) -> String {
        let generation = self
            .peer_lifecycle_generations
            .entry(target.peer_id.clone())
            .or_default();
        *generation = generation.wrapping_add(1);
        let Some(peer) = self.state.peers.remove(&target.peer_id) else {
            return encode_error_body(
                id,
                ErrorBody {
                    code: "peer_not_found".into(),
                    message: format!("peer {} not found", target.peer_id),
                },
            );
        };
        self.terminate_remote_presentations_for_route_peer(&target.peer_id);
        self.shutdown_outbound_owner_terminals_for_peer(&target.peer_id);
        self.cancel_pending_owner_activations_for_peer(&target.peer_id);
        if let Some(mut bridges) = self.peer_bridges.remove(&target.peer_id) {
            bridges.stop_all(true);
        }
        self.ssh_shell_bridges.retain(|_, bridges| {
            bridges.retain(|(peer_id, _)| peer_id != &target.peer_id);
            !bridges.is_empty()
        });
        self.ssh_shell_bridge_deadlines
            .retain(|(peer_id, _), _| peer_id != &target.peer_id);
        self.cancel_peer_refresh(&target.peer_id);
        self.state.mark_session_dirty();
        encode_success(id, ResponseResult::PeerInfo { peer })
    }

    pub(super) fn handle_peer_list(&mut self, id: String) -> String {
        let mut peers: Vec<PeerInfo> = self.state.peers.values().cloned().collect();
        peers.sort_by(|a, b| a.id.cmp(&b.id));
        encode_success(id, ResponseResult::PeerList { peers })
    }

    pub(super) fn handle_peer_health(&mut self, id: String, target: PeerTarget) -> String {
        let Some(mut peer) = self.state.peers.get(&target.peer_id).cloned() else {
            return encode_error_body(
                id,
                ErrorBody {
                    code: "peer_not_found".into(),
                    message: format!("peer {} not found", target.peer_id),
                },
            );
        };
        let request = match self
            .peer_bridges
            .get(&target.peer_id)
            .and_then(|bridges| bridges.latest())
            .and_then(|bridge| bridge.remote_peer_id())
        {
            Some(remote_peer_id) => Request {
                id: "peer:health:reverse".into(),
                method: Method::PeerHealth(PeerTarget {
                    peer_id: remote_peer_id.to_string(),
                }),
            },
            None => Request {
                id: "peer:health".into(),
                method: Method::Ping(PingParams::default()),
            },
        };
        peer.status = match self.peer_request_value(&peer, &request) {
            Ok(response)
                if response.get("error").is_none()
                    && response["result"]["peer"]["status"]
                        .as_str()
                        .is_none_or(|status| status == "connected") =>
            {
                PeerStatus::Connected
            }
            _ => PeerStatus::Disconnected,
        };
        self.state.peers.insert(peer.id.clone(), peer.clone());
        encode_success(id, ResponseResult::PeerInfo { peer })
    }

    pub(super) fn begin_peer_health(
        &mut self,
        id: String,
        target: PeerTarget,
        respond_to: std::sync::mpsc::Sender<String>,
    ) {
        let Some(peer) = self.state.peers.get(&target.peer_id).cloned() else {
            let _ = respond_to.send(encode_error_body(
                id,
                ErrorBody {
                    code: "peer_not_found".into(),
                    message: format!("peer {} not found", target.peer_id),
                },
            ));
            return;
        };
        let generation = self
            .peer_refresh_generations
            .get(&target.peer_id)
            .copied()
            .unwrap_or_default();
        let request = match self
            .peer_bridges
            .get(&target.peer_id)
            .and_then(|bridges| bridges.latest())
            .and_then(|bridge| bridge.remote_peer_id())
        {
            Some(remote_peer_id) => Request {
                id: "peer:health:reverse".into(),
                method: Method::PeerHealth(PeerTarget {
                    peer_id: remote_peer_id.to_string(),
                }),
            },
            None => Request {
                id: "peer:health".into(),
                method: Method::Ping(PingParams::default()),
            },
        };
        if !self.try_begin_remote_api_job(&id, &respond_to) {
            return;
        }
        let event_tx = self.event_tx.clone();
        let worker_id = id.clone();
        let worker_peer = peer.clone();
        let worker_respond_to = respond_to.clone();
        let spawn = std::thread::Builder::new()
            .name("herdr-peer-health".into())
            .spawn(move || {
                let result = super::super::peer_agents::peer_request_value(&worker_peer, &request);
                let _ = event_tx.blocking_send(crate::events::AppEvent::PeerHealthRequestFinished(
                    Box::new(crate::events::PeerHealthRequestResult {
                        id: worker_id,
                        peer: worker_peer,
                        generation,
                        result,
                        respond_to: worker_respond_to,
                    }),
                ));
            });
        if let Err(err) = spawn {
            self.finish_remote_api_job();
            let _ = respond_to.send(encode_error_body(
                id,
                ErrorBody {
                    code: "peer_unreachable".into(),
                    message: format!("could not start peer health check: {err}"),
                },
            ));
        }
    }

    pub(super) fn handle_peer_health_finished(
        &mut self,
        finished: crate::events::PeerHealthRequestResult,
    ) {
        let crate::events::PeerHealthRequestResult {
            id,
            mut peer,
            generation,
            result,
            respond_to,
        } = finished;
        let current_generation = self.peer_refresh_generations.get(&peer.id).copied();
        if current_generation != Some(generation) {
            let response = match self.state.peers.get(&peer.id).cloned() {
                Some(current) => encode_success(id, ResponseResult::PeerInfo { peer: current }),
                None => encode_error(id, "peer_not_found", format!("peer {} not found", peer.id)),
            };
            let _ = respond_to.send(response);
            return;
        }
        let remote_connected = result.is_ok_and(|response| {
            response.get("error").is_none()
                && response["result"]["peer"]["status"]
                    .as_str()
                    .is_none_or(|status| status == "connected")
        });
        let bridge_connected = self
            .peer_bridges
            .get(&peer.id)
            .is_none_or(|bridges| bridges.has_healthy_bridge());
        peer.status = if remote_connected && bridge_connected {
            PeerStatus::Connected
        } else {
            PeerStatus::Disconnected
        };
        let response_peer = match self.state.peers.get_mut(&peer.id) {
            Some(current) if current.transport == peer.transport => {
                current.status = peer.status;
                current.clone()
            }
            Some(current) => current.clone(),
            None => peer,
        };
        let _ = respond_to.send(encode_success(
            id,
            ResponseResult::PeerInfo {
                peer: response_peer,
            },
        ));
    }

    pub(crate) fn release_peer_bridge(&mut self, peer_id: &str, connection_id: &str) {
        self.cancel_pending_owner_activations_for_bridge(peer_id, connection_id);
        let mut owner_terminals = self.outbound_owner_terminals_for_bridge(peer_id, connection_id);
        self.ssh_shell_bridge_deadlines
            .remove(&(peer_id.to_string(), connection_id.to_string()));
        self.remove_shell_bridge_owner(peer_id, connection_id);
        let Some(bridges) = self.peer_bridges.get_mut(peer_id) else {
            return;
        };
        let Some(mut removed) = bridges.remove(connection_id) else {
            return;
        };
        let is_empty = bridges.is_empty();
        #[cfg(unix)]
        let replacement = if !is_empty {
            bridges.latest().map(|bridge| {
                let peer = bridge.peer_info();
                let registration = bridge.registration();
                (peer, registration)
            })
        } else {
            None
        };
        if is_empty {
            owner_terminals.extend(self.remote_owner_peers.iter().filter_map(
                |(terminal_id, owner_peer_id)| {
                    if owner_peer_id == peer_id {
                        Some(terminal_id.clone())
                    } else {
                        None
                    }
                },
            ));
            removed.stop(true);
            self.peer_bridges.remove(peer_id);
            self.state.peers.remove(peer_id);
            self.cancel_peer_refresh(peer_id);
        } else {
            // A surviving bridge will replace this registration. Do not explicitly
            // unregister the removed bridge first: remote unregister destroys every
            // presentation routed through the peer before replacement can win.
            removed.stop(false);
            #[cfg(unix)]
            if let Some((peer, registration)) = replacement {
                let replacement_peer_id = peer_id.to_string();
                std::thread::spawn(move || {
                    if let Err(err) = registration.register() {
                        tracing::warn!(%err, peer = replacement_peer_id, "could not move remote peer registration to a surviving SSH bridge");
                    }
                });
                self.state.peers.insert(peer_id.to_string(), peer);
                if let Some(peer) = self.state.peers.get(peer_id).cloned() {
                    self.start_peer_refresh(peer);
                }
            }
        }
        self.state.mark_session_dirty();
        let mut seen = std::collections::HashSet::new();
        owner_terminals.retain(|terminal_id| seen.insert(terminal_id.clone()));
        self.shutdown_outbound_owner_terminals(owner_terminals);
    }

    fn stop_uncommitted_peer_bridge(
        &mut self,
        peer_id: &str,
        mut bridge: crate::remote_agent::PeerBridgeRuntime,
    ) {
        #[cfg(not(unix))]
        let _ = peer_id;

        #[cfg(unix)]
        if let Some((peer, registration)) = self
            .peer_bridges
            .get(peer_id)
            .and_then(|bridges| bridges.latest())
            .map(|survivor| (survivor.peer_info(), survivor.registration()))
        {
            bridge.stop(false);
            let replacement_peer_id = peer_id.to_string();
            std::thread::spawn(move || {
                if let Err(err) = registration.register() {
                    tracing::warn!(%err, peer = replacement_peer_id, "could not restore remote peer registration after cancelling a new SSH bridge");
                }
            });
            self.state.peers.insert(peer_id.to_string(), peer);
            if let Some(peer) = self.state.peers.get(peer_id).cloned() {
                self.start_peer_refresh(peer);
            }
            return;
        }

        bridge.stop(true);
    }

    pub(crate) fn next_ssh_shell_bridge_deadline(&self) -> Option<Instant> {
        self.ssh_shell_bridge_deadlines.values().copied().min()
    }

    pub(crate) fn renew_incoming_peer_lease(&mut self, peer_id: &str, now: Instant) {
        let incoming = self
            .state
            .peers
            .get(peer_id)
            .is_some_and(|peer| matches!(&peer.transport, PeerTransportInfo::ApiSocket { .. }));
        if incoming {
            self.incoming_peer_deadlines
                .insert(peer_id.to_string(), now + INCOMING_PEER_LEASE);
        }
    }

    pub(crate) fn next_incoming_peer_deadline(&self) -> Option<Instant> {
        self.incoming_peer_deadlines.values().copied().min()
    }

    pub(crate) fn next_remote_owner_deadline(&self) -> Option<Instant> {
        self.remote_owner_deadlines.values().copied().min()
    }

    #[cfg(unix)]
    pub(crate) fn renew_remote_owner_lease(
        &mut self,
        terminal_id: &crate::terminal::TerminalId,
        now: Instant,
    ) {
        if let Some(deadline) = self.remote_owner_deadlines.get_mut(terminal_id) {
            *deadline = now + REMOTE_OWNER_LEASE;
        }
    }

    pub(crate) fn expire_remote_owner_leases(&mut self, now: Instant) -> bool {
        let expired = self
            .remote_owner_deadlines
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(terminal_id, _)| terminal_id.clone())
            .collect::<Vec<_>>();
        for terminal_id in &expired {
            self.remote_owner_deadlines.remove(terminal_id);
            tracing::warn!(terminal = %terminal_id, "remote owner presentation lease expired");
        }
        let changed = !expired.is_empty();
        self.shutdown_outbound_owner_terminals(expired);
        changed
    }

    pub(crate) fn expire_incoming_peer_leases(&mut self, now: Instant) -> bool {
        let expired = self
            .incoming_peer_deadlines
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(peer_id, _)| peer_id.clone())
            .collect::<Vec<_>>();
        let mut changed = false;
        for peer_id in expired {
            self.incoming_peer_deadlines.remove(&peer_id);
            let incoming =
                self.state.peers.get(&peer_id).is_some_and(|peer| {
                    matches!(&peer.transport, PeerTransportInfo::ApiSocket { .. })
                });
            if !incoming {
                continue;
            }
            tracing::warn!(peer = %peer_id, "incoming peer lease expired");
            self.state.peers.remove(&peer_id);
            self.cancel_peer_refresh(&peer_id);
            self.terminate_remote_presentations_for_route_peer(&peer_id);
            self.state.mark_session_dirty();
            changed = true;
        }
        changed
    }

    pub(crate) fn expire_ssh_shell_bridge_leases(&mut self, now: Instant) -> bool {
        let expired = self
            .ssh_shell_bridge_deadlines
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|((peer_id, connection_id), _)| (peer_id.clone(), connection_id.clone()))
            .collect::<Vec<_>>();
        for (peer_id, connection_id) in &expired {
            tracing::warn!(
                peer = peer_id,
                connection = connection_id,
                "SSH peer connection lease expired"
            );
            self.release_peer_bridge(peer_id, connection_id);
        }
        !expired.is_empty()
    }

    pub(crate) fn release_remote_pane_bridge(&mut self, pane_id: crate::layout::PaneId) {
        let terminal_id = self.state.workspaces.iter().find_map(|workspace| {
            workspace
                .pane_state(pane_id)
                .map(|pane| pane.attached_terminal_id.clone())
        });
        let Some(terminal_id) = terminal_id else {
            return;
        };
        self.release_remote_terminal_bridge(&terminal_id);
    }

    pub(crate) fn release_remote_terminal_bridge(
        &mut self,
        terminal_id: &crate::terminal::TerminalId,
    ) {
        self.cancel_pending_owner_activations_for_terminal(terminal_id);
        self.remote_owner_deadlines.remove(terminal_id);
        let shell_bridges = self
            .ssh_shell_bridges
            .remove(terminal_id)
            .unwrap_or_default();
        for (peer_id, connection_id) in shell_bridges {
            self.release_peer_bridge(&peer_id, &connection_id);
        }
        let mirror_cancelled = self
            .remote_mirror_cancellations
            .remove(&terminal_id.to_string());
        self.remote_owner_presentations.remove(terminal_id);
        self.remote_owner_peers.remove(terminal_id);
        if let Some(cancelled) = mirror_cancelled.as_ref() {
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        }
        let bridge = self.remote_terminal_bridges.remove(terminal_id);
        let was_remote = mirror_cancelled.is_some()
            || bridge.is_some()
            || self
                .state
                .terminals
                .get(terminal_id)
                .is_some_and(|terminal| terminal.remote_agent_transport.is_some());
        if was_remote {
            if let Some(terminal) = self.state.terminals.get_mut(terminal_id) {
                terminal.remote_agent_transport = None;
                terminal.launch_argv = None;
            }
        }
        if let Some((peer_id, connection_id)) = bridge {
            self.release_peer_bridge(&peer_id, &connection_id);
        }
    }

    fn start_ssh_shell_mirror(
        &mut self,
        local_terminal_id: &crate::terminal::TerminalId,
        peer: &PeerInfo,
        delegation: &crate::api::schema::TerminalDelegationInfo,
    ) {
        let Some(transport) = ssh_shell_transport(peer, delegation) else {
            return;
        };
        let local_pane_id = self
            .state
            .workspaces
            .iter()
            .find_map(|workspace| {
                workspace.tabs.iter().find_map(|tab| {
                    tab.panes.iter().find_map(|(pane_id, pane)| {
                        (&pane.attached_terminal_id == local_terminal_id).then_some(*pane_id)
                    })
                })
            })
            .or_else(|| {
                self.remote_presentations
                    .active
                    .values()
                    .find(|presented| {
                        &presented.moved.pane_state.attached_terminal_id == local_terminal_id
                    })
                    .map(|presented| presented.moved.pane_id)
            });
        let Some(local_pane_id) = local_pane_id else {
            return;
        };
        if let Some(terminal) = self.state.terminals.get_mut(local_terminal_id) {
            terminal.remote_agent_transport = Some(transport.clone());
        }
        if let Some(cancelled) = self.remote_mirror_cancellations.insert(
            local_terminal_id.to_string(),
            crate::remote_agent::spawn_mirror(transport, local_pane_id, self.event_tx.clone()),
        ) {
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    pub(crate) fn shutdown_peer_runtime(&mut self) {
        let owner_terminals = self
            .remote_owner_presentations
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        self.shutdown_outbound_owner_terminals(owner_terminals);
        for (_, pending) in self.pending_owner_activations.drain() {
            pending
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
        for (_, cancelled) in self.peer_refresh_cancellations.drain() {
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        }
        for (_, cancelled) in self.remote_mirror_cancellations.drain() {
            cancelled.store(true, std::sync::atomic::Ordering::Release);
        }
        for (_, mut bridges) in self.peer_bridges.drain() {
            bridges.stop_all(true);
        }
        self.peer_refresh_generations.clear();
        self.peer_agent_cache.clear();
        self.ssh_shell_bridges.clear();
        self.remote_owner_presentations.clear();
        self.remote_owner_peers.clear();
        self.remote_owner_deadlines.clear();
        self.ssh_shell_bridge_deadlines.clear();
        self.incoming_peer_deadlines.clear();
        self.remote_terminal_bridges.clear();
        self.peer_lifecycle_generations.clear();
        self.state.peers.clear();
    }

    fn remove_shell_bridge_owner(&mut self, peer_id: &str, connection_id: &str) {
        let affected = self
            .ssh_shell_bridges
            .iter()
            .filter(|(_, bridges)| {
                bridges
                    .iter()
                    .any(|(candidate_peer, candidate_connection)| {
                        candidate_peer == peer_id && candidate_connection == connection_id
                    })
            })
            .map(|(terminal_id, _)| terminal_id.clone())
            .collect::<Vec<_>>();
        self.ssh_shell_bridges.retain(|_, bridges| {
            bridges.retain(|(candidate_peer, candidate_connection)| {
                candidate_peer != peer_id || candidate_connection != connection_id
            });
            !bridges.is_empty()
        });
        for terminal_id in affected {
            if !self.ssh_shell_bridges.contains_key(&terminal_id)
                && !self.remote_terminal_bridges.contains_key(&terminal_id)
            {
                self.remote_owner_presentations.remove(&terminal_id);
                self.remote_owner_peers.remove(&terminal_id);
                self.remote_owner_deadlines.remove(&terminal_id);
                if let Some(cancelled) = self
                    .remote_mirror_cancellations
                    .remove(&terminal_id.to_string())
                {
                    cancelled.store(true, std::sync::atomic::Ordering::Release);
                }
                if let Some(terminal) = self.state.terminals.get_mut(&terminal_id) {
                    terminal.remote_agent_transport = None;
                }
            }
        }
    }

    pub(super) fn register_pending_owner_activation(
        &mut self,
        terminal_id: crate::terminal::TerminalId,
        peer: PeerInfo,
        connection_id: String,
        delegation: crate::api::schema::TerminalDelegationInfo,
        attach: crate::api::schema::AgentAttachInfo,
        release_bridge_on_owner_close: bool,
    ) {
        let stale = self
            .pending_owner_activations
            .iter()
            .filter(|(_, pending)| pending.terminal_id == terminal_id)
            .map(|(delegation_id, _)| delegation_id.clone())
            .collect::<Vec<_>>();
        for delegation_id in stale {
            self.cancel_pending_owner_activation(&delegation_id);
        }
        let cancelled =
            crate::remote_agent::spawn_delegation_activation_watch(attach, self.event_tx.clone());
        self.pending_owner_activations.insert(
            delegation.delegation_id.clone(),
            crate::app::PendingOwnerActivation {
                terminal_id,
                peer_id: peer.id.clone(),
                connection_id,
                peer,
                delegation,
                release_bridge_on_owner_close,
                cancelled,
            },
        );
    }

    pub(crate) fn finish_pending_owner_activation(&mut self, delegation_id: &str, activated: bool) {
        let Some(pending) = self.pending_owner_activations.remove(delegation_id) else {
            return;
        };
        pending
            .cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        if !activated
            || !self.state.terminals.contains_key(&pending.terminal_id)
            || !self
                .peer_bridges
                .get(&pending.peer_id)
                .is_some_and(|bridges| bridges.contains(&pending.connection_id))
        {
            return;
        }
        if pending.release_bridge_on_owner_close {
            let owner = (pending.peer_id.clone(), pending.connection_id.clone());
            let owners = self
                .ssh_shell_bridges
                .entry(pending.terminal_id.clone())
                .or_default();
            if !owners.contains(&owner) {
                owners.push(owner);
            }
        } else {
            self.remote_owner_deadlines.insert(
                pending.terminal_id.clone(),
                Instant::now() + REMOTE_OWNER_LEASE,
            );
        }
        self.remote_owner_presentations
            .insert(pending.terminal_id.clone(), pending.delegation.clone());
        self.remote_owner_peers
            .insert(pending.terminal_id.clone(), pending.peer_id.clone());
        self.start_ssh_shell_mirror(&pending.terminal_id, &pending.peer, &pending.delegation);
        self.state.mark_session_dirty();
    }

    pub(super) fn cancel_pending_owner_activation(&mut self, delegation_id: &str) {
        if let Some(pending) = self.pending_owner_activations.remove(delegation_id) {
            pending
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    fn cancel_pending_owner_activations_for_bridge(&mut self, peer_id: &str, connection_id: &str) {
        let delegation_ids = self
            .pending_owner_activations
            .iter()
            .filter(|(_, pending)| {
                pending.peer_id == peer_id && pending.connection_id == connection_id
            })
            .map(|(delegation_id, _)| delegation_id.clone())
            .collect::<Vec<_>>();
        for delegation_id in delegation_ids {
            self.cancel_pending_owner_activation(&delegation_id);
        }
    }

    fn cancel_pending_owner_activations_for_peer(&mut self, peer_id: &str) {
        let delegation_ids = self
            .pending_owner_activations
            .iter()
            .filter(|(_, pending)| pending.peer_id == peer_id)
            .map(|(delegation_id, _)| delegation_id.clone())
            .collect::<Vec<_>>();
        for delegation_id in delegation_ids {
            self.cancel_pending_owner_activation(&delegation_id);
        }
    }

    fn cancel_pending_owner_activations_for_terminal(
        &mut self,
        terminal_id: &crate::terminal::TerminalId,
    ) {
        let delegation_ids = self
            .pending_owner_activations
            .iter()
            .filter(|(_, pending)| &pending.terminal_id == terminal_id)
            .map(|(delegation_id, _)| delegation_id.clone())
            .collect::<Vec<_>>();
        let mut dedicated_bridges = Vec::new();
        for delegation_id in delegation_ids {
            let Some(pending) = self.pending_owner_activations.remove(&delegation_id) else {
                continue;
            };
            pending
                .cancelled
                .store(true, std::sync::atomic::Ordering::Release);
            if pending.release_bridge_on_owner_close {
                dedicated_bridges.push((pending.peer_id, pending.connection_id));
            }
        }
        dedicated_bridges.sort_unstable();
        dedicated_bridges.dedup();
        for (peer_id, connection_id) in dedicated_bridges {
            self.release_peer_bridge(&peer_id, &connection_id);
        }
    }

    fn outbound_owner_terminals_for_bridge(
        &self,
        peer_id: &str,
        connection_id: &str,
    ) -> Vec<crate::terminal::TerminalId> {
        let mut terminals = self
            .ssh_shell_bridges
            .iter()
            .filter(|(_, bridges)| {
                bridges
                    .iter()
                    .any(|(candidate_peer, candidate_connection)| {
                        candidate_peer == peer_id && candidate_connection == connection_id
                    })
            })
            .map(|(terminal_id, _)| terminal_id.clone())
            .collect::<Vec<_>>();
        terminals.extend(self.remote_terminal_bridges.iter().filter_map(
            |(terminal_id, (candidate_peer, candidate_connection))| {
                if candidate_peer == peer_id && candidate_connection == connection_id {
                    Some(terminal_id.clone())
                } else {
                    None
                }
            },
        ));
        let mut seen = std::collections::HashSet::new();
        terminals.retain(|terminal_id| seen.insert(terminal_id.clone()));
        terminals
    }

    fn shutdown_outbound_owner_terminals_for_peer(&mut self, peer_id: &str) {
        let mut terminals = self
            .ssh_shell_bridges
            .iter()
            .filter(|(_, bridges)| bridges.iter().any(|(candidate, _)| candidate == peer_id))
            .map(|(terminal_id, _)| terminal_id.clone())
            .collect::<Vec<_>>();
        terminals.extend(self.remote_terminal_bridges.iter().filter_map(
            |(terminal_id, (candidate, _))| {
                if candidate == peer_id {
                    Some(terminal_id.clone())
                } else {
                    None
                }
            },
        ));
        terminals.extend(
            self.remote_owner_peers
                .iter()
                .filter_map(|(terminal_id, candidate)| {
                    if candidate == peer_id {
                        Some(terminal_id.clone())
                    } else {
                        None
                    }
                }),
        );
        let mut seen = std::collections::HashSet::new();
        terminals.retain(|terminal_id| seen.insert(terminal_id.clone()));
        self.shutdown_outbound_owner_terminals(terminals);
    }

    pub(super) fn shutdown_outbound_owner_terminals(
        &mut self,
        terminal_ids: Vec<crate::terminal::TerminalId>,
    ) {
        for terminal_id in terminal_ids {
            if let Some(claim) = self
                .remote_presentations
                .active
                .values()
                .find(|delegated| delegated.moved.pane_state.attached_terminal_id == terminal_id)
                .map(|delegated| crate::api::schema::TerminalDelegationClaim {
                    delegation_id: delegated.info.delegation_id.clone(),
                    epoch: delegated.info.epoch,
                })
            {
                self.terminate_terminal_delegation(&claim);
                continue;
            }

            let pane_id = self.state.workspaces.iter().find_map(|workspace| {
                workspace.tabs.iter().find_map(|tab| {
                    tab.panes.iter().find_map(|(pane_id, pane)| {
                        (pane.attached_terminal_id == terminal_id).then_some(*pane_id)
                    })
                })
            });
            self.release_remote_terminal_bridge(&terminal_id);
            self.state.terminals.remove(&terminal_id);
            if let Some(runtime) = self.terminal_runtimes.remove(&terminal_id) {
                runtime.shutdown();
            }
            if let Some(pane_id) = pane_id {
                self.handle_internal_event(crate::events::AppEvent::PaneDied { pane_id });
            }
        }
    }
}

fn ssh_shell_transport(
    peer: &PeerInfo,
    delegation: &crate::api::schema::TerminalDelegationInfo,
) -> Option<crate::api::schema::AgentTransportInfo> {
    let PeerTransportInfo::Ssh {
        target,
        ssh_args,
        managed_control_path,
        session,
    } = &peer.transport
    else {
        return None;
    };
    Some(crate::api::schema::AgentTransportInfo::Ssh {
        target: target.clone(),
        ssh_args: ssh_args.clone(),
        managed_control_path: managed_control_path.clone(),
        session: session.clone(),
        remote_terminal_id: delegation.terminal_id.clone(),
        remote_pane_id: delegation.pane_id.clone(),
        remote_agent: None,
        remote_cwd: None,
    })
}

fn valid_peer_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_app() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        )
    }

    fn api_socket_peer(id: &str, socket: &str) -> PeerInfo {
        PeerInfo {
            id: id.into(),
            label: id.into(),
            status: PeerStatus::Connected,
            transport: PeerTransportInfo::ApiSocket {
                api_socket: socket.into(),
            },
        }
    }

    fn ssh_peer(id: &str) -> PeerInfo {
        PeerInfo {
            id: id.into(),
            label: id.into(),
            status: PeerStatus::Connected,
            transport: PeerTransportInfo::Ssh {
                target: id.into(),
                ssh_args: Vec::new(),
                managed_control_path: None,
                session: None,
            },
        }
    }

    fn test_owner_delegation(id: &str) -> crate::api::schema::TerminalDelegationInfo {
        crate::api::schema::TerminalDelegationInfo {
            delegation_id: id.into(),
            epoch: 1,
            terminal_id: "remote-terminal".into(),
            pane_id: "remote-pane".into(),
            origin_peer_id: "remote".into(),
            owner: crate::api::schema::TerminalPresentationOwner {
                peer_id: "owner".into(),
                pane_id: "owner-pane".into(),
                route: vec!["owner".into(), "remote".into()],
            },
            status: crate::api::schema::TerminalDelegationStatus::Pending,
        }
    }

    #[test]
    fn ssh_shell_transport_keeps_remote_terminal_and_pane_ids_distinct() {
        let peer = ssh_peer("remote");
        let delegation = crate::api::schema::TerminalDelegationInfo {
            delegation_id: "delegation".into(),
            epoch: 1,
            terminal_id: "term_remote".into(),
            pane_id: "w2:p3".into(),
            origin_peer_id: "remote".into(),
            owner: crate::api::schema::TerminalPresentationOwner {
                peer_id: "owner".into(),
                pane_id: "w1:p1".into(),
                route: vec!["owner".into(), "remote".into()],
            },
            status: crate::api::schema::TerminalDelegationStatus::Active,
        };

        let transport = ssh_shell_transport(&peer, &delegation).unwrap();
        let crate::api::schema::AgentTransportInfo::Ssh {
            remote_terminal_id,
            remote_pane_id,
            ..
        } = transport;

        assert_eq!(remote_terminal_id, "term_remote");
        assert_eq!(remote_pane_id, "w2:p3");
    }

    #[tokio::test]
    async fn releasing_an_active_owner_bridge_removes_the_owner_pane_synchronously() {
        let mut app = test_app();
        let mut workspace = crate::workspace::Workspace::test_new("owner");
        workspace.worktree_space = Some(crate::workspace::WorktreeSpaceMembership {
            key: "repo".into(),
            label: "owner".into(),
            repo_root: "/repo".into(),
            checkout_path: "/repo".into(),
            is_linked_worktree: false,
        });
        let pane_id = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        app.terminal_runtimes.insert(
            terminal_id.clone(),
            crate::terminal::TerminalRuntime::test_with_screen_bytes(80, 24, b""),
        );
        app.ssh_shell_bridges.insert(
            terminal_id.clone(),
            vec![("remote".into(), "connection".into())],
        );
        app.remote_owner_presentations.insert(
            terminal_id.clone(),
            crate::api::schema::TerminalDelegationInfo {
                delegation_id: "delegation".into(),
                epoch: 1,
                terminal_id: "remote-terminal".into(),
                pane_id: "remote-pane".into(),
                origin_peer_id: "remote".into(),
                owner: crate::api::schema::TerminalPresentationOwner {
                    peer_id: "owner".into(),
                    pane_id: "owner-pane".into(),
                    route: vec!["owner".into(), "remote".into()],
                },
                status: crate::api::schema::TerminalDelegationStatus::Active,
            },
        );

        app.shutdown_outbound_owner_terminals(vec![terminal_id.clone()]);

        assert!(app.state.workspaces.is_empty());
        assert!(!app.state.terminals.contains_key(&terminal_id));
        assert!(app.terminal_runtimes.get(&terminal_id).is_none());
        assert!(!app.remote_owner_presentations.contains_key(&terminal_id));
    }

    #[test]
    #[cfg(unix)]
    fn mirrored_remote_cwd_updates_the_owner_transport_metadata() {
        let mut app = test_app();
        let workspace = crate::workspace::Workspace::test_new("owner");
        let pane_id = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .expect("terminal state")
            .remote_agent_transport = Some(crate::api::schema::AgentTransportInfo::Ssh {
            target: "remote".into(),
            ssh_args: Vec::new(),
            managed_control_path: None,
            session: None,
            remote_terminal_id: "remote-terminal".into(),
            remote_pane_id: "remote-pane".into(),
            remote_agent: None,
            remote_cwd: None,
        });

        app.handle_internal_event(crate::events::AppEvent::RemoteAgentInfoMirrored {
            pane_id,
            remote_cwd: Some("/remote/repo".into()),
        });

        let Some(crate::api::schema::AgentTransportInfo::Ssh { remote_cwd, .. }) = app
            .state
            .terminals
            .get(&terminal_id)
            .and_then(|terminal| terminal.remote_agent_transport.as_ref())
        else {
            panic!("expected SSH transport");
        };
        assert_eq!(remote_cwd.as_deref(), Some("/remote/repo"));
    }

    #[test]
    fn peer_agent_owner_cleanup_preserves_the_shared_peer_bridge() {
        let mut app = test_app();
        let workspace = crate::workspace::Workspace::test_new("owner");
        let pane_id = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        let peer = ssh_peer("remote");
        let connection_id = app.peer_bridges.entry(peer.id.clone()).or_default().push(
            crate::remote_agent::PeerBridgeRuntime::test("shared-connection", &peer.id),
        );
        let delegation = crate::api::schema::TerminalDelegationInfo {
            delegation_id: "delegation".into(),
            epoch: 1,
            terminal_id: "remote-terminal".into(),
            pane_id: "remote-pane".into(),
            origin_peer_id: "remote".into(),
            owner: crate::api::schema::TerminalPresentationOwner {
                peer_id: "owner".into(),
                pane_id: "owner-pane".into(),
                route: vec!["owner".into(), "remote".into()],
            },
            status: crate::api::schema::TerminalDelegationStatus::Active,
        };
        app.pending_owner_activations.insert(
            delegation.delegation_id.clone(),
            crate::app::PendingOwnerActivation {
                terminal_id: terminal_id.clone(),
                peer_id: peer.id.clone(),
                connection_id: connection_id.clone(),
                peer,
                delegation: delegation.clone(),
                release_bridge_on_owner_close: false,
                cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );

        let response = app.handle_peer_presentation_activate(
            "activate".into(),
            crate::api::schema::TerminalDelegationClaim {
                delegation_id: delegation.delegation_id.clone(),
                epoch: delegation.epoch,
            },
        );
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(response["result"]["type"], "ok");
        assert!(!app.ssh_shell_bridges.contains_key(&terminal_id));
        assert!(app.remote_owner_deadlines.contains_key(&terminal_id));
        assert!(app.remote_owner_presentations.contains_key(&terminal_id));
        assert!(app.expire_remote_owner_leases(
            Instant::now() + REMOTE_OWNER_LEASE + Duration::from_millis(1)
        ));
        assert!(app.state.workspaces.is_empty());
        assert!(app
            .peer_bridges
            .get("remote")
            .is_some_and(|bridges| bridges.contains(&connection_id)));
    }

    #[test]
    fn owner_teardown_releases_a_pending_direct_ssh_bridge() {
        let mut app = test_app();
        let workspace = crate::workspace::Workspace::test_new("owner");
        let pane_id = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        let peer = ssh_peer("remote");
        let connection_id = app.peer_bridges.entry(peer.id.clone()).or_default().push(
            crate::remote_agent::PeerBridgeRuntime::test("direct-connection", &peer.id),
        );
        app.state.peers.insert(peer.id.clone(), peer.clone());
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delegation = test_owner_delegation("pending-direct");
        app.pending_owner_activations.insert(
            delegation.delegation_id.clone(),
            crate::app::PendingOwnerActivation {
                terminal_id: terminal_id.clone(),
                peer_id: peer.id.clone(),
                connection_id,
                peer,
                delegation,
                release_bridge_on_owner_close: true,
                cancelled: std::sync::Arc::clone(&cancelled),
            },
        );

        app.release_remote_terminal_bridge(&terminal_id);

        assert!(app.pending_owner_activations.is_empty());
        assert!(cancelled.load(std::sync::atomic::Ordering::Acquire));
        assert!(!app.peer_bridges.contains_key("remote"));
        assert!(!app.state.peers.contains_key("remote"));
    }

    #[test]
    fn owner_teardown_preserves_a_pending_shared_peer_bridge() {
        let mut app = test_app();
        let workspace = crate::workspace::Workspace::test_new("owner");
        let pane_id = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        let peer = ssh_peer("remote");
        let connection_id = app.peer_bridges.entry(peer.id.clone()).or_default().push(
            crate::remote_agent::PeerBridgeRuntime::test("shared-connection", &peer.id),
        );
        app.state.peers.insert(peer.id.clone(), peer.clone());
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let delegation = test_owner_delegation("pending-shared");
        app.pending_owner_activations.insert(
            delegation.delegation_id.clone(),
            crate::app::PendingOwnerActivation {
                terminal_id: terminal_id.clone(),
                peer_id: peer.id.clone(),
                connection_id: connection_id.clone(),
                peer,
                delegation,
                release_bridge_on_owner_close: false,
                cancelled: std::sync::Arc::clone(&cancelled),
            },
        );

        app.release_remote_terminal_bridge(&terminal_id);

        assert!(app.pending_owner_activations.is_empty());
        assert!(cancelled.load(std::sync::atomic::Ordering::Acquire));
        assert!(app
            .peer_bridges
            .get("remote")
            .is_some_and(|bridges| bridges.contains(&connection_id)));
        assert!(app.state.peers.contains_key("remote"));
    }

    #[test]
    #[cfg(unix)]
    fn releasing_one_same_peer_bridge_preserves_the_surviving_registration() {
        let mut app = test_app();
        let peer = ssh_peer("remote");
        let bridges = app.peer_bridges.entry(peer.id.clone()).or_default();
        let surviving_connection = bridges.push(crate::remote_agent::PeerBridgeRuntime::test(
            "surviving-connection",
            &peer.id,
        ));
        let removed_connection = bridges.push(crate::remote_agent::PeerBridgeRuntime::test(
            "removed-connection",
            &peer.id,
        ));
        app.state.peers.insert(peer.id.clone(), peer);

        app.release_peer_bridge("remote", &removed_connection);

        assert!(app
            .peer_bridges
            .get("remote")
            .is_some_and(|bridges| bridges.contains(&surviving_connection)));
        assert!(app.state.peers.contains_key("remote"));
    }

    #[test]
    #[cfg(unix)]
    fn cancelling_an_uncommitted_same_peer_bridge_restores_the_survivor() {
        let mut app = test_app();
        let peer = ssh_peer("remote");
        let surviving_connection = app.peer_bridges.entry(peer.id.clone()).or_default().push(
            crate::remote_agent::PeerBridgeRuntime::test("surviving-connection", &peer.id),
        );
        app.state.peers.insert(peer.id.clone(), peer.clone());
        let uncommitted =
            crate::remote_agent::PeerBridgeRuntime::test("uncommitted-connection", &peer.id);

        app.stop_uncommitted_peer_bridge(&peer.id, uncommitted);

        assert!(app
            .peer_bridges
            .get("remote")
            .is_some_and(|bridges| bridges.contains(&surviving_connection)));
        assert!(app.state.peers.contains_key("remote"));
    }

    #[test]
    fn peer_id_validation_accepts_stable_socket_targets() {
        assert!(valid_peer_id("workbox"));
        assert!(valid_peer_id("dev.example_1"));
        assert!(valid_peer_id("remote-8f3a"));
    }

    #[test]
    fn peer_id_validation_rejects_shell_like_values() {
        assert!(!valid_peer_id(""));
        assert!(!valid_peer_id("../remote"));
        assert!(!valid_peer_id("remote::agent"));
        assert!(!valid_peer_id(&"a".repeat(65)));
    }

    #[test]
    fn ssh_connect_rejects_a_missing_owner_before_network_access() {
        let mut app = test_app();
        let response = app.handle_peer_connect_ssh(
            "connect".into(),
            PeerConnectSshParams {
                target: "must-not-be-contacted".into(),
                ssh_args: Vec::new(),
                managed_control_path: None,
                session: None,
                label: None,
                owner_pane_id: Some("w999:p999".into()),
                owner: None,
            },
        );
        let response: crate::api::schema::ErrorResponse = serde_json::from_str(&response).unwrap();

        assert_eq!(response.error.code, "invalid_ssh_owner");
    }

    #[test]
    fn expired_ssh_shell_bridge_lease_is_released() {
        let mut app = test_app();
        let now = Instant::now();
        app.ssh_shell_bridge_deadlines.insert(
            ("remote".into(), "connection".into()),
            now - Duration::from_millis(1),
        );

        assert!(app.expire_ssh_shell_bridge_leases(now));
        assert!(app.ssh_shell_bridge_deadlines.is_empty());
    }

    #[test]
    fn ssh_keepalive_rejects_an_unknown_connection() {
        let mut app = test_app();
        let response = app.handle_peer_keepalive_ssh(
            "keepalive".into(),
            PeerKeepaliveSshParams {
                peer_id: "remote".into(),
                connection_id: "missing".into(),
            },
        );
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(response["error"]["code"], "peer_connection_not_found");
    }

    #[test]
    fn unregister_advances_lifecycle_even_before_connect_completion() {
        let mut app = test_app();

        let response = app.handle_peer_unregister(
            "unregister".into(),
            PeerTarget {
                peer_id: "workbox".into(),
            },
        );

        let response: serde_json::Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["error"]["code"], "peer_not_found");
        assert_eq!(app.peer_lifecycle_generations["workbox"], 1);
    }

    #[tokio::test]
    async fn unregistering_an_incoming_peer_terminates_its_hosted_delegation() {
        let mut app = test_app();
        app.state.peers.insert(
            "origin".into(),
            api_socket_peer("origin", "/tmp/origin.sock"),
        );
        let delegation = app
            .prepare_delegated_terminal(crate::api::schema::TerminalDelegateCreateParams {
                cwd: Some(std::env::temp_dir().display().to_string()),
                label: Some("remote".into()),
                env: std::collections::HashMap::new(),
                owner: crate::api::schema::TerminalPresentationOwner {
                    peer_id: "origin".into(),
                    pane_id: "origin-pane".into(),
                    route: vec!["origin".into()],
                },
            })
            .expect("prepare delegation");
        app.commit_terminal_delegation(
            &crate::api::schema::TerminalDelegationClaim {
                delegation_id: delegation.delegation_id.clone(),
                epoch: delegation.epoch,
            },
            &delegation.terminal_id,
        )
        .expect("commit delegation");

        let response = app.handle_peer_unregister(
            "unregister".into(),
            PeerTarget {
                peer_id: "origin".into(),
            },
        );
        let response: serde_json::Value = serde_json::from_str(&response).unwrap();

        assert_eq!(response["result"]["type"], "peer_info");
        assert_eq!(app.active_remote_presentation_count(), 0);
        assert_eq!(
            app.terminal_delegation_info(&delegation.delegation_id, delegation.epoch)
                .expect("terminal status")
                .status,
            crate::api::schema::TerminalDelegationStatus::Terminated
        );
    }

    #[test]
    fn incoming_registration_starts_a_lease() {
        let mut app = test_app();
        let before = Instant::now();

        let _ = app.handle_peer_register(
            "register".into(),
            PeerRegisterParams {
                peer: api_socket_peer("origin", "/tmp/origin.sock"),
            },
        );

        assert!(app
            .incoming_peer_deadlines
            .get("origin")
            .is_some_and(|deadline| *deadline > before));
        app.cancel_peer_refresh("origin");
    }

    #[test]
    fn successful_incoming_refresh_renews_its_lease() {
        let mut app = test_app();
        let peer_id = "origin".to_string();
        let generation = 7;
        let old_deadline = Instant::now() - Duration::from_secs(1);
        app.state.peers.insert(
            peer_id.clone(),
            api_socket_peer(&peer_id, "/tmp/origin.sock"),
        );
        app.peer_refresh_generations
            .insert(peer_id.clone(), generation);
        app.incoming_peer_deadlines
            .insert(peer_id.clone(), old_deadline);

        app.apply_peer_refresh(peer_id.clone(), generation, Instant::now(), Ok(Vec::new()));

        assert!(app.incoming_peer_deadlines[&peer_id] > old_deadline);
    }

    #[test]
    fn delayed_refresh_success_does_not_extend_an_already_expired_lease() {
        let mut app = test_app();
        let peer_id = "origin".to_string();
        let generation = 7;
        let now = Instant::now();
        app.state.peers.insert(
            peer_id.clone(),
            api_socket_peer(&peer_id, "/tmp/origin.sock"),
        );
        app.peer_refresh_generations
            .insert(peer_id.clone(), generation);

        app.apply_peer_refresh(
            peer_id.clone(),
            generation,
            now - INCOMING_PEER_LEASE - Duration::from_millis(1),
            Ok(Vec::new()),
        );

        assert!(app.expire_incoming_peer_leases(now));
        assert!(!app.state.peers.contains_key(&peer_id));
    }

    #[test]
    fn expired_incoming_peer_is_removed_and_cancelled() {
        let mut app = test_app();
        let peer_id = "origin".to_string();
        let now = Instant::now();
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        app.state.peers.insert(
            peer_id.clone(),
            api_socket_peer(&peer_id, "/tmp/origin.sock"),
        );
        app.peer_refresh_cancellations
            .insert(peer_id.clone(), std::sync::Arc::clone(&cancelled));
        app.peer_refresh_generations.insert(peer_id.clone(), 3);
        app.incoming_peer_deadlines
            .insert(peer_id.clone(), now - Duration::from_millis(1));

        assert!(app.expire_incoming_peer_leases(now));
        assert!(!app.state.peers.contains_key(&peer_id));
        assert!(!app.incoming_peer_deadlines.contains_key(&peer_id));
        assert!(cancelled.load(std::sync::atomic::Ordering::Acquire));
    }

    #[test]
    fn stale_incoming_deadline_does_not_remove_an_outgoing_peer() {
        let mut app = test_app();
        let peer_id = "remote".to_string();
        let now = Instant::now();
        app.state.peers.insert(peer_id.clone(), ssh_peer(&peer_id));
        app.incoming_peer_deadlines
            .insert(peer_id.clone(), now - Duration::from_millis(1));

        assert!(!app.expire_incoming_peer_leases(now));
        assert!(app.state.peers.contains_key(&peer_id));
        assert!(!app.incoming_peer_deadlines.contains_key(&peer_id));
    }

    #[test]
    fn stale_health_generation_cannot_overwrite_a_reregistered_peer() {
        let mut app = test_app();
        let old_peer = api_socket_peer("origin", "/tmp/new.sock");
        let mut replacement = api_socket_peer("origin", "/tmp/new.sock");
        replacement.status = PeerStatus::Disconnected;
        app.state
            .peers
            .insert(replacement.id.clone(), replacement.clone());
        app.peer_refresh_generations.insert("origin".into(), 2);
        let (respond_to, response_rx) = std::sync::mpsc::channel();

        app.handle_peer_health_finished(crate::events::PeerHealthRequestResult {
            id: "health".into(),
            peer: old_peer,
            generation: 1,
            result: Ok(serde_json::json!({
                "result": { "peer": { "status": "connected" } }
            })),
            respond_to,
        });

        assert_eq!(app.state.peers["origin"], replacement);
        let response: serde_json::Value =
            serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
        assert_eq!(
            response["result"]["peer"]["transport"]["api_socket"],
            "/tmp/new.sock"
        );
        assert_eq!(
            response["result"]["peer"]["status"],
            serde_json::json!("disconnected")
        );
    }

    #[test]
    fn health_completion_for_a_removed_peer_returns_not_found() {
        let mut app = test_app();
        let (respond_to, response_rx) = std::sync::mpsc::channel();

        app.handle_peer_health_finished(crate::events::PeerHealthRequestResult {
            id: "health".into(),
            peer: api_socket_peer("origin", "/tmp/origin.sock"),
            generation: 1,
            result: Ok(serde_json::json!({"result": {"type": "pong"}})),
            respond_to,
        });

        let response: serde_json::Value =
            serde_json::from_str(&response_rx.recv().unwrap()).unwrap();
        assert_eq!(response["error"]["code"], "peer_not_found");
    }
}
