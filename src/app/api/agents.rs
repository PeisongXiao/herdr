use bytes::Bytes;

use crate::api::schema::{
    AgentListParams, AgentReadParams, AgentRenameParams, AgentSendParams, AgentStartParams,
    AgentTarget, ErrorResponse, Method, PaneReadResult, ReadFormat, ReadSource, Request,
    ResponseResult, SuccessResponse,
};
use crate::app::{
    peer_agents::{project_peer_agent_metadata, AgentRoute},
    App,
};

use super::responses::{encode_error, encode_error_body, encode_success};

impl App {
    pub(super) fn should_defer_peer_agent_request(&self, method: &Method) -> bool {
        match method {
            Method::AgentGet(target) | Method::AgentExplain(target) => self
                .resolve_agent_route(&target.target)
                .is_ok_and(|route| matches!(route, AgentRoute::Peer { .. })),
            Method::AgentRename(params) => self
                .resolve_agent_route(&params.target)
                .is_ok_and(|route| matches!(route, AgentRoute::Peer { .. })),
            Method::AgentRead(params) => self
                .resolve_agent_route(&params.target)
                .is_ok_and(|route| matches!(route, AgentRoute::Peer { .. })),
            Method::AgentSend(params) => self
                .resolve_agent_route(&params.target)
                .is_ok_and(|route| matches!(route, AgentRoute::Peer { .. })),
            Method::AgentStart(params) => params.peer.is_some(),
            Method::AgentAttachPrepare(params) => self
                .resolve_agent_route(&params.target)
                .is_ok_and(|route| matches!(route, AgentRoute::Peer { .. })),
            _ => false,
        }
    }

    pub(super) fn begin_peer_agent_request(
        &mut self,
        id: String,
        method: Method,
        respond_to: std::sync::mpsc::Sender<String>,
    ) {
        let owner_terminal_id = match self.peer_agent_request_owner_terminal(&method) {
            Ok(owner_terminal_id) => owner_terminal_id,
            Err(error) => {
                let _ = respond_to.send(encode_error_body(id, error));
                return;
            }
        };
        let (peer, request) = match self.prepare_peer_agent_request(&id, method) {
            Ok(prepared) => prepared,
            Err(response) => {
                let _ = respond_to.send(response);
                return;
            }
        };
        if !self.try_begin_remote_api_job(&id, &respond_to) {
            return;
        }
        self.begin_pending_owner_operation(owner_terminal_id.as_ref());
        let event_tx = self.event_tx.clone();
        let worker_id = id.clone();
        let worker_peer = peer.clone();
        let worker_request = request.clone();
        let worker_owner_terminal_id = owner_terminal_id.clone();
        let worker_respond_to = respond_to.clone();
        let spawn = std::thread::Builder::new()
            .name("herdr-peer-agent-request".into())
            .spawn(move || {
                let result =
                    super::super::peer_agents::peer_request_value(&worker_peer, &worker_request);
                let _ = event_tx.blocking_send(crate::events::AppEvent::PeerAgentRequestFinished(
                    Box::new(crate::events::PeerAgentRequestResult {
                        id: worker_id,
                        peer: worker_peer,
                        request: worker_request,
                        owner_terminal_id: worker_owner_terminal_id,
                        result,
                        respond_to: worker_respond_to,
                    }),
                ));
            });
        if let Err(err) = spawn {
            self.finish_pending_owner_operation(owner_terminal_id.as_ref());
            self.finish_remote_api_job();
            let _ = respond_to.send(encode_error(
                id,
                "peer_unreachable",
                format!("could not start peer request: {err}"),
            ));
        }
    }

    fn peer_agent_request_owner_terminal(
        &self,
        method: &Method,
    ) -> Result<Option<crate::terminal::TerminalId>, crate::api::schema::ErrorBody> {
        let Method::AgentAttachPrepare(params) = method else {
            return Ok(None);
        };
        let terminal_id = self
            .resolve_ssh_owner_terminal(Some(&params.owner_pane_id))?
            .ok_or_else(|| crate::api::schema::ErrorBody {
                code: "invalid_ssh_owner".into(),
                message: "peer attach preparation has no local owner terminal".into(),
            })?;
        Ok(Some(terminal_id))
    }

    fn prepare_peer_agent_request(
        &self,
        response_id: &str,
        method: Method,
    ) -> Result<(crate::api::schema::PeerInfo, Request), String> {
        let routed = |target: &str| {
            self.resolve_agent_route(target)
                .map_err(|error| encode_error_body(response_id.to_string(), error))
        };
        match method {
            Method::AgentGet(target) => match routed(&target.target)? {
                AgentRoute::Peer { peer, target } => Ok((peer, self.peer_agent_get(target))),
                AgentRoute::Local(_) => Err(encode_error(
                    response_id.to_string(),
                    "invalid_request",
                    "local agent request was incorrectly deferred",
                )),
            },
            Method::AgentRename(params) => match routed(&params.target)? {
                AgentRoute::Peer { peer, target } => Ok((
                    peer,
                    Request {
                        id: "peer:agent:rename".into(),
                        method: Method::PeerAgentRename(AgentRenameParams {
                            target,
                            name: params.name,
                        }),
                    },
                )),
                AgentRoute::Local(_) => Err(encode_error(
                    response_id.to_string(),
                    "invalid_request",
                    "local agent request was incorrectly deferred",
                )),
            },
            Method::AgentRead(params) => match routed(&params.target)? {
                AgentRoute::Peer { peer, target } => Ok((
                    peer,
                    Request {
                        id: "peer:agent:read".into(),
                        method: Method::PeerAgentRead(AgentReadParams { target, ..params }),
                    },
                )),
                AgentRoute::Local(_) => Err(encode_error(
                    response_id.to_string(),
                    "invalid_request",
                    "local agent request was incorrectly deferred",
                )),
            },
            Method::AgentExplain(target) => match routed(&target.target)? {
                AgentRoute::Peer { peer, target } => Ok((
                    peer,
                    Request {
                        id: "peer:agent:explain".into(),
                        method: Method::PeerAgentExplain(AgentTarget { target }),
                    },
                )),
                AgentRoute::Local(_) => Err(encode_error(
                    response_id.to_string(),
                    "invalid_request",
                    "local agent request was incorrectly deferred",
                )),
            },
            Method::AgentSend(params) => match routed(&params.target)? {
                AgentRoute::Peer { peer, target } => Ok((
                    peer,
                    Request {
                        id: "peer:agent:send".into(),
                        method: Method::PeerAgentSend(AgentSendParams {
                            target,
                            text: params.text,
                        }),
                    },
                )),
                AgentRoute::Local(_) => Err(encode_error(
                    response_id.to_string(),
                    "invalid_request",
                    "local agent request was incorrectly deferred",
                )),
            },
            Method::AgentStart(mut params) => {
                let Some(peer_id) = params.peer.take() else {
                    return Err(encode_error(
                        response_id.to_string(),
                        "invalid_request",
                        "peer agent start is missing a peer id",
                    ));
                };
                let Some(peer) = self.state.peers.get(&peer_id).cloned() else {
                    return Err(encode_error(
                        response_id.to_string(),
                        "peer_not_found",
                        format!("peer {peer_id} not found"),
                    ));
                };
                if params.focus {
                    return Err(encode_error(
                        response_id.to_string(),
                        "peer_agent_focus_unsupported",
                        format!(
                            "agent.start cannot focus an agent on peer {peer_id}; start without focus, then use `herdr agent attach {peer_id}::{}`",
                            params.name
                        ),
                    ));
                }
                Ok((
                    peer,
                    Request {
                        id: "peer:agent:start".into(),
                        method: Method::PeerAgentStart(params),
                    },
                ))
            }
            Method::AgentAttachPrepare(params) => match routed(&params.target)? {
                AgentRoute::Peer { peer, target } => {
                    let owner = self
                        .delegated_pane_for_public_id(&params.owner_pane_id)
                        .map(|delegated| delegated.info.owner.clone())
                        .unwrap_or_else(|| {
                            let peer_id = crate::remote_agent::local_peer_id();
                            crate::api::schema::TerminalPresentationOwner {
                                peer_id: peer_id.clone(),
                                pane_id: params.owner_pane_id,
                                route: vec![peer_id],
                            }
                        });
                    Ok((
                        peer,
                        Request {
                            id: "peer:terminal:delegate:claim".into(),
                            method: Method::TerminalDelegateClaim(
                                crate::api::schema::TerminalDelegateClaimParams {
                                    target,
                                    owner,
                                    takeover: params.takeover,
                                    terminate_on_expire: false,
                                },
                            ),
                        },
                    ))
                }
                AgentRoute::Local(_) => Err(encode_error(
                    response_id.to_string(),
                    "invalid_request",
                    "local agents do not need remote attach preparation",
                )),
            },
            _ => Err(encode_error(
                response_id.to_string(),
                "invalid_request",
                "method is not a peer agent operation",
            )),
        }
    }

    pub(super) fn handle_peer_agent_request_finished(
        &mut self,
        finished: crate::events::PeerAgentRequestResult,
    ) {
        self.finish_pending_owner_operation(finished.owner_terminal_id.as_ref());
        let response = encode_peer_request_result(
            self,
            finished.id,
            &finished.peer,
            &finished.request,
            finished.owner_terminal_id.as_ref(),
            finished.result,
        );
        let pending_delegation_id = serde_json::from_str::<serde_json::Value>(&response)
            .ok()
            .and_then(|value| {
                value["result"]["prepared"]["delegation"]["delegation_id"]
                    .as_str()
                    .map(str::to_string)
            });
        if finished.respond_to.send(response).is_err() {
            if let Some(delegation_id) = pending_delegation_id {
                self.cancel_pending_owner_activation(&delegation_id);
            }
        }
    }

    pub(super) fn handle_agent_list(&mut self, id: String, params: AgentListParams) -> String {
        encode_success(
            id,
            ResponseResult::AgentList {
                agents: self.collect_agent_infos_with_peers(params.include_peers),
            },
        )
    }

    pub(super) fn handle_peer_agent_list(&mut self, id: String) -> String {
        encode_success(
            id,
            ResponseResult::AgentList {
                agents: self
                    .collect_agent_infos()
                    .into_iter()
                    .map(project_peer_agent_metadata)
                    .collect(),
            },
        )
    }

    pub(super) fn handle_agent_get(&mut self, id: String, target: AgentTarget) -> String {
        let route = match self.resolve_agent_route(&target.target) {
            Ok(route) => route,
            Err(err) => return encode_error_body(id, err),
        };
        match route {
            AgentRoute::Local(resolved) => {
                let Some(agent) = self.agent_info(resolved.ws_idx, resolved.pane_id) else {
                    return agent_not_found(id, &target.target);
                };
                encode_success(id, ResponseResult::AgentInfo { agent })
            }
            AgentRoute::Peer { peer, target } => {
                let request = self.peer_agent_get(target);
                self.forward_peer_agent_response(id, &peer, request)
            }
        }
    }

    pub(super) fn handle_peer_agent_get(&mut self, id: String, target: AgentTarget) -> String {
        if let Some(agent) = self.delegated_agent_info_for_target(&target.target) {
            return encode_success(
                id,
                ResponseResult::AgentInfo {
                    agent: project_peer_agent_metadata(agent),
                },
            );
        }
        let resolved = match self.resolve_terminal_target(&target.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some(agent) = self.agent_info(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &target.target);
        };
        encode_success(
            id,
            ResponseResult::AgentInfo {
                agent: project_peer_agent_metadata(agent),
            },
        )
    }

    pub(super) fn handle_agent_focus(&mut self, id: String, target: AgentTarget) -> String {
        let route = match self.resolve_agent_route(&target.target) {
            Ok(route) => route,
            Err(err) => return encode_error_body(id, err),
        };
        let agent = match route {
            AgentRoute::Local(resolved) => {
                self.state
                    .focus_pane_in_workspace(resolved.ws_idx, resolved.pane_id);
                self.state.mark_active_tab_seen();
                self.state.settle_terminal_mode_after_focus();
                match self.agent_info(resolved.ws_idx, resolved.pane_id) {
                    Some(agent) => agent,
                    None => return agent_not_found(id, &target.target),
                }
            }
            AgentRoute::Peer { peer, target } => return encode_error(
                id,
                "peer_agent_focus_unsupported",
                format!(
                    "agent.focus cannot focus an agent on peer {}; use `herdr agent attach {}::{target}` instead",
                    peer.id, peer.id
                ),
            ),
        };

        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_rename(&mut self, id: String, params: AgentRenameParams) -> String {
        let route = match self.resolve_agent_route(&params.target) {
            Ok(route) => route,
            Err(err) => return encode_error_body(id, err),
        };
        match route {
            AgentRoute::Local(_) => self.handle_peer_agent_rename(id, params),
            AgentRoute::Peer { peer, target } => self.forward_peer_agent_response(
                id,
                &peer,
                Request {
                    id: "peer:agent:rename".into(),
                    method: Method::PeerAgentRename(AgentRenameParams {
                        target,
                        name: params.name,
                    }),
                },
            ),
        }
    }

    pub(super) fn handle_peer_agent_rename(
        &mut self,
        id: String,
        params: AgentRenameParams,
    ) -> String {
        let agent = match self.rename_agent_target(&params.target, params.name) {
            Ok(agent) => agent,
            Err(err) => return encode_error_body(id, self.agent_rename_error_body(err)),
        };
        encode_success(id, ResponseResult::AgentInfo { agent })
    }

    pub(super) fn handle_agent_start(&mut self, id: String, params: AgentStartParams) -> String {
        if let Some(peer_id) = params.peer.clone() {
            let Some(peer) = self.state.peers.get(&peer_id).cloned() else {
                return encode_error(id, "peer_not_found", format!("peer {peer_id} not found"));
            };
            if params.focus {
                return encode_error(
                    id,
                    "peer_agent_focus_unsupported",
                    format!(
                        "agent.start cannot focus an agent on peer {peer_id}; start without focus, then use `herdr agent attach {peer_id}::{}`",
                        params.name
                    ),
                );
            }
            let mut peer_params = params;
            peer_params.peer = None;
            return self.forward_peer_agent_response(
                id,
                &peer,
                Request {
                    id: "peer:agent:start".into(),
                    method: Method::PeerAgentStart(peer_params),
                },
            );
        }
        self.start_agent_response(id, params)
    }

    pub(super) fn handle_peer_agent_start(
        &mut self,
        id: String,
        params: AgentStartParams,
    ) -> String {
        if let Some(peer_id) = params.peer.as_deref() {
            return encode_error(
                id,
                "invalid_peer_target",
                format!(
                    "peer.agent.start cannot forward to peer {peer_id}; send the request directly to that peer"
                ),
            );
        }
        if params.transport.is_some() {
            return encode_error(
                id,
                "invalid_peer_transport",
                "peer.agent.start only starts agents on the receiving server",
            );
        }
        if params.focus {
            return encode_error(
                id,
                "peer_agent_focus_unsupported",
                "peer.agent.start cannot change focus on the receiving server",
            );
        }
        self.start_agent_response(id, params)
    }

    fn start_agent_response(&mut self, id: String, params: AgentStartParams) -> String {
        let extra_env = match super::env::normalize_launch_env(params.env.clone()) {
            Ok(env) => env,
            Err((code, message)) => return encode_error(id, &code, message),
        };
        let (agent, argv) = match self.start_agent(params, extra_env) {
            Ok(started) => started,
            Err(err) => return encode_error_body(id, self.agent_start_error_body(err)),
        };

        encode_success(id, ResponseResult::AgentStarted { agent, argv })
    }

    pub(super) fn handle_agent_read(&mut self, id: String, params: AgentReadParams) -> String {
        let route = match self.resolve_agent_route(&params.target) {
            Ok(route) => route,
            Err(err) => return encode_error_body(id, err),
        };
        match route {
            AgentRoute::Local(_) => self.handle_peer_agent_read(id, params),
            AgentRoute::Peer { peer, target } => self.forward_peer_response(
                id,
                &peer,
                Request {
                    id: "peer:agent:read".into(),
                    method: Method::PeerAgentRead(AgentReadParams { target, ..params }),
                },
            ),
        }
    }

    pub(super) fn handle_peer_agent_read(&mut self, id: String, params: AgentReadParams) -> String {
        let resolved = match self.resolve_terminal_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some((pane, workspace_id)) = self.lookup_runtime(resolved.ws_idx, resolved.pane_id)
        else {
            return agent_not_found(id, &params.target);
        };
        let requested_lines = params.lines.unwrap_or(80).min(1000) as usize;
        let text = match params.format {
            ReadFormat::Text => match params.source {
                ReadSource::Visible => pane.visible_text(),
                ReadSource::Recent => pane.recent_text(requested_lines),
                ReadSource::RecentUnwrapped => pane.recent_unwrapped_text(requested_lines),
                ReadSource::Detection => pane.detection_text(),
            },
            ReadFormat::Ansi => match params.source {
                ReadSource::Visible => pane.visible_ansi(),
                ReadSource::Recent => pane.recent_ansi(requested_lines),
                ReadSource::RecentUnwrapped => pane.recent_unwrapped_ansi(requested_lines),
                ReadSource::Detection => pane.detection_text(),
            },
        };

        encode_success(
            id,
            ResponseResult::PaneRead {
                read: PaneReadResult {
                    pane_id: self
                        .public_pane_id(resolved.ws_idx, resolved.pane_id)
                        .unwrap_or_else(|| params.target.clone()),
                    workspace_id,
                    tab_id: self
                        .public_tab_id(resolved.ws_idx, resolved.tab_idx)
                        .unwrap(),
                    source: params.source,
                    format: params.format,
                    text,
                    revision: 0,
                    truncated: false,
                },
            },
        )
    }

    pub(super) fn handle_agent_explain(&mut self, id: String, target: AgentTarget) -> String {
        let route = match self.resolve_agent_route(&target.target) {
            Ok(route) => route,
            Err(err) => return encode_error_body(id, err),
        };
        match route {
            AgentRoute::Local(_) => self.handle_peer_agent_explain(id, target),
            AgentRoute::Peer { peer, target } => self.forward_peer_response(
                id,
                &peer,
                Request {
                    id: "peer:agent:explain".into(),
                    method: Method::PeerAgentExplain(AgentTarget { target }),
                },
            ),
        }
    }

    pub(super) fn handle_peer_agent_explain(&mut self, id: String, target: AgentTarget) -> String {
        let resolved = match self.resolve_terminal_target(&target.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some((pane, _workspace_id)) = self.lookup_runtime(resolved.ws_idx, resolved.pane_id)
        else {
            return agent_not_found(id, &target.target);
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(resolved.ws_idx)
            .and_then(|workspace| workspace.terminal_id(resolved.pane_id))
        else {
            return agent_not_found(id, &target.target);
        };
        let Some(terminal) = self.state.terminals.get(terminal_id) else {
            return agent_not_found(id, &target.target);
        };
        if terminal.full_lifecycle_hook_authority_active() {
            let explain = serde_json::json!({
                "agent": terminal.effective_agent_label().unwrap_or("unknown"),
                "state": crate::detect::manifest::agent_state_label(terminal.state),
                "manifest_source": null,
                "manifest_version": null,
                "cached_remote_version": null,
                "local_override_shadowing_remote": false,
                "remote_update_status": null,
                "remote_update_error": null,
                "matched_rule": null,
                "visible_idle": false,
                "visible_blocker": false,
                "visible_working": false,
                "screen_detection_skipped": true,
                "screen_detection_skip_reason": "full_lifecycle_hook_authority",
                "skip_state_update": false,
                "skipped_update_reason": null,
                "fallback_reason": null,
                "warning": null,
                "evaluated_rules": [],
            });
            return encode_success(id, ResponseResult::AgentExplain { explain });
        }
        let Some(agent) = terminal.effective_known_agent().or(terminal.detected_agent) else {
            return encode_error(
                id,
                "agent_explain_unavailable",
                format!(
                    "agent target {} does not have a detected agent label",
                    target.target
                ),
            );
        };

        let screen = pane.detection_text();
        let osc_title = pane.agent_osc_title();
        let osc_progress = pane.agent_osc_progress();
        let explain = crate::detect::manifest::explain_with_input(
            agent,
            crate::detect::manifest::DetectionInput {
                screen: &screen,
                osc_title: &osc_title,
                osc_progress: &osc_progress,
            },
        );
        let value = crate::detect::manifest::explain_to_json_value(&explain);

        encode_success(id, ResponseResult::AgentExplain { explain: value })
    }

    pub(super) fn handle_agent_send(&mut self, id: String, params: AgentSendParams) -> String {
        let route = match self.resolve_agent_route(&params.target) {
            Ok(route) => route,
            Err(err) => return encode_error_body(id, err),
        };
        match route {
            AgentRoute::Local(_) => self.handle_peer_agent_send(id, params),
            AgentRoute::Peer { peer, target } => self.forward_peer_response(
                id,
                &peer,
                Request {
                    id: "peer:agent:send".into(),
                    method: Method::PeerAgentSend(AgentSendParams {
                        target,
                        text: params.text,
                    }),
                },
            ),
        }
    }

    pub(super) fn handle_peer_agent_send(&mut self, id: String, params: AgentSendParams) -> String {
        let resolved = match self.resolve_terminal_target(&params.target) {
            Ok(resolved) => resolved,
            Err(err) => return encode_error_body(id, self.agent_target_error_body(err)),
        };
        let Some(runtime) = self.lookup_runtime_sender(resolved.ws_idx, resolved.pane_id) else {
            return agent_not_found(id, &params.target);
        };
        if let Err(err) = runtime.try_send_bytes(Bytes::from(params.text)) {
            return encode_error(id, "agent_send_failed", err.to_string());
        }

        encode_success(
            id,
            ResponseResult::Ok {
                terminated_remote_presentations: None,
                handed_off_remote_presentations: None,
            },
        )
    }

    fn forward_peer_response(
        &mut self,
        id: String,
        peer: &crate::api::schema::PeerInfo,
        request: Request,
    ) -> String {
        let result = self.peer_request_value(peer, &request);
        encode_peer_request_result(self, id, peer, &request, None, result)
    }

    fn forward_peer_agent_response(
        &mut self,
        id: String,
        peer: &crate::api::schema::PeerInfo,
        request: Request,
    ) -> String {
        let result = self.peer_request_value(peer, &request);
        encode_peer_request_result(self, id, peer, &request, None, result)
    }
}

fn encode_peer_request_result(
    app: &mut App,
    id: String,
    peer: &crate::api::schema::PeerInfo,
    request: &Request,
    owner_terminal_id: Option<&crate::terminal::TerminalId>,
    result: Result<serde_json::Value, String>,
) -> String {
    let response = match result {
        Ok(response) => response,
        Err(message) => return encode_error(id, "peer_unreachable", message),
    };
    if response.get("error").is_some() {
        return match serde_json::from_value::<ErrorResponse>(response) {
            Ok(error) => encode_error_body(id, error.error),
            Err(err) => encode_error(
                id,
                "peer_response_invalid",
                format!("peer returned an invalid error response: {err}"),
            ),
        };
    }
    match sanitize_peer_agent_success(app, peer, request, owner_terminal_id, response) {
        Ok(result) => encode_success(id, result),
        Err(message) => encode_error(id, "peer_response_invalid", message),
    }
}

fn agent_not_found(id: String, target: &str) -> String {
    encode_error(
        id,
        "agent_not_found",
        format!("agent target {target} not found"),
    )
}

fn sanitize_peer_agent_success(
    app: &mut App,
    peer: &crate::api::schema::PeerInfo,
    request: &Request,
    owner_terminal_id: Option<&crate::terminal::TerminalId>,
    response: serde_json::Value,
) -> Result<ResponseResult, String> {
    let success = serde_json::from_value::<SuccessResponse>(response)
        .map_err(|err| format!("peer returned an invalid agent response: {err}"))?;
    match (&request.method, success.result) {
        (
            Method::PeerAgentGet(_) | Method::PeerAgentRename(_),
            ResponseResult::AgentInfo { mut agent },
        ) => {
            app.annotate_peer_agent(peer, &mut agent);
            Ok(ResponseResult::AgentInfo { agent })
        }
        (Method::PeerAgentStart(_), ResponseResult::AgentStarted { mut agent, argv }) => {
            app.annotate_peer_agent(peer, &mut agent);
            Ok(ResponseResult::AgentStarted { agent, argv })
        }
        (Method::PeerAgentRead(_), result @ ResponseResult::PaneRead { .. })
        | (Method::PeerAgentExplain(_), result @ ResponseResult::AgentExplain { .. })
        | (Method::PeerAgentSend(_), result @ ResponseResult::Ok { .. }) => Ok(result),
        (Method::TerminalDelegateClaim(_), ResponseResult::TerminalDelegation { delegation }) => {
            let crate::api::schema::PeerTransportInfo::Ssh {
                target,
                ssh_args,
                managed_control_path,
                session,
            } = &peer.transport
            else {
                return Err("peer does not provide an SSH attach transport".into());
            };
            let attach = crate::api::schema::AgentAttachInfo::Ssh {
                target: target.clone(),
                ssh_args: ssh_args.clone(),
                managed_control_path: managed_control_path.clone(),
                session: session.clone(),
                terminal_id: delegation.terminal_id.clone(),
                delegation: Some(crate::api::schema::TerminalDelegationClaim {
                    delegation_id: delegation.delegation_id.clone(),
                    epoch: delegation.epoch,
                }),
            };
            let Some(owner_terminal_id) = owner_terminal_id else {
                return Err("peer attach preparation lost its local owner context".into());
            };
            if !app.state.terminals.contains_key(owner_terminal_id) {
                return Err("peer attach owner pane closed while preparation was running".into());
            }
            let owner_terminal_id = owner_terminal_id.clone();
            let connection_id = app
                .peer_bridges
                .get(&peer.id)
                .and_then(|bridges| bridges.latest())
                .map(|bridge| bridge.connection_id().to_string())
                .ok_or_else(|| "peer does not have an active SSH bridge".to_string())?;
            app.register_pending_owner_activation(
                owner_terminal_id,
                peer.clone(),
                connection_id,
                delegation.clone(),
                attach.clone(),
                false,
            );
            Ok(ResponseResult::AgentAttachPrepared {
                prepared: crate::api::schema::AgentAttachPrepared {
                    attach,
                    delegation: Some(delegation),
                },
            })
        }
        _ => Err("peer returned the wrong response type for the requested agent operation".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{
        AgentAttachInfo, AgentInfo, AgentStatus, AgentTransportInfo, EmptyParams, ErrorResponse,
        PeerInfo, PeerStatus, PeerTransportInfo, Request,
    };
    use crate::app::Mode;
    use crate::detect::{Agent, AgentState};
    use crate::workspace::Workspace;

    fn app_with_agent() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![Workspace::test_new("agent")];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        app.state.mode = Mode::Terminal;
        app
    }

    fn app_with_loop_peer() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.peers.insert(
            "loop".into(),
            PeerInfo {
                id: "loop".into(),
                label: "loop".into(),
                status: PeerStatus::Connected,
                transport: PeerTransportInfo::ApiSocket {
                    api_socket: "/path/that/must/not/be-opened.sock".into(),
                },
            },
        );
        app
    }

    #[test]
    fn agent_focus_marks_already_focused_done_agent_seen() {
        let mut app = app_with_agent();
        app.state.outer_terminal_focus = Some(false);

        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(Some(Agent::Pi), AgentState::Idle);
        app.state.workspaces[0].tabs[0]
            .panes
            .get_mut(&pane_id)
            .unwrap()
            .seen = false;
        app.state.workspaces[0].tabs[0].layout.focus_pane(pane_id);

        let response = app.handle_agent_focus(
            "req".into(),
            AgentTarget {
                target: "pi".into(),
            },
        );

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("expected agent info response");
        };
        assert_eq!(agent.agent_status, AgentStatus::Idle);
    }

    fn add_sensitive_local_agent(app: &mut App) -> AgentInfo {
        let workspace = Workspace::test_new("peer-metadata");
        let root = workspace.tabs[0].root_pane;
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app.state.selected = 0;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&root]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
        terminal.set_agent_name("remote-mirror".into());
        terminal.set_persisted_agent_session(crate::agent_resume::PersistedAgentSession {
            source: "herdr:codex".into(),
            agent: "codex".into(),
            session_ref: crate::agent_resume::AgentSessionRef::id("native-session").unwrap(),
        });
        terminal.remote_agent_transport = Some(AgentTransportInfo::Ssh {
            target: "remote.example".into(),
            ssh_args: vec!["-p".into(), "2222".into()],
            managed_control_path: Some("/tmp/herdr-control".into()),
            session: Some("remote-shell".into()),
            remote_terminal_id: "remote-terminal".into(),
            remote_pane_id: "remote-pane".into(),
            remote_agent: Some("codex".into()),
            remote_cwd: Some("/remote/worktree".into()),
        });

        app.agent_info(0, root).unwrap()
    }

    fn assert_safe_peer_metadata(actual: &AgentInfo, raw: &AgentInfo) {
        let mut expected = raw.clone();
        expected.mirror_of_terminal_id = raw.transport.as_ref().map(|transport| match transport {
            AgentTransportInfo::Ssh {
                remote_terminal_id, ..
            } => remote_terminal_id.clone(),
        });
        expected.agent_session = None;
        expected.transport = None;
        expected.attach = None;
        expected.cwd = None;
        expected.foreground_cwd = None;
        assert_eq!(actual, &expected);
    }

    #[test]
    fn peer_agent_list_and_get_expose_safe_metadata() {
        let mut app = app_with_loop_peer();
        let raw = add_sensitive_local_agent(&mut app);
        assert!(raw.agent_session.is_some());
        assert!(raw.transport.is_some());

        let list_response = app.handle_api_request(Request {
            id: "peer_list".into(),
            method: Method::PeerAgentList(EmptyParams::default()),
        });
        let list_response: SuccessResponse = serde_json::from_str(&list_response).unwrap();
        let ResponseResult::AgentList { agents } = list_response.result else {
            panic!("expected peer agent list response");
        };
        assert_eq!(agents.len(), 1);
        assert_safe_peer_metadata(&agents[0], &raw);

        let get_response = app.handle_api_request(Request {
            id: "peer_get".into(),
            method: Method::PeerAgentGet(AgentTarget {
                target: raw.terminal_id.clone(),
            }),
        });
        let get_response: SuccessResponse = serde_json::from_str(&get_response).unwrap();
        let ResponseResult::AgentInfo { agent } = get_response.result else {
            panic!("expected peer agent info response");
        };
        assert_safe_peer_metadata(&agent, &raw);

        let local_get_response = app.handle_api_request(Request {
            id: "local_get".into(),
            method: Method::AgentGet(AgentTarget {
                target: raw.terminal_id.clone(),
            }),
        });
        let local_get_response: SuccessResponse =
            serde_json::from_str(&local_get_response).unwrap();
        let ResponseResult::AgentInfo { agent } = local_get_response.result else {
            panic!("expected local agent info response");
        };
        let expected_local: AgentInfo =
            serde_json::from_value(serde_json::to_value(&raw).unwrap()).unwrap();
        assert_eq!(agent, expected_local);
        assert!(agent.agent_session.is_some());
        assert!(agent.transport.is_some());
    }

    #[test]
    fn peer_metadata_projection_removes_untrusted_attach_data() {
        let mut app = app_with_loop_peer();
        let mut raw = add_sensitive_local_agent(&mut app);
        raw.attach = Some(AgentAttachInfo::SshShell {
            target: "attacker".into(),
            ssh_args: vec!["-oProxyCommand=run-local-code".into()],
            managed_control_path: Some("/tmp/untrusted-control".into()),
            session: Some("untrusted-session".into()),
            label: Some("untrusted-label".into()),
        });

        let projected = project_peer_agent_metadata(raw.clone());

        assert_safe_peer_metadata(&projected, &raw);
    }

    #[test]
    fn peer_agent_get_does_not_recurse_through_registered_peer() {
        let mut app = app_with_loop_peer();
        let response = app.handle_api_request(Request {
            id: "peer_get".into(),
            method: Method::PeerAgentGet(AgentTarget {
                target: "loop::missing".into(),
            }),
        });
        let response: ErrorResponse = serde_json::from_str(&response).unwrap();

        assert_eq!(response.id, "peer_get");
        assert_eq!(response.error.code, "agent_not_found");
    }

    #[test]
    fn normal_agent_focus_rejects_peer_presentation_changes() {
        let mut app = app_with_loop_peer();
        let response = app.handle_api_request(Request {
            id: "peer_focus".into(),
            method: Method::AgentFocus(AgentTarget {
                target: "loop::agent".into(),
            }),
        });
        let response: ErrorResponse = serde_json::from_str(&response).unwrap();

        assert_eq!(response.error.code, "peer_agent_focus_unsupported");
        assert!(response.error.message.contains("herdr agent attach"));
    }

    #[test]
    fn peer_annotation_replaces_untrusted_attach_transport() {
        let app = app_with_loop_peer();
        let mut agent: AgentInfo = serde_json::from_value(serde_json::json!({
            "terminal_id": "untrusted",
            "agent_status": "working",
            "attach": {
                "type": "ssh",
                "target": "attacker",
                "ssh_args": ["-oProxyCommand=run-local-code"],
                "terminal_id": "untrusted"
            },
            "transport": {
                "type": "ssh",
                "target": "attacker",
                "remote_terminal_id": "untrusted",
                "remote_pane_id": "w1:p9"
            },
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "pane_id": "w1:p1",
            "focused": false,
            "revision": 0
        }))
        .unwrap();
        let api_peer = app.state.peers.get("loop").unwrap().clone();

        app.annotate_peer_agent(&api_peer, &mut agent);
        assert!(agent.attach.is_none());
        assert!(agent.transport.is_none());

        let ssh_peer = PeerInfo {
            id: "trusted".into(),
            label: "workbox".into(),
            status: PeerStatus::Connected,
            transport: PeerTransportInfo::Ssh {
                target: "workbox".into(),
                ssh_args: vec!["-p".into(), "2222".into()],
                managed_control_path: None,
                session: Some("agents".into()),
            },
        };
        app.annotate_peer_agent(&ssh_peer, &mut agent);
        let Some(AgentAttachInfo::Ssh {
            target, ssh_args, ..
        }) = agent.attach
        else {
            panic!("expected trusted SSH attach metadata");
        };
        assert_eq!(target, "workbox");
        assert_eq!(ssh_args, ["-p", "2222"]);
    }

    #[test]
    fn malformed_peer_agent_response_is_rejected_before_attach_metadata_can_escape() {
        let mut app = app_with_loop_peer();
        let peer = app.state.peers.get("loop").unwrap().clone();
        let request = Request {
            id: "peer_get".into(),
            method: Method::PeerAgentGet(AgentTarget {
                target: "agent".into(),
            }),
        };
        let response = serde_json::json!({
            "id": "peer_get",
            "result": {
                "type": "agent_info",
                "agent": {
                    "terminal_id": "untrusted",
                    "agent_status": "working",
                    "attach": {
                        "type": "ssh",
                        "target": "attacker",
                        "ssh_args": ["-oProxyCommand=run-local-code"],
                        "terminal_id": "untrusted"
                    },
                    "workspace_id": "w1",
                    "tab_id": "w1:t1",
                    "pane_id": "w1:p1",
                    "revision": 0
                }
            }
        });

        let error =
            sanitize_peer_agent_success(&mut app, &peer, &request, None, response).unwrap_err();

        assert!(error.contains("invalid agent response"));
    }

    #[tokio::test]
    async fn nested_peer_attach_keeps_the_local_hidden_owner_terminal_context() {
        let mut app = app_with_loop_peer();
        let workspace = Workspace::test_new("nested-owner");
        let pane_id = workspace.tabs[0].root_pane;
        let public_pane_id = crate::workspace::public_pane_id_for_number(&workspace.id, 1);
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        let terminal_id = app.state.workspaces[0]
            .terminal_id(pane_id)
            .expect("terminal")
            .clone();
        app.terminal_runtimes.insert(
            terminal_id.clone(),
            crate::terminal::TerminalRuntime::test_with_screen_bytes(80, 24, b""),
        );
        let delegation = app
            .prepare_existing_terminal_delegation(crate::api::schema::TerminalDelegateClaimParams {
                target: terminal_id.to_string(),
                owner: crate::api::schema::TerminalPresentationOwner {
                    peer_id: "origin-a".into(),
                    pane_id: "a-owner-pane".into(),
                    route: vec!["origin-a".into()],
                },
                takeover: false,
                terminate_on_expire: false,
            })
            .expect("prepare delegated owner");
        app.commit_terminal_delegation(
            &crate::api::schema::TerminalDelegationClaim {
                delegation_id: delegation.delegation_id,
                epoch: delegation.epoch,
            },
            &terminal_id.to_string(),
        )
        .expect("commit delegated owner");
        let method = Method::AgentAttachPrepare(crate::api::schema::AgentAttachPrepareParams {
            target: "loop::worker".into(),
            owner_pane_id: public_pane_id,
            takeover: false,
        });

        assert_eq!(
            app.peer_agent_request_owner_terminal(&method)
                .expect("resolve local owner"),
            Some(terminal_id)
        );
        let (_, request) = app
            .prepare_peer_agent_request("attach", method)
            .expect("prepare peer attach request");
        let Method::TerminalDelegateClaim(params) = request.method else {
            panic!("expected terminal delegation claim");
        };
        assert_eq!(params.owner.pane_id, "a-owner-pane");
    }
}
