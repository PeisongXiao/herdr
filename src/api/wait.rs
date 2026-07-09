use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use regex::Regex;

use crate::api::schema::{
    ErrorBody, ErrorResponse, EventData, EventEnvelope, EventKind, EventMatch, EventsWaitParams,
    Method, Request, ResponseResult, Subscription, SubscriptionEventData,
    SubscriptionEventEnvelope, SuccessResponse,
};
use crate::api::server::{
    dispatch_to_app_with_timeout, should_stop_connection, APP_RESPONSE_TIMEOUT,
    CONNECTION_POLL_INTERVAL,
};
use crate::api::subscriptions::ActiveSubscription;
use crate::api::subscriptions::{match_output, output_match_read_source};
use crate::api::{ApiRequestSender, EventHub};
use crate::ipc::LocalStream;

const MAX_EVENT_WAIT_AGENT_STATUSES: usize = 5;

pub(super) fn wait_for_output(
    request_id: String,
    params: crate::api::schema::PaneWaitForOutputParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    crate::logging::api_wait_started(&request_id, &params.pane_id, params.timeout_ms);
    let deadline = params
        .timeout_ms
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));

    let regex = match &params.r#match {
        crate::api::schema::OutputMatch::Regex { value } => match Regex::new(value) {
            Ok(regex) => Some(regex),
            Err(err) => {
                return Ok(Some(
                    serde_json::to_string(&ErrorResponse {
                        id: request_id,
                        error: ErrorBody {
                            code: "invalid_regex".into(),
                            message: err.to_string(),
                        },
                    })
                    .unwrap(),
                ));
            }
        },
        crate::api::schema::OutputMatch::Substring { .. } => None,
    };

    loop {
        if should_stop_connection(stream, running)? {
            crate::logging::api_wait_completed(&request_id, &params.pane_id, "client_disconnected");
            return Ok(None);
        }

        let read_request = Request {
            id: format!("{request_id}:read"),
            method: Method::PaneRead(crate::api::schema::PaneReadParams {
                pane_id: params.pane_id.clone(),
                source: output_match_read_source(&params.source),
                lines: params.lines,
                format: crate::api::schema::ReadFormat::Text,
                strip_ansi: params.strip_ansi,
            }),
        };
        let response =
            dispatch_to_app_with_timeout(read_request, api_tx, Some(APP_RESPONSE_TIMEOUT));
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&response) else {
            return Ok(Some(response));
        };
        if value.get("error").is_some() {
            let mut value = value;
            value["id"] = serde_json::Value::String(request_id.clone());
            return Ok(Some(serde_json::to_string(&value).unwrap()));
        }

        let read_value = value["result"]["read"].clone();
        let Ok(read) = serde_json::from_value::<crate::api::schema::PaneReadResult>(read_value)
        else {
            return Ok(Some(
                serde_json::to_string(&ErrorResponse {
                    id: request_id,
                    error: ErrorBody {
                        code: "internal_error".into(),
                        message: "failed to decode pane read result".into(),
                    },
                })
                .unwrap(),
            ));
        };

        let matched_line = match_output(&read.text, &params.r#match, regex.as_ref());
        if matched_line.is_some() {
            let revision = read.revision;
            crate::logging::api_wait_completed(&request_id, &params.pane_id, "matched");
            return Ok(Some(
                serde_json::to_string(&SuccessResponse {
                    id: request_id,
                    result: ResponseResult::OutputMatched {
                        pane_id: read.pane_id.clone(),
                        revision,
                        matched_line,
                        read,
                    },
                })
                .unwrap(),
            ));
        }

        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            crate::logging::api_wait_timed_out(&request_id, &params.pane_id);
            return Ok(Some(
                serde_json::to_string(&ErrorResponse {
                    id: request_id,
                    error: ErrorBody {
                        code: "timeout".into(),
                        message: "timed out waiting for output match".into(),
                    },
                })
                .unwrap(),
            ));
        }

        std::thread::sleep(CONNECTION_POLL_INTERVAL);
    }
}

pub(super) fn wait_for_event(
    request_id: String,
    params: EventsWaitParams,
    stream: &mut LocalStream,
    api_tx: &ApiRequestSender,
    event_hub: &EventHub,
    running: &Arc<AtomicBool>,
) -> std::io::Result<Option<String>> {
    let deadline = params
        .timeout_ms
        .map(|ms| std::time::Instant::now() + std::time::Duration::from_millis(ms));

    let subscriptions = match event_match_subscriptions(&request_id, params.match_event) {
        Ok(subscriptions) => subscriptions,
        Err(response) => return Ok(Some(serde_json::to_string(&response).unwrap())),
    };
    let mut active = Vec::with_capacity(subscriptions.len());
    for (index, subscription) in subscriptions.into_iter().enumerate() {
        match ActiveSubscription::new(subscription, &request_id, index, api_tx, event_hub) {
            Ok(subscription) => active.push(subscription),
            Err(response) => return Ok(Some(serde_json::to_string(&response).unwrap())),
        }
    }

    loop {
        if should_stop_connection(stream, running)? {
            return Ok(None);
        }

        for subscription in &mut active {
            if let Some(event) = subscription.poll(api_tx, event_hub) {
                return Ok(Some(wait_matched_response(&request_id, event)));
            }
        }

        if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            return Ok(Some(
                serde_json::to_string(&ErrorResponse {
                    id: request_id,
                    error: ErrorBody {
                        code: "timeout".into(),
                        message: "timed out waiting for event match".into(),
                    },
                })
                .unwrap(),
            ));
        }

        std::thread::sleep(CONNECTION_POLL_INTERVAL);
    }
}

fn event_match_subscriptions(
    request_id: &str,
    match_event: EventMatch,
) -> Result<Vec<Subscription>, ErrorResponse> {
    match match_event {
        EventMatch::PaneAgentStatusChanged {
            pane_id,
            agent_status,
        } => Ok(vec![Subscription::PaneAgentStatusChanged {
            pane_id,
            agent_status: Some(agent_status),
        }]),
        EventMatch::PaneAgentStatusChangedAny {
            pane_id,
            agent_statuses,
        } => {
            if agent_statuses.is_empty() {
                return Err(invalid_event_wait_match(
                    request_id,
                    "agent_statuses must contain at least one status",
                ));
            }
            if agent_statuses.len() > MAX_EVENT_WAIT_AGENT_STATUSES {
                return Err(invalid_event_wait_match(
                    request_id,
                    &format!(
                        "agent_statuses cannot contain more than {MAX_EVENT_WAIT_AGENT_STATUSES} statuses"
                    ),
                ));
            }

            let mut unique_statuses = Vec::with_capacity(agent_statuses.len());
            for status in agent_statuses {
                if !unique_statuses.contains(&status) {
                    unique_statuses.push(status);
                }
            }
            Ok(unique_statuses
                .into_iter()
                .map(|agent_status| Subscription::PaneAgentStatusChanged {
                    pane_id: pane_id.clone(),
                    agent_status: Some(agent_status),
                })
                .collect())
        }
        _ => Err(ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "unsupported_event_wait_match".into(),
                message: "events.wait currently supports pane agent status matches".into(),
            },
        }),
    }
}

fn invalid_event_wait_match(request_id: &str, message: &str) -> ErrorResponse {
    ErrorResponse {
        id: request_id.into(),
        error: ErrorBody {
            code: "invalid_event_wait_match".into(),
            message: message.into(),
        },
    }
}

fn wait_matched_response(request_id: &str, event: serde_json::Value) -> String {
    let Ok(event) = serde_json::from_value::<SubscriptionEventEnvelope>(event) else {
        return serde_json::to_string(&ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "internal_error".into(),
                message: "failed to decode matched event".into(),
            },
        })
        .unwrap();
    };

    let SubscriptionEventData::PaneAgentStatusChanged(data) = event.data else {
        return serde_json::to_string(&ErrorResponse {
            id: request_id.into(),
            error: ErrorBody {
                code: "unsupported_event_wait_match".into(),
                message: "events.wait currently supports pane agent status matches".into(),
            },
        })
        .unwrap();
    };

    serde_json::to_string(&SuccessResponse {
        id: request_id.into(),
        result: ResponseResult::WaitMatched {
            event: EventEnvelope {
                event: EventKind::PaneAgentStatusChanged,
                data: EventData::PaneAgentStatusChanged {
                    pane_id: data.pane_id,
                    workspace_id: data.workspace_id,
                    agent_status: data.agent_status,
                    agent: data.agent,
                    title: data.title,
                    display_agent: data.display_agent,
                    custom_status: data.custom_status,
                    state_labels: data.state_labels,
                },
            },
        },
    })
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::AgentStatus;

    #[test]
    fn agent_status_match_any_deduplicates_filters() {
        let subscriptions = event_match_subscriptions(
            "wait",
            EventMatch::PaneAgentStatusChangedAny {
                pane_id: "pane_1".into(),
                agent_statuses: vec![AgentStatus::Idle, AgentStatus::Done, AgentStatus::Idle],
            },
        )
        .expect("valid match-any filter");

        assert_eq!(
            subscriptions,
            vec![
                Subscription::PaneAgentStatusChanged {
                    pane_id: "pane_1".into(),
                    agent_status: Some(AgentStatus::Idle),
                },
                Subscription::PaneAgentStatusChanged {
                    pane_id: "pane_1".into(),
                    agent_status: Some(AgentStatus::Done),
                },
            ]
        );
    }

    #[test]
    fn agent_status_match_any_rejects_empty_and_oversized_filters() {
        let empty = event_match_subscriptions(
            "empty",
            EventMatch::PaneAgentStatusChangedAny {
                pane_id: "pane_1".into(),
                agent_statuses: Vec::new(),
            },
        )
        .expect_err("empty match-any filter should fail");
        assert_eq!(empty.error.code, "invalid_event_wait_match");

        let oversized = event_match_subscriptions(
            "oversized",
            EventMatch::PaneAgentStatusChangedAny {
                pane_id: "pane_1".into(),
                agent_statuses: vec![AgentStatus::Idle; MAX_EVENT_WAIT_AGENT_STATUSES + 1],
            },
        )
        .expect_err("oversized match-any filter should fail");
        assert_eq!(oversized.error.code, "invalid_event_wait_match");
    }
}
