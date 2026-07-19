use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalRecoveryListParams {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalRecoveryTarget {
    pub remote_terminal_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalRecoveryStatus {
    Pending,
    Queued,
    Restoring,
    TimedOut,
    Retryable,
    Ended,
    Discarding,
    CleanupPending,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TerminalRecoveryInfo {
    pub remote_terminal_id: String,
    pub peer_id: String,
    pub status: TerminalRecoveryStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{
        ErrorBody, ErrorResponse, Method, Request, ResponseResult, SuccessResponse,
    };

    #[test]
    fn recovery_methods_use_neutral_wire_names() {
        let target = || TerminalRecoveryTarget {
            remote_terminal_id: "terminal-a".into(),
        };
        let methods = [
            (
                Method::TerminalRecoveryList(TerminalRecoveryListParams::default()),
                "terminal.recovery.list",
            ),
            (
                Method::TerminalRecoveryStatus(target()),
                "terminal.recovery.status",
            ),
            (
                Method::TerminalRecoveryRetry(target()),
                "terminal.recovery.retry",
            ),
            (
                Method::TerminalRecoveryDiscard(target()),
                "terminal.recovery.discard",
            ),
        ];

        for (method, expected_name) in methods {
            let value = serde_json::to_value(Request {
                id: "recovery-test".into(),
                method,
            })
            .expect("serialize recovery request");
            assert_eq!(value["method"], expected_name);
            assert!(value["params"].is_object());
        }
    }

    #[test]
    fn recovery_info_round_trips_status_and_optional_message() {
        let info = TerminalRecoveryInfo {
            remote_terminal_id: "terminal-a".into(),
            peer_id: "peer-a".into(),
            status: TerminalRecoveryStatus::TimedOut,
            message: Some("connection timed out".into()),
        };

        let encoded = serde_json::to_value(&info).expect("serialize recovery info");
        assert_eq!(encoded["status"], "timed_out");
        assert_eq!(
            serde_json::from_value::<TerminalRecoveryInfo>(encoded)
                .expect("deserialize recovery info"),
            info
        );
    }

    #[test]
    fn recovery_list_and_status_success_responses_are_typed() {
        let recovery = TerminalRecoveryInfo {
            remote_terminal_id: "terminal-a".into(),
            peer_id: "peer-a".into(),
            status: TerminalRecoveryStatus::Pending,
            message: None,
        };
        let list = serde_json::to_value(SuccessResponse {
            id: "list-request".into(),
            result: ResponseResult::TerminalRecoveryList {
                recoveries: vec![recovery.clone()],
            },
        })
        .expect("serialize recovery list response");
        let status = serde_json::to_value(SuccessResponse {
            id: "status-request".into(),
            result: ResponseResult::TerminalRecovery { recovery },
        })
        .expect("serialize recovery status response");

        assert_eq!(list["id"], "list-request");
        assert_eq!(list["result"]["recoveries"][0]["status"], "pending");
        assert_eq!(status["id"], "status-request");
        assert_eq!(
            status["result"]["recovery"]["remote_terminal_id"],
            "terminal-a"
        );
    }

    #[test]
    fn recovery_not_found_error_preserves_type_and_request_id() {
        let response = serde_json::to_value(ErrorResponse {
            id: "status-request".into(),
            error: ErrorBody {
                code: "terminal_recovery_not_found".into(),
                message: "terminal recovery terminal-missing was not found".into(),
            },
        })
        .expect("serialize recovery not-found response");

        assert_eq!(response["id"], "status-request");
        assert_eq!(response["error"]["code"], "terminal_recovery_not_found");
    }

    #[test]
    fn active_and_retryable_recovery_statuses_remain_distinct_on_wire() {
        let encode_status = |status| {
            serde_json::to_value(TerminalRecoveryInfo {
                remote_terminal_id: "terminal-a".into(),
                peer_id: "peer-a".into(),
                status,
                message: None,
            })
            .expect("serialize recovery status")
        };

        assert_eq!(
            encode_status(TerminalRecoveryStatus::Restoring)["status"],
            "restoring"
        );
        assert_eq!(
            encode_status(TerminalRecoveryStatus::Retryable)["status"],
            "retryable"
        );
    }
}
