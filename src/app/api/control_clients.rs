use std::time::Instant;

use crate::api::schema::{ControlClientRegisterParams, ControlClientTarget, ResponseResult};
use crate::app::App;

use super::responses::{encode_error, encode_success};

impl App {
    pub(super) fn handle_control_client_register(
        &mut self,
        id: String,
        params: ControlClientRegisterParams,
    ) -> String {
        let _kind = params.kind;
        match self.register_control_client(params.client_id, params.access_mode, Instant::now()) {
            Ok(status) => encode_success(id, ResponseResult::ControlClientStatus { status }),
            Err(err) => encode_error(id, err.code(), err.message()),
        }
    }

    pub(super) fn handle_control_client_heartbeat(
        &mut self,
        id: String,
        target: ControlClientTarget,
    ) -> String {
        match self.heartbeat_control_client(&target.client_id, Instant::now()) {
            Ok(status) => encode_success(id, ResponseResult::ControlClientStatus { status }),
            Err(err) => encode_error(id, err.code(), err.message()),
        }
    }

    pub(super) fn handle_control_client_unregister(
        &mut self,
        id: String,
        target: ControlClientTarget,
    ) -> String {
        match self.unregister_control_client(&target.client_id) {
            Ok(status) => encode_success(id, ResponseResult::ControlClientStatus { status }),
            Err(err) => encode_error(id, err.code(), err.message()),
        }
    }

    pub(super) fn handle_control_client_status(&self, id: String) -> String {
        encode_success(
            id,
            ResponseResult::ControlClientStatus {
                status: self.control_client_status(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{
        ControlClientAccessMode, ControlClientKind, EventData, EventKind, SuccessResponse,
    };
    use crate::config::Config;

    fn test_app() -> App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        App::new(
            &Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        )
    }

    #[test]
    fn registration_updates_status_and_emits_only_aggregate_changes() {
        let mut app = test_app();
        let response = app.handle_control_client_register(
            "register".into(),
            ControlClientRegisterParams {
                client_id: "mcp".into(),
                kind: ControlClientKind::Mcp,
                access_mode: ControlClientAccessMode::Restricted,
            },
        );
        let response: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::ControlClientStatus { status } = response.result else {
            panic!("expected control client status");
        };
        assert_eq!(status.total_count, 1);
        assert_eq!(status.restricted_count, 1);
        let events = app.event_hub.events_after(0);
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].1,
            crate::api::schema::EventEnvelope {
                event: EventKind::ControlClientPresenceChanged,
                data: EventData::ControlClientPresenceChanged { status }
            } if status.total_count == 1
        ));

        app.handle_control_client_register(
            "register_again".into(),
            ControlClientRegisterParams {
                client_id: "mcp".into(),
                kind: ControlClientKind::Mcp,
                access_mode: ControlClientAccessMode::Restricted,
            },
        );
        assert_eq!(app.event_hub.events_after(0).len(), 1);
    }

    #[test]
    fn unregister_is_idempotent_and_returns_current_status() {
        let mut app = test_app();
        let response = app.handle_control_client_unregister(
            "unregister".into(),
            ControlClientTarget {
                client_id: "missing".into(),
            },
        );
        let response: SuccessResponse = serde_json::from_str(&response).unwrap();
        assert!(matches!(
            response.result,
            ResponseResult::ControlClientStatus {
                status: crate::api::schema::ControlClientStatus { total_count: 0, .. }
            }
        ));
    }
}
