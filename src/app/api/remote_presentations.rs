use crate::api::schema::{
    ResponseResult, TerminalDelegateClaimParams, TerminalDelegateCreateParams,
    TerminalDelegateHandoffParams, TerminalDelegationClaim, TerminalDelegationTarget,
};
use crate::app::App;

use super::responses::{encode_error, encode_error_body, encode_success};

impl App {
    pub(super) fn handle_terminal_delegate_create(
        &mut self,
        id: String,
        params: TerminalDelegateCreateParams,
    ) -> String {
        match self.prepare_delegated_terminal(params) {
            Ok(delegation) => encode_success(id, ResponseResult::TerminalDelegation { delegation }),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_delegate_claim(
        &mut self,
        id: String,
        params: TerminalDelegateClaimParams,
    ) -> String {
        match self.prepare_existing_terminal_delegation(params) {
            Ok(delegation) => encode_success(id, ResponseResult::TerminalDelegation { delegation }),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_delegate_status(
        &mut self,
        id: String,
        target: TerminalDelegationTarget,
    ) -> String {
        match self.terminal_delegation_info(&target.delegation_id, target.epoch) {
            Some(delegation) => {
                encode_success(id, ResponseResult::TerminalDelegation { delegation })
            }
            None => encode_error(
                id,
                "terminal_delegation_not_found",
                "terminal delegation was not found or has expired",
            ),
        }
    }

    pub(super) fn handle_terminal_delegate_terminate(
        &mut self,
        id: String,
        target: TerminalDelegationTarget,
    ) -> String {
        let claim = TerminalDelegationClaim {
            delegation_id: target.delegation_id,
            epoch: target.epoch,
        };
        if self.terminate_terminal_delegation(&claim) {
            encode_success(
                id,
                ResponseResult::Ok {
                    terminated_remote_presentations: None,
                    handed_off_remote_presentations: None,
                },
            )
        } else {
            encode_error(
                id,
                "terminal_delegation_not_found",
                "terminal delegation was not found or no longer belongs to this owner",
            )
        }
    }

    pub(super) fn handle_terminal_delegate_handoff(
        &mut self,
        id: String,
        params: TerminalDelegateHandoffParams,
    ) -> String {
        match self.handoff_delegated_pane(&params.pane_id) {
            Ok(delegation) => encode_success(id, ResponseResult::TerminalDelegation { delegation }),
            Err(error) => encode_error_body(id, error),
        }
    }
}
