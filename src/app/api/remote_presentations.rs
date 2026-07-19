use crate::api::schema::{
    ResponseResult, TerminalDelegateClaimParams, TerminalDelegateCreateParams,
    TerminalDelegateHandoffParams, TerminalDelegateParkParams, TerminalDelegationClaim,
    TerminalDelegationTarget, TerminalParkedAdminListParams, TerminalParkedAdminResolveParams,
    TerminalParkedListParams, TerminalParkedResolveAction, TerminalParkedResolveParams,
    TerminalParkedResumeParams, TerminalParkedTarget,
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

    pub(super) fn handle_terminal_delegate_park(
        &mut self,
        id: String,
        params: TerminalDelegateParkParams,
    ) -> String {
        match self.park_terminal_delegation(params) {
            Ok(parked) => encode_success(id, ResponseResult::TerminalParked { parked }),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_parked_status(
        &mut self,
        id: String,
        params: TerminalParkedTarget,
    ) -> String {
        match self.parked_terminal_status(&params) {
            Ok(parked) => encode_success(id, ResponseResult::TerminalParked { parked }),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_parked_resume(
        &mut self,
        id: String,
        params: TerminalParkedResumeParams,
    ) -> String {
        let agent = self.parked_agent_info_for_id(&params.target.park_id);
        match self.prepare_parked_terminal_resume(params) {
            Ok(delegation) => encode_success(
                id,
                ResponseResult::TerminalParkedResume {
                    prepared: crate::api::schema::TerminalParkedResumePrepared {
                        delegation,
                        agent,
                    },
                },
            ),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_parked_list(
        &mut self,
        id: String,
        params: TerminalParkedListParams,
    ) -> String {
        match self.list_parked_terminals(&params) {
            Ok(parked) => encode_success(id, ResponseResult::TerminalParkedList { parked }),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_parked_resolve(
        &mut self,
        id: String,
        params: TerminalParkedResolveParams,
    ) -> String {
        let promoted_agent = params
            .owner
            .as_ref()
            .and_then(|_| self.parked_agent_info_for_id(&params.park_id));
        let result = match params.action {
            TerminalParkedResolveAction::Retain => self
                .retain_parked_terminal(&params)
                .map(|parked| ResponseResult::TerminalParked { parked }),
            TerminalParkedResolveAction::Terminate => {
                self.terminate_parked_terminal(&params)
                    .map(|_| ResponseResult::Ok {
                        terminated_remote_presentations: None,
                        handed_off_remote_presentations: None,
                    })
            }
            TerminalParkedResolveAction::Promote => {
                let resumes_to_owner = params.owner.is_some();
                self.promote_parked_terminal(&params).map(|delegation| {
                    if resumes_to_owner {
                        ResponseResult::TerminalParkedResume {
                            prepared: crate::api::schema::TerminalParkedResumePrepared {
                                delegation,
                                agent: promoted_agent,
                            },
                        }
                    } else {
                        ResponseResult::TerminalDelegation { delegation }
                    }
                })
            }
        };
        match result {
            Ok(result) => encode_success(id, result),
            Err(error) => encode_error_body(id, error),
        }
    }

    pub(super) fn handle_terminal_parked_admin_list(
        &mut self,
        id: String,
        params: TerminalParkedAdminListParams,
    ) -> String {
        if let Err(error) = authorize_parked_admin(&params.admin_token) {
            return encode_error_body(id, error);
        }
        encode_success(
            id,
            ResponseResult::TerminalParkedList {
                parked: self.parked_terminal_inventory(),
            },
        )
    }

    pub(super) fn handle_terminal_parked_admin_resolve(
        &mut self,
        id: String,
        params: TerminalParkedAdminResolveParams,
    ) -> String {
        if let Err(error) = authorize_parked_admin(&params.admin_token) {
            return encode_error_body(id, error);
        }
        let result = match params.action {
            TerminalParkedResolveAction::Retain => self
                .retain_parked_terminal_admin(&params.park_id)
                .map(|parked| ResponseResult::TerminalParked { parked }),
            TerminalParkedResolveAction::Terminate => self
                .terminate_parked_terminal_admin(&params.park_id)
                .map(|_| ResponseResult::Ok {
                    terminated_remote_presentations: None,
                    handed_off_remote_presentations: None,
                }),
            TerminalParkedResolveAction::Promote => self
                .promote_parked_terminal_admin(&params.park_id)
                .map(|delegation| ResponseResult::TerminalDelegation { delegation }),
        };
        match result {
            Ok(result) => encode_success(id, result),
            Err(error) => encode_error_body(id, error),
        }
    }
}

#[cfg(unix)]
fn authorize_parked_admin(token: &str) -> Result<(), crate::api::schema::ErrorBody> {
    let store = crate::remote_resume::LocalAdminTokenStore::open_global().map_err(|error| {
        crate::api::schema::ErrorBody {
            code: "terminal_parked_admin_unavailable".into(),
            message: format!("could not load the local parked-terminal admin token: {error}"),
        }
    })?;
    if store.verify(token) {
        Ok(())
    } else {
        Err(crate::api::schema::ErrorBody {
            code: "terminal_parked_admin_unauthorized".into(),
            message: "local parked-terminal admin token does not match".into(),
        })
    }
}

#[cfg(not(unix))]
fn authorize_parked_admin(_token: &str) -> Result<(), crate::api::schema::ErrorBody> {
    Err(crate::api::schema::ErrorBody {
        code: "unsupported_platform".into(),
        message: "parked terminal administration is not supported on this platform".into(),
    })
}
