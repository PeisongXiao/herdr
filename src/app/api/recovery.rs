#[cfg(unix)]
use crate::api::schema::ResponseResult;
use crate::api::schema::{ErrorBody, TerminalRecoveryListParams, TerminalRecoveryTarget};
use crate::app::App;

use super::responses::encode_error_body;
#[cfg(unix)]
use super::responses::encode_success;

impl App {
    pub(super) fn handle_terminal_recovery_list(
        &mut self,
        id: String,
        _params: TerminalRecoveryListParams,
    ) -> String {
        #[cfg(unix)]
        {
            encode_success(
                id,
                ResponseResult::TerminalRecoveryList {
                    recoveries: self.terminal_recovery_list(),
                },
            )
        }
        #[cfg(not(unix))]
        encode_error_body(id, recovery_unsupported())
    }

    pub(super) fn handle_terminal_recovery_status(
        &mut self,
        id: String,
        target: TerminalRecoveryTarget,
    ) -> String {
        #[cfg(unix)]
        {
            match self.terminal_recovery_status(&target.remote_terminal_id) {
                Some(recovery) => encode_success(id, ResponseResult::TerminalRecovery { recovery }),
                None => encode_error_body(id, recovery_not_found(&target.remote_terminal_id)),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            encode_error_body(id, recovery_unsupported())
        }
    }

    pub(super) fn handle_terminal_recovery_retry(
        &mut self,
        id: String,
        target: TerminalRecoveryTarget,
    ) -> String {
        #[cfg(unix)]
        {
            match self.retry_terminal_recovery(&target.remote_terminal_id) {
                Ok(recovery) => encode_success(id, ResponseResult::TerminalRecovery { recovery }),
                Err(error) => encode_error_body(id, error),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            encode_error_body(id, recovery_unsupported())
        }
    }

    pub(super) fn handle_terminal_recovery_discard(
        &mut self,
        id: String,
        target: TerminalRecoveryTarget,
    ) -> String {
        #[cfg(unix)]
        {
            match self.discard_terminal_recovery(&target.remote_terminal_id) {
                Ok(()) => encode_success(
                    id,
                    ResponseResult::Ok {
                        terminated_remote_presentations: None,
                        handed_off_remote_presentations: None,
                    },
                ),
                Err(error) => encode_error_body(id, error),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = target;
            encode_error_body(id, recovery_unsupported())
        }
    }
}

#[cfg(unix)]
fn recovery_not_found(remote_terminal_id: &str) -> ErrorBody {
    ErrorBody {
        code: "terminal_recovery_not_found".into(),
        message: format!("terminal recovery {remote_terminal_id} was not found"),
    }
}

#[cfg(not(unix))]
fn recovery_unsupported() -> ErrorBody {
    ErrorBody {
        code: "unsupported_platform".into(),
        message: "terminal recovery is only supported on Unix platforms".into(),
    }
}
