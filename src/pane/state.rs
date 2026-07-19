use crate::terminal::TerminalId;

/// Viewport state for a pane.
///
/// Terminal identity, cwd, labels, and agent metadata live in TerminalState.
pub struct PaneState {
    pub attached_terminal_id: TerminalId,
    /// This pane reserves its persisted layout slot for a parked remote
    /// terminal. Reservations deliberately have no PTY runtime: the pane is
    /// rendered by the restore panel until the remote terminal is attached or
    /// the user closes it.
    pub remote_restore_reservation: bool,
    /// Whether the user has seen this pane since its last state change to Idle.
    /// False = "Done" (agent finished while user was in another workspace).
    pub seen: bool,
}

impl PaneState {
    pub fn new(attached_terminal_id: TerminalId) -> Self {
        Self {
            attached_terminal_id,
            remote_restore_reservation: false,
            seen: true,
        }
    }

    pub fn with_remote_restore_reservation(mut self) -> Self {
        self.remote_restore_reservation = true;
        self
    }
}
