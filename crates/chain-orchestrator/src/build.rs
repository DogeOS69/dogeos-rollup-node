use dogeos_reth_primitives::DogeosBlock;
use tokio::sync::oneshot;

/// The terminal outcome of an admitted manual block build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildBlockOutcome {
    /// The payload was finalized and queued for signing.
    Sequenced(DogeosBlock),
    /// The payload was empty, invalidated by a state transition, or explicitly cancelled.
    Skipped,
    /// Payload finalization, persistence, or signer enqueue failed after admission.
    Failed(String),
}

/// A completion receiver uniquely associated with one admitted manual block build.
///
/// Dropping the ticket does not cancel the build. Call [`Self::wait`] to receive its terminal
/// outcome without relying on an uncorrelated global event stream.
#[derive(Debug)]
pub struct BuildBlockTicket {
    completion: oneshot::Receiver<BuildBlockOutcome>,
}

#[derive(Debug)]
pub(crate) struct BuildBlockCompletion(oneshot::Sender<BuildBlockOutcome>);

pub(crate) fn build_block_channel() -> (BuildBlockCompletion, BuildBlockTicket) {
    let (sender, receiver) = oneshot::channel();
    (BuildBlockCompletion(sender), BuildBlockTicket::new(receiver))
}

impl BuildBlockCompletion {
    pub(crate) fn complete(self, outcome: BuildBlockOutcome) {
        let _ = self.0.send(outcome);
    }
}

impl BuildBlockTicket {
    pub(crate) const fn new(completion: oneshot::Receiver<BuildBlockOutcome>) -> Self {
        Self { completion }
    }

    /// Waits for the admitted build's terminal outcome.
    pub async fn wait(self) -> Result<BuildBlockOutcome, oneshot::error::RecvError> {
        self.completion.await
    }
}

#[cfg(test)]
mod tests {
    use super::{build_block_channel, BuildBlockOutcome};

    #[tokio::test]
    async fn ticket_receives_only_its_correlated_terminal_outcome() {
        let (first_completion, first_ticket) = build_block_channel();
        let (second_completion, second_ticket) = build_block_channel();

        second_completion.complete(BuildBlockOutcome::Failed("payload failed".to_string()));
        first_completion.complete(BuildBlockOutcome::Skipped);

        assert_eq!(first_ticket.wait().await.unwrap(), BuildBlockOutcome::Skipped);
        assert_eq!(
            second_ticket.wait().await.unwrap(),
            BuildBlockOutcome::Failed("payload failed".to_string())
        );
    }
}
