use crate::BlockInfo;
use alloy_primitives::Address;

/// An update to the consensus configuration.
///
/// Runtime authorized-signer refresh is a two-phase, head-qualified protocol. For every distinct
/// L1 head observed in dynamic mode, the producer first opens an authorization barrier with
/// [`ConsensusUpdate::AuthorizationPending`] and later closes it with a matching
/// [`ConsensusUpdate::AuthorizedSigner`]. The head identity `(number, hash)` binds the two phases
/// so that a stale or reorged update can never clear the barrier for a different head.
///
/// The two phases travel on different transports by design. Phase one is delivered on a dedicated,
/// unconditionally-polled authorization-control channel that is never subject to ordinary L1 data
/// backpressure, so the barrier is always opened promptly. Phase two is delivered on the ordinary
/// FIFO L1 notification channel, ordered *after* the reorg/new-block notifications for the same
/// head, so the signer is applied in step with the chain data it was read against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsensusUpdate {
    /// Phase one: a dynamic L1 head transition opened an authorization window for `head`.
    ///
    /// While pending, the consumer must withhold sequencing, local block
    /// finalization/announcement, and inbound block acceptance (fail-closed) until a matching
    /// [`ConsensusUpdate::AuthorizedSigner`] for the same `head` arrives.
    AuthorizationPending(BlockInfo),
    /// Phase two: the authorized signer read (pinned to `head`'s hash) for the pending head.
    ///
    /// Applied only when `head` matches the currently pending head; a stale or reorged head is
    /// ignored and leaves the barrier untouched.
    AuthorizedSigner {
        /// The L1 head the signer was read at.
        head: BlockInfo,
        /// The authorized signer at `head`.
        signer: Address,
    },
}
