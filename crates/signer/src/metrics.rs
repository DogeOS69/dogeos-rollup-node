use metrics::Histogram;
use metrics_derive::Metrics;

/// The metrics for the [`super::Signer`].
#[derive(Metrics, Clone)]
#[metrics(scope = "signer")]
pub struct SignerMetrics {
    /// Time from a block's signing request to its signature, including timed-out attempts and
    /// retry delays.
    pub signing_duration: Histogram,
}
