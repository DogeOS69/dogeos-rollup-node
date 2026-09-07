//! Remote block source add-on for importing blocks from a remote L2 node
//! and building new blocks on top.

use crate::args::RemoteBlockSourceArgs;
use alloy_primitives::Signature;
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_client::RpcClient;
use alloy_transport::layers::RetryBackoffLayer;
use dogeos_rpc_types::Scroll;
use futures::StreamExt;
use reth_network_api::{FullNetwork, PeerId};
use reth_provider::BlockReader;
use reth_tokio_util::EventStream;
use rollup_node_chain_orchestrator::{ChainOrchestratorEvent, ChainOrchestratorHandle};
use scroll_network::{DogeosNetworkPrimitives, NewBlockWithPeer};
use tokio::time::{interval, Duration};

/// Remote block source add-on that imports blocks from a trusted remote L2 node
/// and triggers block building on top of each imported block.
#[derive(Debug)]
pub struct RemoteBlockSourceAddOn<N, P>
where
    N: FullNetwork<Primitives = DogeosNetworkPrimitives>,
{
    /// Configuration for the remote block source.
    config: RemoteBlockSourceArgs,
    /// Handle to the chain orchestrator for sending commands.
    orchestrator_handle: ChainOrchestratorHandle<N>,
    /// An event stream for listening to chain orchestrator events, used to wait for block build
    /// completion.
    events: EventStream<ChainOrchestratorEvent>,
    /// A provider for the remote node, used to fetch blocks and block information.
    remote: RootProvider<Scroll>,
    /// Local block reader, used to find the highest common block with the remote.
    provider: P,
    /// Tracks the last block number we imported from remote.
    /// This is different from local head because we build blocks on top of imports.
    ///
    /// `None` until the remote has been reached once and the highest common
    /// block determined — construction must not depend on the remote being up
    /// (issue #38): a connection error at startup used to abort the whole node.
    last_imported_block: Option<u64>,
}

/// The remote endpoint reduced to `scheme://host:port`, safe to log.
///
/// The configured URL can carry basic-auth credentials and query-string API
/// keys, and those survive into transport error messages: alloy wraps
/// `reqwest::Error`, whose `Display` appends `for url ({url})` with the
/// userinfo and query intact. Anything derived from an error chain has to be
/// scrubbed against the full URL before it reaches a log line.
fn redact_remote(url: Option<&reqwest::Url>, message: &str) -> String {
    let Some(url) = url else { return message.to_string() };
    let host = safe_remote_host(url);
    // Normalize both sides before matching: gateways can echo equivalent escapes with different
    // hex case or decode only part of a credential. Raw and fully decoded needles alone miss
    // those spellings. This is display-only; the actual request URL is never changed.
    let message = percent_decode(message);
    let mut redacted = replace_url(&message, &percent_decode(url.as_str()), &host);
    // reqwest moves userinfo into an Authorization header before it builds the
    // request, so its error text carries the STRIPPED url — which does not match
    // the configured string. Without this second pass, configuring credentials
    // is precisely what turns the redaction off, and the path and query (where
    // API keys usually live) reach the log.
    let mut stripped = url.clone();
    if stripped.set_username("").is_ok() && stripped.set_password(None).is_ok() {
        let stripped = stripped.as_str();
        if stripped != url.as_str() {
            redacted = replace_url(&redacted, &percent_decode(stripped), &host);
        }
    }
    // Third pass, for text the remote controls: a non-2xx response is rendered
    // by the transport as `HTTP error {status} with body: {body}` with the body
    // verbatim, and a gateway or rate limiter may echo the request target or
    // the credentials on their own, in a form neither URL pass matches. Each
    // component is scrubbed individually, in both the configured form and the
    // percent-decoded one (reqwest decodes userinfo before it sends it, so
    // `ops%40dogeos.com` comes back as `ops@dogeos.com`), and each query value
    // on its own, since a body can echo `apikey=SECRET` without the rest.
    //
    // Every replacement is bounded (`replace_token`): a username or path that
    // is also a substring of a source path (`node` and `/src` against eyre's
    // `Location:` line, `/rpc` against the `rpc.internal` host) leaves the
    // diagnostics alone. The username only counts in the `user:` and `user@`
    // shapes, the only ones in which it identifies itself.
    let mut components: Vec<(String, &str)> = Vec::new();
    let mut push = |raw: &str, marker: &'static str| {
        if raw.is_empty() {
            return;
        }
        let decoded = percent_decode(raw);
        if decoded != raw {
            components.push((decoded, marker));
        }
        components.push((raw.to_string(), marker));
    };
    push(url.password().unwrap_or_default(), "<password>");
    let username = url.username();
    if !username.is_empty() {
        push(&format!("{username}:"), "<username>:");
        push(&format!("{username}@"), "<username>@");
    }
    let query = url.query().unwrap_or_default();
    push(query, "<query>");
    for (_, value) in query.split('&').filter_map(|pair| pair.split_once('=')) {
        push(value, "<query>");
    }
    // Form-encoded query values may also be echoed with '+' decoded to a space.
    for (_, value) in url.query_pairs() {
        push(&value, "<query>");
    }
    if url.path() != "/" {
        push(url.path(), "<path>");
        // API keys commonly occupy one path segment (e.g. /v2/KEY), and an error body can
        // quote that segment without its prefix. The same token bounds preserve source paths.
        for segment in url.path().split('/').filter(|segment| !segment.is_empty()) {
            push(segment, "<path>");
        }
    }
    // Longest first, so the whole query wins over one of its values.
    components.sort_by_key(|(needle, _)| std::cmp::Reverse(needle.len()));
    components.iter().fold(redacted, |text, (needle, marker)| replace_token(&text, needle, marker))
}

/// Replaces `needle` with `marker` wherever it stands as a whole URL token:
/// where it cannot be extended into a longer run of URL characters on either
/// side. `/src` is left alone inside `crates/node/src/`, `/rpc` inside
/// `//rpc.internal`, `rs:` inside `source.rs:174`; `ops:` still matches in
/// `user ops:ops123`, and `/v2/KEY` in `denied for /v2/KEY?token=T`. A needle
/// that itself starts or ends with a delimiter (`ops:`) is its own boundary on
/// that side.
fn replace_token(text: &str, needle: &str, marker: &str) -> String {
    const fn is_url_char(c: char) -> bool {
        c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~' | '%' | '/')
    }
    if needle.is_empty() {
        return text.to_string();
    }
    let open = needle.starts_with(is_url_char);
    let close = needle.ends_with(is_url_char);
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(needle) {
        let start = cursor + offset;
        let end = start + needle.len();
        // Dots inside hosts or paths extend a token; trailing sentence punctuation does not.
        let before = text[..start].trim_end_matches('.');
        let after = text[end..].trim_start_matches('.');
        let bounded =
            !(open && before.ends_with(is_url_char)) && !(close && after.starts_with(is_url_char));
        if bounded {
            out.push_str(&text[cursor..start]);
            out.push_str(marker);
            cursor = end;
        } else {
            // Step one character past the unbounded match and keep looking.
            let step = text[start..].chars().next().map_or(1, char::len_utf8);
            out.push_str(&text[cursor..start + step]);
            cursor = start + step;
        }
    }
    out.push_str(&text[cursor..]);
    out
}

/// Percent-decodes a URL component (`ops%40dogeos.com` to `ops@dogeos.com`).
/// A `%` not followed by two hex digits is kept as is.
fn percent_decode(raw: &str) -> String {
    let mut out = Vec::with_capacity(raw.len());
    let mut rest = raw.as_bytes();
    while let Some((&first, tail)) = rest.split_first() {
        let decoded = match tail {
            [hi, lo, ..] if first == b'%' && hi.is_ascii_hexdigit() && lo.is_ascii_hexdigit() => {
                u8::from_str_radix(std::str::from_utf8(&tail[..2]).unwrap_or_default(), 16).ok()
            }
            _ => None,
        };
        match decoded {
            Some(byte) => {
                out.push(byte);
                rest = &tail[2..];
            }
            None => {
                out.push(first);
                rest = tail;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Replaces every occurrence of `url` in `message` with `host`, keeping the
/// path separator when the URL string ends in one. A path-less URL renders as
/// `scheme://host:port/`, so a plain replacement of that prefix inside a longer
/// endpoint (`.../eth/v1/...`) would glue the sub-path onto the port.
fn replace_url(message: &str, url: &str, host: &str) -> String {
    if url.ends_with('/') {
        message.replace(url, &format!("{host}/"))
    } else {
        message.replace(url, host)
    }
}

/// `scheme://host:port` for the configured remote, with no userinfo or query.
fn safe_remote_host(url: &reqwest::Url) -> String {
    format!(
        "{}://{}:{}",
        url.scheme(),
        url.host_str().unwrap_or("<none>"),
        url.port_or_known_default().unwrap_or(0)
    )
}

impl<N, P> RemoteBlockSourceAddOn<N, P>
where
    N: FullNetwork<Primitives = DogeosNetworkPrimitives> + Send + Sync + 'static,
    P: BlockReader,
{
    /// Creates a new remote block source add-on.
    ///
    /// Performs no remote I/O: the resume point is determined lazily on the
    /// first successful poll, where errors are logged and retried at poll
    /// cadence instead of failing node launch.
    pub async fn new(
        config: RemoteBlockSourceArgs,
        handle: ChainOrchestratorHandle<N>,
        provider: P,
    ) -> eyre::Result<Self> {
        // Build remote provider with retry layer.
        let Some(url) = config.url.clone() else {
            tracing::error!(target: "scroll::remote_source", "URL required when remote-source is enabled");
            return Err(eyre::eyre!("URL required when remote-source is enabled"));
        };
        let retry_layer = RetryBackoffLayer::new(10, 100, 330);
        let client = RpcClient::builder().layer(retry_layer).http(url);
        let remote = ProviderBuilder::<_, _, Scroll>::default().connect_client(client);

        // Get event listener for waiting on block completion
        let events = match handle.get_event_listener().await {
            Ok(stream) => stream,
            Err(e) => {
                tracing::error!(target: "scroll::remote_source", ?e, "Failed to get event listener");
                return Err(eyre::eyre!(e));
            }
        };

        Ok(Self {
            config,
            orchestrator_handle: handle,
            events,
            remote,
            provider,
            last_imported_block: None,
        })
    }

    /// Determines the last imported block by finding the highest common block
    /// between the local chain and the remote node.
    ///
    /// Called on every poll tick until it succeeds; a failure here (e.g. the
    /// remote is not up yet) is retried on the next poll tick.
    async fn init_last_imported_block(&self) -> eyre::Result<u64> {
        let local_head = self.orchestrator_handle.status().await?.l2.fcs.head_block_info().number;
        let remote_head = self.remote.get_block_number().await?;

        let last_imported_block;
        let mut search = local_head.min(remote_head);
        loop {
            if search == 0 {
                // Genesis is always a common block (same chain spec assumed).
                last_imported_block = 0;
                break;
            }
            let local_hash = self.provider.block_hash(search)?;
            let remote_block = self.remote.get_block_by_number(search.into()).await?;
            match (local_hash, remote_block) {
                (Some(lh), Some(rb)) if lh == rb.header.hash => {
                    last_imported_block = search;
                    break;
                }
                _ => {
                    search = search.saturating_sub(1);
                }
            }
        }
        tracing::info!(
            target: "scroll::remote_source",
            last_imported_block,
            local_head,
            remote_head,
            "Determined highest common block with remote"
        );
        Ok(last_imported_block)
    }

    /// Runs the remote block source until shutdown.
    pub async fn run_until_shutdown(
        mut self,
        mut shutdown: impl std::future::Future<Output = ()> + Unpin,
    ) -> eyre::Result<()> {
        let mut poll_interval = interval(Duration::from_millis(self.config.poll_interval_ms));

        loop {
            tokio::select! {
                biased;
                _guard = &mut shutdown => break,
                _ = poll_interval.tick() => {
                    if let Err(e) = self.follow_and_build().await {
                        // `{e:?}` keeps eyre's `Caused by:` chain and `Location:`.
                        // Every transport error renders identically once the URL
                        // is scrubbed, so the location is what tells a
                        // first-contact failure apart from one mid catch-up.
                        // The full URL carries basic-auth credentials and query
                        // strings (API keys) and the transport's Display appends
                        // it, so the rendered report is scrubbed before it is
                        // logged; `?e` is never handed to tracing directly.
                        let msg = redact_remote(self.config.url.as_ref(), &format!("{e:?}"));
                        let remote_host = self.config.url.as_ref().map(safe_remote_host);
                        tracing::error!(
                            target: "scroll::remote_source",
                            error = %msg,
                            remote_host = ?remote_host,
                            "Sync error"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Follows the remote node and builds blocks on top of imported blocks.
    async fn follow_and_build(&mut self) -> eyre::Result<()> {
        // First successful contact with the remote determines the resume point.
        if self.last_imported_block.is_none() {
            let resume = self.init_last_imported_block().await?;
            self.last_imported_block = Some(resume);
        }

        loop {
            let last_imported = self.last_imported_block.expect("initialized above");

            // Get remote head
            let remote_block = self
                .remote
                .get_block_by_number(alloy_eips::BlockNumberOrTag::Latest)
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Remote block not found"))?;

            let remote_head = remote_block.header.number;

            // Compare against last imported block
            if remote_head <= last_imported {
                tracing::trace!(target: "scroll::remote_source",
                    last_imported,
                    remote_head,
                    "Already synced with remote");
                return Ok(());
            }

            let blocks_behind = remote_head - last_imported;
            tracing::info!(target: "scroll::remote_source",
                last_imported,
                remote_head,
                blocks_behind,
                "Catching up");

            // Fetch and import the next block from remote
            let next_block_num = last_imported + 1;
            let block = self
                .remote
                .get_block_by_number(next_block_num.into())
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Block {} not found", next_block_num))?
                .into_consensus()
                .map_transactions(|tx| tx.inner.into_inner());

            // Create NewBlockWithPeer with dummy peer_id and signature (trusted source)
            let block_with_peer = NewBlockWithPeer {
                peer_id: PeerId::default(),
                block,
                signature: Signature::new(Default::default(), Default::default(), false),
            };

            // Import the block (this will cause a reorg if we had a locally built block at this
            // height)
            let chain_import = match self.orchestrator_handle.import_block(block_with_peer).await {
                Ok(Ok(chain_import)) => {
                    self.last_imported_block = Some(next_block_num);
                    chain_import
                }
                Ok(Err(e)) => {
                    return Err(eyre::eyre!("Import block failed: {}", e));
                }
                Err(e) => {
                    return Err(eyre::eyre!("chain orchestrator command channel error: {}", e));
                }
            };

            if !chain_import.result.is_valid() {
                tracing::info!(target: "scroll::remote_source",
                    result = ?chain_import.result,
                    "Imported block is not valid according to forkchoice, skipping build");
                continue;
            }

            if !self.config.build {
                tracing::debug!(target: "scroll::remote_source", "Imported block is valid, but build is disabled, skipping build");
                continue;
            }

            if !self.orchestrator_handle.status().await?.is_synced() {
                tracing::debug!(target: "scroll::remote_source", "Imported block is valid, but orchestrator is not synced, skipping build");
                continue;
            }

            // Trigger block building on top of the imported block
            self.orchestrator_handle.build_block();

            // Wait for BlockSequenced event
            tracing::debug!(target: "scroll::remote_source", "Waiting for block to be built...");
            loop {
                match self.events.next().await {
                    Some(ChainOrchestratorEvent::BlockSequenced(block)) => {
                        tracing::info!(target: "scroll::remote_source",
                            block_number = block.header.number,
                            block_hash = ?block.hash_slow(),
                            "Block built successfully, proceeding to next");
                        break;
                    }
                    Some(ChainOrchestratorEvent::BlockBuildingSkipped) => {
                        tracing::debug!(target: "scroll::remote_source", "Block building skipped (empty block)");
                        break;
                    }
                    Some(_) => {
                        // Ignore other events, keep waiting
                    }
                    None => {
                        return Err(eyre::eyre!("Event stream ended unexpectedly"));
                    }
                }
            }

            // Loop continues to process next block
        }
    }
}

#[cfg(test)]
mod tests {
    /// `redact_remote` is the only thing between a URL carrying basic-auth
    /// credentials or a query-string API key and an `error!` line: alloy wraps
    /// `reqwest::Error`, whose `Display` appends `for url ({url})` with both
    /// intact. A regression here is silent by construction.
    #[test]
    fn redact_remote_scrubs_credentials_and_query() {
        let url: reqwest::Url =
            "https://ops:s3cr3t@rpc.internal:8545/v1?apikey=ABC".parse().unwrap();
        let message = format!("error sending request for url ({url})");

        let redacted = super::redact_remote(Some(&url), &message);
        assert!(!redacted.contains("s3cr3t"), "credential survived: {redacted}");
        assert!(!redacted.contains("ABC"), "API key survived: {redacted}");
        assert!(!redacted.contains("ops:"), "userinfo survived: {redacted}");
        assert!(redacted.contains("https://rpc.internal:8545"), "host lost: {redacted}");

        // Without a configured URL there is nothing to scrub against, and the
        // message must still pass through rather than be dropped.
        assert_eq!(super::redact_remote(None, "plain failure"), "plain failure");
        // A message that never mentions the URL is unchanged.
        assert_eq!(super::redact_remote(Some(&url), "timed out"), "timed out");

        // THE path that actually occurs: reqwest strips userinfo into an
        // Authorization header before building the request, so its error text
        // carries the stripped URL, which never matches the configured string.
        // Scrubbing only the configured form meant configuring credentials was
        // what switched the redaction off.
        let stripped = "https://rpc.internal:8545/v1?apikey=ABC";
        let from_reqwest = format!("error sending request for url ({stripped})");
        let redacted = super::redact_remote(Some(&url), &from_reqwest);
        assert!(!redacted.contains("ABC"), "API key survived the stripped form: {redacted}");
        assert!(!redacted.contains("/v1"), "path survived the stripped form: {redacted}");
        assert!(redacted.contains("https://rpc.internal:8545"), "host lost: {redacted}");
    }

    /// The sync-error log renders the report with `{:?}`, so eyre's `Caused by:`
    /// chain and `Location:` survive into the log line. The redaction therefore
    /// has to hold across the multi-line form, where the URL can appear both
    /// in the top-level message and again inside the cause chain.
    #[test]
    fn redact_remote_scrubs_full_eyre_report() {
        let url: reqwest::Url =
            "https://ops:s3cr3t@rpc.internal:8545/v1?apikey=ABC".parse().unwrap();
        let stripped = "https://rpc.internal:8545/v1?apikey=ABC";
        let message = format!("error sending request for url ({stripped})");
        let transport = std::io::Error::other(message);
        let report = eyre::Report::new(transport).wrap_err(format!("polling {url}"));
        let rendered = format!("{report:?}");
        assert!(rendered.contains("Caused by:"), "fixture lost the cause chain: {rendered}");
        assert!(rendered.contains("Location:"), "fixture lost the location: {rendered}");

        let redacted = super::redact_remote(Some(&url), &rendered);
        assert!(!redacted.contains("s3cr3t"), "credential survived: {redacted}");
        assert!(!redacted.contains("ops:"), "userinfo survived: {redacted}");
        assert!(!redacted.contains("ABC"), "API key survived: {redacted}");
        assert!(!redacted.contains("/v1"), "path survived: {redacted}");
        assert!(redacted.contains("Caused by:"), "cause chain lost: {redacted}");
        assert!(redacted.contains("Location:"), "location lost: {redacted}");
        assert!(redacted.contains("polling https://rpc.internal:8545"), "{redacted}");
        assert!(
            redacted.contains("error sending request for url (https://rpc.internal:8545)"),
            "{redacted}"
        );
    }

    /// A non-2xx response is rendered as `HTTP error {status} with body: {body}`
    /// with the body verbatim, so a gateway that echoes the request target or
    /// the credentials produces text that matches neither URL form. Each
    /// component has to be scrubbed on its own, and the status line and the
    /// safe host have to survive so the log still says what failed where.
    #[test]
    fn redact_remote_scrubs_echoed_components_from_response_bodies() {
        let url: reqwest::Url =
            "https://ops:ops123@rpc.internal:8545/v2/API_KEY_123?token=QUERY_KEY".parse().unwrap();
        let body = "HTTP error 403 with body: user ops:ops123 denied for \
                    /v2/API_KEY_123?token=QUERY_KEY (retry via https://rpc.internal:8545)";

        let redacted = super::redact_remote(Some(&url), body);
        for leak in ["ops123", "ops:", "API_KEY_123", "QUERY_KEY", "/v2"] {
            assert!(!redacted.contains(leak), "`{leak}` survived the echoed body: {redacted}");
        }
        assert!(redacted.contains("HTTP error 403 with body:"), "status lost: {redacted}");
        assert!(redacted.contains("https://rpc.internal:8545"), "host lost: {redacted}");
        assert_eq!(
            redacted,
            "HTTP error 403 with body: user <username>:<password> denied for \
             <path>?<query> (retry via https://rpc.internal:8545)"
        );

        // A component that is a substring of the safe host must not mangle it:
        // the host is the one thing the line is supposed to keep.
        let url: reqwest::Url = "https://rpc.internal/rpc".parse().unwrap();
        let body = "HTTP error 429 with body: slow down on /rpc";
        let redacted = super::redact_remote(Some(&url), body);
        assert_eq!(redacted, "HTTP error 429 with body: slow down on <path>");
        let with_host = format!("{body} (https://rpc.internal:443)");
        let redacted = super::redact_remote(Some(&url), &with_host);
        assert!(redacted.contains("https://rpc.internal:443"), "host mangled: {redacted}");

        // A URL with nothing beyond the host has nothing to scrub, so the
        // message passes through untouched rather than being sprinkled with
        // markers.
        let bare: reqwest::Url = "https://rpc.internal:8545".parse().unwrap();
        let body = "HTTP error 502 with body: upstream / unavailable";
        assert_eq!(super::redact_remote(Some(&bare), body), body);
    }

    /// The third pass must not touch eyre's own diagnostics. `Location:` names
    /// a source file, and a configured username or path can be a substring of
    /// it: with `node:…@…/src`, an unbounded replacement turned this file's
    /// `crates/node/src/add_ons/remote_block_source.rs:NNN` into
    /// `crates/<username>/<path>/...`, and it fired with no credentials at all,
    /// since the path is always set.
    #[test]
    fn redact_remote_preserves_location_when_components_are_source_path_substrings() {
        let location = file!();
        assert!(location.contains("/node/src/"), "fixture moved: {location}");
        let body = "HTTP error 403 with body: user node:hunter2 denied for /src?apikey=crates";
        let report = eyre::Report::new(std::io::Error::other(body)).wrap_err("polling remote");
        let rendered = format!("{report:?}");
        assert!(rendered.contains(location), "fixture lost the location: {rendered}");

        let url: reqwest::Url =
            "https://node:hunter2@rpc.internal:8545/src?apikey=crates".parse().unwrap();
        let redacted = super::redact_remote(Some(&url), &rendered);
        assert!(redacted.contains("Caused by:"), "cause chain lost: {redacted}");
        assert!(redacted.contains("Location:"), "location lost: {redacted}");
        assert!(redacted.contains(location), "location mangled: {redacted}");
        assert!(!redacted.contains("hunter2"), "credential survived: {redacted}");
        assert!(
            redacted.contains("user <username>:<password> denied for <path>?<query>"),
            "{redacted}"
        );

        // No userinfo at all: the path on its own must not fire on the source
        // path either.
        let plain: reqwest::Url = "https://rpc.internal:8545/src".parse().unwrap();
        let redacted = super::redact_remote(Some(&plain), &rendered);
        assert!(redacted.contains(location), "location mangled: {redacted}");
        assert!(redacted.contains("denied for <path>?apikey=crates"), "{redacted}");
    }

    /// A query value can be echoed on its own, with or without its key, and
    /// has to be scrubbed individually rather than only as part of the whole
    /// query string. Replacement is bounded, so a short value (`v=2`) does not
    /// eat every `2` in the status code or the prose, and the safe host stays.
    #[test]
    fn redact_remote_scrubs_individual_query_values() {
        let url: reqwest::Url =
            "https://rpc.internal:8545/?apikey=SUPERSECRET&v=2".parse().unwrap();
        let body = "HTTP error 402 with body: v2 API rejected apikey=SUPERSECRET \
                    (SUPERSECRET revoked) for https://rpc.internal:8545";
        assert_eq!(
            super::redact_remote(Some(&url), body),
            "HTTP error 402 with body: v2 API rejected apikey=<query> \
             (<query> revoked) for https://rpc.internal:8545"
        );
    }

    /// reqwest percent-decodes userinfo before it sends it (into an
    /// Authorization header), so a gateway echoes `ops@dogeos.com`, never the
    /// configured `ops%40dogeos.com`. Both forms have to be scrubbed.
    #[test]
    fn redact_remote_scrubs_percent_decoded_credentials() {
        let url: reqwest::Url =
            "https://ops%40dogeos.com:p%40ss%2Fword@rpc.internal:8545/".parse().unwrap();
        assert_eq!(url.username(), "ops%40dogeos.com", "fixture: url normalized the userinfo");
        for echoed in ["ops@dogeos.com:p@ss/word", "ops%40dogeos.com:p%40ss%2Fword"] {
            let body = format!("HTTP error 401 with body: user {echoed} rejected");
            assert_eq!(
                super::redact_remote(Some(&url), &body),
                "HTTP error 401 with body: user <username>:<password> rejected",
                "echoed as `{echoed}`"
            );
        }
        // The `user@host` shape, the other one in which a username names itself.
        let body = "HTTP error 401 with body: ops@dogeos.com@rpc.internal denied";
        assert_eq!(
            super::redact_remote(Some(&url), body),
            "HTTP error 401 with body: <username>@rpc.internal denied"
        );
    }

    #[test]
    fn percent_decode_keeps_malformed_escapes() {
        assert_eq!(super::percent_decode("ops%40dogeos.com"), "ops@dogeos.com");
        assert_eq!(super::percent_decode("100%25"), "100%");
        assert_eq!(super::percent_decode("50%"), "50%");
        assert_eq!(super::percent_decode("a%zzb%4"), "a%zzb%4");
        assert_eq!(super::percent_decode("plain"), "plain");
    }

    #[test]
    fn redact_remote_scrubs_credentials_next_to_sentence_punctuation() {
        let url = "https://ops:ops123@rpc.internal/?apikey=SUPERSECRET".parse().unwrap();
        for body in [
            "HTTP error 401: Invalid password ops123. Invalid API key SUPERSECRET.",
            "HTTP error 401: (ops123...) [...SUPERSECRET]",
        ] {
            let result = super::redact_remote(Some(&url), body);
            assert!(!result.contains("ops123"), "{result}");
            assert!(!result.contains("SUPERSECRET"), "{result}");
            assert!(result.contains("HTTP error 401"), "{result}");
        }
    }

    #[test]
    fn redact_remote_scrubs_individually_echoed_path_keys() {
        let url = "https://rpc.internal/v2/API_KEY_123".parse().unwrap();
        let body = format!("Invalid API key: API_KEY_123. Location: {}", file!());
        let result = super::redact_remote(Some(&url), &body);
        assert!(!result.contains("API_KEY_123"), "{result}");
        assert!(result.contains(file!()), "{result}");
    }

    #[test]
    fn redact_remote_normalizes_equivalent_percent_encodings() {
        let url =
            "https://ops:p%40ss%2Fword@rpc.internal/v2/PATH%2FKEY?key=QUERY%2FKEY".parse().unwrap();
        for password in ["p%40ss%2fword", "p@ss%2Fword", "p@ss/word"] {
            let body = format!("Invalid password={password}. key=QUERY%2fKEY path=PATH%2fKEY");
            let result = super::redact_remote(Some(&url), &body);
            assert_eq!(result, "Invalid password=<password>. key=<query> path=<path>");
        }
        let query_url = "https://rpc.internal/?key=QUERY+SECRET".parse().unwrap();
        assert_eq!(
            super::redact_remote(Some(&query_url), "Invalid key: QUERY SECRET."),
            "Invalid key: <query>."
        );
    }

    /// A path-less URL renders as `scheme://host:port/`. When that string is a
    /// prefix of a longer endpoint in the message, replacing it with the bare
    /// host must not glue the remaining sub-path onto the port.
    #[test]
    fn redact_remote_keeps_path_separator_on_prefix_match() {
        let url: reqwest::Url = "https://ops:s3cr3t@rpc.internal:8545".parse().unwrap();
        let credentialed = "error for url (https://ops:s3cr3t@rpc.internal:8545/eth/v1/blocks)";
        let stripped = "error for url (https://rpc.internal:8545/eth/v1/blocks)";
        for message in [credentialed, stripped] {
            let redacted = super::redact_remote(Some(&url), message);
            assert_eq!(redacted, "error for url (https://rpc.internal:8545/eth/v1/blocks)");
        }

        // An exact match keeps the trailing separator the URL itself carries.
        let exact = "error for url (https://ops:s3cr3t@rpc.internal:8545/)";
        assert_eq!(
            super::redact_remote(Some(&url), exact),
            "error for url (https://rpc.internal:8545/)"
        );
    }

    /// The host form used both for the `remote_host` log field and as the
    /// replacement text above: scheme, host and port only.
    #[test]
    fn safe_remote_host_drops_userinfo_path_and_query() {
        let url: reqwest::Url =
            "https://ops:s3cr3t@rpc.internal:8545/v1?apikey=ABC".parse().unwrap();
        assert_eq!(super::safe_remote_host(&url), "https://rpc.internal:8545");
        // Default port is filled in rather than rendered as 0.
        let plain: reqwest::Url = "http://example.com/rpc".parse().unwrap();
        assert_eq!(super::safe_remote_host(&plain), "http://example.com:80");
    }
}
