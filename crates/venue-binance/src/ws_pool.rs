use crate::rest::{FundingMap, SnapshotRequest};
use crate::{handle_message, now_nanos, WsReader, WsWriter, BASE_WS_URL};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use venue_adapter::{EventSink, RawFrameSink, VenueError};
use venue_core::{ControlPayload, Event, Payload, RawFrame, SourceId, VenueId};

/// Binance fapi servers ping every ~3 min; a healthy idle connection always
/// shows traffic within 5. Silence beyond that is a zombie connection.
const STALE_CONN_TIMEOUT: Duration = Duration::from_secs(300);
const ACK_TIMEOUT: Duration = Duration::from_secs(10);
/// A session shorter than this is treated as flapping: the next reconnect
/// waits out the backoff instead of retrying immediately.
const STABLE_SESSION: Duration = Duration::from_secs(60);

/// Shared retry policy (1 s doubling to 30 s, jittered). Used by every
/// connection task here and re-exported for venue-process startup retry (N8).
pub struct ExponentialBackoff {
    current: Duration,
    max: Duration,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self::new()
    }
}

impl ExponentialBackoff {
    pub fn new() -> Self {
        Self {
            current: Duration::from_secs(1),
            max: Duration::from_secs(30),
        }
    }

    /// Doubling delay with up to +25% jitter so a venue-wide disconnect does
    /// not stampede every connection back at the same instant.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(self.max);
        let jitter_ms = rand::rng().random_range(0..=delay.as_millis() as u64 / 4);
        delay + Duration::from_millis(jitter_ms)
    }

    pub fn reset(&mut self) {
        self.current = Duration::from_secs(1);
    }
}

/// SUBSCRIBE acknowledgement tracking. Binance may interleave data frames
/// before the `{"result":null,"id":N}` reply; data parsing always runs first
/// and the reply watcher only sees frames the data parser rejected.
enum AckState {
    Awaiting {
        id: u64,
        deadline: tokio::time::Instant,
    },
    Settled,
}

enum ReplyAction {
    AckOk(u64),
    AckErr(u64, String),
    VenueError(String),
    Ignored,
}

#[derive(serde::Deserialize)]
struct WsReply {
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<serde_json::Value>,
    #[serde(default)]
    id: Option<u64>,
}

/// Classify a non-data frame against the pending ack (N12: venue error frames
/// must not vanish at debug level).
fn classify_reply(text: &str, ack: &AckState) -> ReplyAction {
    let Ok(reply) = serde_json::from_str::<WsReply>(text) else {
        return ReplyAction::Ignored;
    };
    let awaiting_id = match ack {
        AckState::Awaiting { id, .. } => Some(*id),
        AckState::Settled => None,
    };
    if let Some(err) = reply.error {
        let detail = err.to_string();
        return match (reply.id, awaiting_id) {
            (Some(id), Some(awaited)) if id == awaited => ReplyAction::AckErr(id, detail),
            _ => ReplyAction::VenueError(detail),
        };
    }
    match (reply.id, awaiting_id) {
        (Some(id), Some(awaited)) if id == awaited && reply.result.is_none() => {
            ReplyAction::AckOk(id)
        }
        _ => ReplyAction::Ignored,
    }
}

enum LoopExit {
    Shutdown,
    Reconnect(&'static str),
}

/// Everything a connection task needs that is independent of one TCP session.
struct ConnCtx<S: EventSink, R: RawFrameSink> {
    venue_id: VenueId,
    sink: S,
    raw: R,
    source: SourceId,
    label: Arc<str>,
    funding: FundingMap,
    snapshot_tx: Option<mpsc::Sender<SnapshotRequest>>,
}

impl<S: EventSink, R: RawFrameSink> ConnCtx<S, R> {
    /// Control events are recorded like market data (A7): venue-scoped, no
    /// instrument, no venue_ts.
    async fn emit_control(&self, payload: ControlPayload) {
        let event = Event {
            venue: self.venue_id.clone(),
            instrument: None,
            venue_ts: None,
            local_ts: now_nanos(),
            source: self.source,
            provenance: None,
            payload: Payload::Control(payload),
        };
        if let Err(e) = self.sink.send(event).await {
            tracing::debug!(error = ?e, "control event dropped (sink closing?)");
        }
    }
}

struct WsConn {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

pub(crate) struct WsPool {
    conns: Vec<WsConn>,
    max_streams_per_conn: usize,
}

impl WsPool {
    pub(crate) fn new(max_streams_per_conn: usize) -> Self {
        Self {
            conns: Vec::new(),
            max_streams_per_conn,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn subscribe<S: EventSink, R: RawFrameSink>(
        &mut self,
        streams: Vec<String>,
        sink: &S,
        raw: &R,
        venue_id: &VenueId,
        next_id: &mut u64,
        funding: FundingMap,
        snapshot_tx: Option<mpsc::Sender<SnapshotRequest>>,
    ) -> Result<(), VenueError> {
        for chunk in streams.chunks(self.max_streams_per_conn) {
            let chunk = chunk.to_vec();
            let conn_no = self.conns.len() + 1;
            // SourceId registry convention: 0 = REST, WS connections from 1.
            let ctx = ConnCtx {
                venue_id: venue_id.clone(),
                sink: sink.clone(),
                raw: raw.clone(),
                source: SourceId(conn_no as u16),
                label: format!("ws-{conn_no}").into(),
                funding: funding.clone(),
                snapshot_tx: snapshot_tx.clone(),
            };

            // Initial connect + SUBSCRIBE inline so the caller gets an
            // immediate error on failure.
            let (ws_stream, _) = connect_async(BASE_WS_URL)
                .await
                .map_err(|e| VenueError::ConnectionFailed(e.to_string()))?;

            tracing::info!(
                connection = conn_no,
                streams = chunk.len(),
                "WebSocket connection opened"
            );

            let (mut writer, reader) = ws_stream.split();

            *next_id += 1;
            let sub_id = *next_id;
            let msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": &chunk,
                "id": sub_id,
            });
            writer
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(|e| VenueError::SubscriptionFailed(e.to_string()))?;

            tracing::info!(
                id = sub_id,
                streams = chunk.len(),
                "subscription message sent"
            );

            let cancel = CancellationToken::new();
            let handle = tokio::spawn(conn_task(
                Some((reader, writer)),
                chunk,
                ctx,
                cancel.clone(),
                sub_id,
            ));

            self.conns.push(WsConn { cancel, handle });
        }

        Ok(())
    }

    pub(crate) async fn disconnect(&mut self) -> Result<(), VenueError> {
        tracing::info!(
            connections = self.conns.len(),
            "disconnecting all WebSocket connections"
        );

        for conn in &self.conns {
            conn.cancel.cancel();
        }

        let handles: Vec<JoinHandle<()>> = self.conns.drain(..).map(|c| c.handle).collect();
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            futures_util::future::join_all(handles),
        )
        .await;

        Ok(())
    }
}

/// One self-healing connection: read until failure, emit ConnDown, reconnect
/// (immediately after a stable session, with jittered backoff otherwise),
/// re-SUBSCRIBE, repeat until cancelled.
async fn conn_task<S: EventSink, R: RawFrameSink>(
    initial: Option<(WsReader, WsWriter)>,
    streams: Vec<String>,
    ctx: ConnCtx<S, R>,
    cancel: CancellationToken,
    mut sub_id: u64,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut session = initial.map(|(r, w)| {
        let ack = AckState::Awaiting {
            id: sub_id,
            deadline: tokio::time::Instant::now() + ACK_TIMEOUT,
        };
        (r, w, ack)
    });
    let mut delay_next = false;

    loop {
        let (mut reader, mut writer, mut ack) = match session.take() {
            Some(s) => s,
            None => {
                if delay_next {
                    let delay = backoff.next_delay();
                    tracing::info!(label = %ctx.label, delay_ms = delay.as_millis() as u64, "reconnecting after backoff");
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel.cancelled() => return,
                    }
                } else {
                    tracing::info!(label = %ctx.label, "reconnecting immediately");
                }

                let ws_stream = match connect_async(BASE_WS_URL).await {
                    Ok((ws, _)) => ws,
                    Err(e) => {
                        tracing::warn!(label = %ctx.label, error = %e, "reconnect failed");
                        delay_next = true;
                        continue;
                    }
                };
                let (mut writer, reader) = ws_stream.split();

                sub_id += 1;
                let msg = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": &streams,
                    "id": sub_id,
                });
                if let Err(e) = writer.send(Message::Text(msg.to_string().into())).await {
                    tracing::warn!(label = %ctx.label, error = %e, "failed to send SUBSCRIBE after reconnect");
                    delay_next = true;
                    continue;
                }
                tracing::info!(label = %ctx.label, streams = streams.len(), "WebSocket reconnected, resubscribed");

                let ack = AckState::Awaiting {
                    id: sub_id,
                    deadline: tokio::time::Instant::now() + ACK_TIMEOUT,
                };
                (reader, writer, ack)
            }
        };

        let session_start = tokio::time::Instant::now();
        // Fresh per session: every reconnect re-triggers the REST snapshot on
        // the first depthUpdate per symbol (the pu-chain broke with the conn).
        let mut depth_seen: HashSet<Arc<str>> = HashSet::new();
        let exit = read_loop(
            &mut reader,
            &mut writer,
            &cancel,
            &ctx,
            &mut ack,
            &mut depth_seen,
        )
        .await;
        match exit {
            LoopExit::Shutdown => {
                ctx.emit_control(ControlPayload::ConnDown {
                    label: ctx.label.clone(),
                    reason: "shutdown".into(),
                })
                .await;
                return;
            }
            LoopExit::Reconnect(reason) => {
                tracing::warn!(label = %ctx.label, reason, "connection lost, will reconnect");
                ctx.emit_control(ControlPayload::ConnDown {
                    label: ctx.label.clone(),
                    reason: reason.into(),
                })
                .await;
                let stable = session_start.elapsed() >= STABLE_SESSION;
                if stable {
                    backoff.reset();
                }
                // Immediate retry only after a stable session; a flapping
                // connection must wait out the (jittered) backoff.
                delay_next = !stable;
            }
        }
    }
}

/// The single read loop for both initial and reconnected sessions. Every text
/// frame is teed raw before parsing; data parsing runs first and only frames
/// it rejects reach the reply watcher.
async fn read_loop<S: EventSink, R: RawFrameSink>(
    reader: &mut WsReader,
    writer: &mut WsWriter,
    cancel: &CancellationToken,
    ctx: &ConnCtx<S, R>,
    ack: &mut AckState,
    depth_seen: &mut HashSet<Arc<str>>,
) -> LoopExit {
    loop {
        // One timeout serves both detectors: the ack deadline while awaiting,
        // the stale-connection watchdog afterwards.
        let read_timeout = match ack {
            AckState::Awaiting { deadline, .. } => STALE_CONN_TIMEOUT
                .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
            AckState::Settled => STALE_CONN_TIMEOUT,
        };

        tokio::select! {
            msg = tokio::time::timeout(read_timeout, reader.next()) => {
                match msg {
                    Err(_elapsed) => {
                        if matches!(ack, AckState::Awaiting { .. }) {
                            tracing::warn!(label = %ctx.label, "SUBSCRIBE not acknowledged in time");
                            ctx.emit_control(ControlPayload::SubAck {
                                request_id: match ack { AckState::Awaiting { id, .. } => *id, _ => 0 },
                                ok: false,
                                detail: Some("ack timeout".into()),
                            }).await;
                            return LoopExit::Reconnect("subscribe ack timeout");
                        }
                        return LoopExit::Reconnect("stale connection (no traffic)");
                    }
                    Ok(Some(Ok(Message::Text(text)))) => {
                        ctx.raw.send_raw(RawFrame {
                            local_ts: now_nanos(),
                            source: ctx.source,
                            bytes: text.as_bytes().to_vec(),
                        });
                        let outcome = handle_message(
                            &text,
                            &ctx.venue_id,
                            &ctx.sink,
                            ctx.source,
                            &ctx.funding,
                        )
                        .await;
                        // First depth update for a symbol this session: fetch
                        // the REST snapshot the diffs splice against (Bug 1).
                        if let Some(symbol) = outcome.depth_symbol {
                            if depth_seen.insert(symbol.clone()) {
                                if let Some(tx) = &ctx.snapshot_tx {
                                    if tx.try_send(SnapshotRequest::new(symbol)).is_err() {
                                        tracing::warn!(label = %ctx.label, "snapshot request queue full");
                                    }
                                }
                            }
                        }
                        if !outcome.is_data {
                            match classify_reply(&text, ack) {
                                ReplyAction::AckOk(id) => {
                                    tracing::info!(label = %ctx.label, id, "SUBSCRIBE acknowledged");
                                    ctx.emit_control(ControlPayload::SubAck {
                                        request_id: id,
                                        ok: true,
                                        detail: None,
                                    }).await;
                                    ctx.emit_control(ControlPayload::ConnUp {
                                        label: ctx.label.clone(),
                                    }).await;
                                    *ack = AckState::Settled;
                                }
                                ReplyAction::AckErr(id, detail) => {
                                    tracing::warn!(label = %ctx.label, id, %detail, "SUBSCRIBE rejected");
                                    ctx.emit_control(ControlPayload::SubAck {
                                        request_id: id,
                                        ok: false,
                                        detail: Some(detail.into()),
                                    }).await;
                                    return LoopExit::Reconnect("subscribe rejected");
                                }
                                ReplyAction::VenueError(detail) => {
                                    tracing::warn!(label = %ctx.label, %detail, "venue error frame");
                                }
                                ReplyAction::Ignored => {}
                            }
                        }
                    }
                    Ok(Some(Ok(Message::Ping(data)))) => {
                        if writer.send(Message::Pong(data)).await.is_err() {
                            return LoopExit::Reconnect("pong send failed");
                        }
                    }
                    Ok(Some(Ok(Message::Close(frame)))) => {
                        tracing::warn!(label = %ctx.label, ?frame, "WebSocket closed by server");
                        return LoopExit::Reconnect("closed by server");
                    }
                    Ok(Some(Err(e))) => {
                        tracing::warn!(label = %ctx.label, error = %e, "WebSocket read error");
                        return LoopExit::Reconnect("read error");
                    }
                    Ok(None) => {
                        return LoopExit::Reconnect("stream ended");
                    }
                    Ok(Some(Ok(_))) => {}
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!(label = %ctx.label, "shutdown signal received, closing connection");
                let _ = writer.close().await;
                return LoopExit::Shutdown;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_caps_and_jitters() {
        let mut b = ExponentialBackoff::new();
        let d1 = b.next_delay();
        assert!(d1 >= Duration::from_secs(1) && d1 <= Duration::from_millis(1250));
        let d2 = b.next_delay();
        assert!(d2 >= Duration::from_secs(2) && d2 <= Duration::from_millis(2500));
        for _ in 0..10 {
            let d = b.next_delay();
            assert!(d <= Duration::from_millis(37_500), "cap 30s + 25% jitter");
        }
        b.reset();
        assert!(b.next_delay() < Duration::from_secs(2));
    }

    fn awaiting(id: u64) -> AckState {
        AckState::Awaiting {
            id,
            deadline: tokio::time::Instant::now() + ACK_TIMEOUT,
        }
    }

    #[tokio::test]
    async fn classify_reply_transitions() {
        assert!(matches!(
            classify_reply(r#"{"result":null,"id":7}"#, &awaiting(7)),
            ReplyAction::AckOk(7)
        ));
        // Reply for a different request id is not our ack.
        assert!(matches!(
            classify_reply(r#"{"result":null,"id":8}"#, &awaiting(7)),
            ReplyAction::Ignored
        ));
        assert!(matches!(
            classify_reply(
                r#"{"error":{"code":2,"msg":"Invalid request"},"id":7}"#,
                &awaiting(7)
            ),
            ReplyAction::AckErr(7, _)
        ));
        // Error frame outside any pending ack is surfaced, not swallowed (N12).
        assert!(matches!(
            classify_reply(
                r#"{"error":{"code":-1121,"msg":"bad symbol"}}"#,
                &AckState::Settled
            ),
            ReplyAction::VenueError(_)
        ));
        assert!(matches!(
            classify_reply("not json", &AckState::Settled),
            ReplyAction::Ignored
        ));
        // Acks after settling are ignored.
        assert!(matches!(
            classify_reply(r#"{"result":null,"id":7}"#, &AckState::Settled),
            ReplyAction::Ignored
        ));
    }
}
