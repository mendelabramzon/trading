use crate::{handle_message, WsReader, WsWriter, BASE_WS_URL};
use futures_util::{SinkExt, StreamExt};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use venue_adapter::{EventSink, VenueError};
use venue_core::VenueId;

struct ExponentialBackoff {
    current: Duration,
    max: Duration,
}

impl ExponentialBackoff {
    fn new() -> Self {
        Self {
            current: Duration::from_secs(1),
            max: Duration::from_secs(30),
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = (self.current * 2).min(self.max);
        delay
    }

    fn reset(&mut self) {
        self.current = Duration::from_secs(1);
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

    pub(crate) async fn subscribe<S: EventSink>(
        &mut self,
        streams: Vec<String>,
        sink: &S,
        venue_id: &VenueId,
        next_id: &mut u64,
        seq: &Arc<AtomicU64>,
    ) -> Result<(), VenueError> {
        for chunk in streams.chunks(self.max_streams_per_conn) {
            let chunk = chunk.to_vec();

            // Initial connect inline so caller gets immediate error on failure
            let (ws_stream, _) = connect_async(BASE_WS_URL)
                .await
                .map_err(|e| VenueError::ConnectionFailed(e.to_string()))?;

            tracing::info!(
                connection = self.conns.len() + 1,
                streams = chunk.len(),
                "WebSocket connection opened"
            );

            let (mut writer, reader) = ws_stream.split();

            // Send initial SUBSCRIBE
            *next_id += 1;
            let msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": &chunk,
                "id": *next_id,
            });
            writer
                .send(Message::Text(msg.to_string().into()))
                .await
                .map_err(|e| VenueError::SubscriptionFailed(e.to_string()))?;

            tracing::info!(
                id = *next_id,
                streams = chunk.len(),
                "subscription message sent"
            );

            // Spawn self-healing task
            let cancel = CancellationToken::new();
            let sink = sink.clone();
            let venue_id = venue_id.clone();
            let cancel_clone = cancel.clone();
            let seq = Arc::clone(seq);

            let handle = tokio::spawn(connection_task_with_reader(
                reader,
                writer,
                chunk,
                sink,
                venue_id,
                cancel_clone,
                seq,
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

        // Cancel all tasks
        for conn in &self.conns {
            conn.cancel.cancel();
        }

        // Await handles with a timeout
        let handles: Vec<JoinHandle<()>> = self.conns.drain(..).map(|c| c.handle).collect();
        let _ = tokio::time::timeout(
            Duration::from_secs(3),
            futures_util::future::join_all(handles),
        )
        .await;

        Ok(())
    }
}

/// First read loop using the already-opened reader from subscribe().
/// On disconnect, falls into reconnect_loop().
async fn connection_task_with_reader<S: EventSink>(
    mut reader: WsReader,
    mut writer: WsWriter,
    streams: Vec<String>,
    sink: S,
    venue_id: VenueId,
    cancel: CancellationToken,
    seq: Arc<AtomicU64>,
) {
    // Read from the initial connection
    loop {
        tokio::select! {
            msg = reader.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        handle_message(&text, &venue_id, &sink, &seq).await;
                    }
                    Some(Ok(Message::Ping(data))) => {
                        let _ = writer.send(Message::Pong(data)).await;
                    }
                    Some(Ok(Message::Close(frame))) => {
                        tracing::warn!(?frame, "WebSocket closed by server, will reconnect");
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::warn!(error = %e, "WebSocket read error, will reconnect");
                        break;
                    }
                    None => {
                        tracing::warn!("WebSocket stream ended, will reconnect");
                        break;
                    }
                    _ => {}
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!("shutdown signal received, closing connection");
                let _ = writer.close().await;
                return;
            }
        }
    }

    // Initial reader disconnected — enter the reconnect loop
    reconnect_loop(streams, sink, venue_id, cancel, seq).await;
}

/// Outer loop: backoff → connect → subscribe → inner read loop.
/// Runs until cancelled.
async fn reconnect_loop<S: EventSink>(
    streams: Vec<String>,
    sink: S,
    venue_id: VenueId,
    cancel: CancellationToken,
    seq: Arc<AtomicU64>,
) {
    let mut backoff = ExponentialBackoff::new();
    let mut sub_id: u64 = 1;

    loop {
        let delay = backoff.next_delay();
        tracing::info!(delay_secs = delay.as_secs(), "reconnecting after backoff");

        // Cancellation-aware sleep
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = cancel.cancelled() => {
                tracing::info!("shutdown during reconnect backoff");
                return;
            }
        }

        // Attempt to reconnect
        let ws_stream = match connect_async(BASE_WS_URL).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                tracing::warn!(error = %e, "reconnect failed");
                continue;
            }
        };

        let (mut writer, mut reader) = ws_stream.split();

        // Re-subscribe
        sub_id += 1;
        let msg = serde_json::json!({
            "method": "SUBSCRIBE",
            "params": &streams,
            "id": sub_id,
        });
        if let Err(e) = writer.send(Message::Text(msg.to_string().into())).await {
            tracing::warn!(error = %e, "failed to send SUBSCRIBE after reconnect");
            continue;
        }

        tracing::info!(
            streams = streams.len(),
            "WebSocket reconnected, resubscribed"
        );
        backoff.reset();

        // Inner read loop
        loop {
            tokio::select! {
                msg = reader.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            handle_message(&text, &venue_id, &sink, &seq).await;
                        }
                        Some(Ok(Message::Ping(data))) => {
                            let _ = writer.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            tracing::warn!(?frame, "WebSocket closed by server, will reconnect");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::warn!(error = %e, "WebSocket read error, will reconnect");
                            break;
                        }
                        None => {
                            tracing::warn!("WebSocket stream ended, will reconnect");
                            break;
                        }
                        _ => {}
                    }
                }
                _ = cancel.cancelled() => {
                    tracing::info!("shutdown signal received, closing connection");
                    let _ = writer.close().await;
                    return;
                }
            }
        }
    }
}
