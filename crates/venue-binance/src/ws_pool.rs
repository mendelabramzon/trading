use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use venue_adapter::{EventSink, VenueError};
use venue_core::VenueId;
use crate::{WsWriter, handle_message, BASE_WS_URL};




struct WsConn {
    writer: WsWriter,
    read_handle: JoinHandle<()>,
    stream_count: usize,
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
    ) -> Result<(), VenueError> {
        for chunk in streams.chunks(self.max_streams_per_conn) {
            // open connection
            let (ws_stream, _) = connect_async(BASE_WS_URL)
                .await
                .map_err(|e| VenueError::ConnectionFailed(e.to_string()))?;

            let (mut writer, mut reader) = ws_stream.split();

            // send subscribe message
            *next_id += 1;
            let msg = serde_json::json!({
                "method": "SUBSCRIBE",
                "params": chunk,
                "id": *next_id,
            });
            writer.send(Message::Text(msg.to_string().into())).await
                .map_err(|e| VenueError::SubscriptionFailed(e.to_string()))?;

            // spawn reader task
            let sink = sink.clone();
            let venue_id = venue_id.clone();
            let read_handle = tokio::spawn(async move {
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(Message::Text(text)) => {
                            handle_message(&text, &venue_id, &sink).await;
                        }
                        Ok(Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            });

            self.conns.push(WsConn {
                writer,
                stream_count: chunk.len(),
                read_handle,
            });
        }

        Ok(())
    }

    pub(crate) async fn disconnect(&mut self) -> Result<(), VenueError> {
        for mut conn in self.conns.drain(..) {
            let _ = conn.writer.close().await;
            conn.read_handle.abort();
        }
        Ok(())
    }
}