use std::future::Future;
use venue_core::{Event, Instrument, InstrumentClass, InstrumentId, RawFrame, VenueId};

mod source;
pub use source::{IngestSource, SourceSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    BookTicker,
    BookDepth,
    Trade,
    FundingRate,
    MarkPrice,
    IndexPrice,
    Liquidation,
    /// REST-only on most CEXes; captured by pollers, not streams.
    OpenInterest,
}

/// What a subscription covers (A6). Venue-wide streams (`All`) are one stream
/// instead of hundreds and immune to listing lag; adapters map them to the
/// venue's native form where one exists.
#[derive(Debug, Clone)]
pub enum Scope {
    Instruments(Vec<InstrumentId>),
    /// Every instrument of a class; expanded by the Phase-2 universe manager.
    Class(InstrumentClass),
    All,
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub scope: Scope,
    pub data: Vec<DataType>,
}

#[derive(Debug)]
pub enum VenueError {
    ConnectionFailed(String),
    AuthenticationFailed(String),
    InvalidInstrument(InstrumentId),
    SubscriptionFailed(String),
    RequestFailed(String),
}

impl std::fmt::Display for VenueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VenueError::ConnectionFailed(msg) => write!(f, "connection failed: {msg}"),
            VenueError::AuthenticationFailed(msg) => write!(f, "authentication failed: {msg}"),
            VenueError::InvalidInstrument(id) => write!(f, "invalid instrument: {:?}", id.value),
            VenueError::SubscriptionFailed(msg) => write!(f, "subscription failed: {msg}"),
            VenueError::RequestFailed(msg) => write!(f, "request failed: {msg}"),
        }
    }
}

impl std::error::Error for VenueError {}

/// The universal boundary between event producers and consumers.
///
/// Not dyn-compatible (RPITIT); the future event bus will need an erasing
/// wrapper if dynamic dispatch ever becomes necessary.
pub trait EventSink: Send + Sync + Clone + 'static {
    fn send(&self, event: Event) -> impl Future<Output = Result<(), EventSinkError>> + Send;

    /// Send several events produced by one venue message without interleaving
    /// other awaits between them. The default loops `send`; sinks with a
    /// cheaper bulk path (e.g. a WAL sink) should override.
    fn send_batch(
        &self,
        events: Vec<Event>,
    ) -> impl Future<Output = Result<(), EventSinkError>> + Send {
        async move {
            for event in events {
                self.send(event).await?;
            }
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum EventSinkError {
    Closed,
    Full,
}

impl std::fmt::Display for EventSinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventSinkError::Closed => write!(f, "event sink closed"),
            EventSinkError::Full => write!(f, "event sink full"),
        }
    }
}

impl std::error::Error for EventSinkError {}

impl EventSink for tokio::sync::mpsc::Sender<Event> {
    async fn send(&self, event: Event) -> Result<(), EventSinkError> {
        // Resolves to the inherent `Sender::send`, not this trait method.
        self.send(event).await.map_err(|_| EventSinkError::Closed)
    }
}

/// Receives raw venue frames before parsing (the R2 raw-capture tee).
///
/// Synchronous by design: implementations hand the frame to a channel and
/// return; the tee must never add an await point to the hot read loop.
pub trait RawFrameSink: Send + Sync + Clone + 'static {
    fn send_raw(&self, frame: RawFrame);
}

/// No-op tee for venues running without raw capture.
impl RawFrameSink for () {
    fn send_raw(&self, _frame: RawFrame) {}
}

pub trait VenueAdapter<S: EventSink>: Send + Sync {
    fn venue_id(&self) -> &VenueId;

    fn fetch_instruments(&self)
        -> impl Future<Output = Result<Vec<Instrument>, VenueError>> + Send;

    fn connect(&mut self) -> impl Future<Output = Result<(), VenueError>> + Send;

    fn subscribe(
        &mut self,
        subscriptions: Vec<Subscription>,
    ) -> impl Future<Output = Result<(), VenueError>> + Send;

    fn disconnect(&mut self) -> impl Future<Output = Result<(), VenueError>> + Send;
}
