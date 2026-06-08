use async_trait::async_trait;
use venue_core::{Event, Instrument, InstrumentId, VenueId};

pub enum DataType {
    BookTicker,
    BookDepth,
    Trade,
    FundingRate,
    MarkPrice,
    IndexPrice,
}

pub struct Subscription {
    pub instrument: InstrumentId,
    pub data_type: Vec<DataType>,
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

#[async_trait]
pub trait EventSink: Send + Sync + Clone + 'static {
    async fn send(&self, event: Event) -> Result<(), EventSinkError>;
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

#[async_trait]
impl EventSink for tokio::sync::mpsc::Sender<Event> {
    async fn send(&self, event: Event) -> Result<(), EventSinkError> {
        self.send(event).await.map_err(|_| EventSinkError::Closed)
    }
}

#[async_trait]
pub trait VenueAdapter<S: EventSink>: Send + Sync {
    fn venue_id(&self) -> &VenueId;

    async fn fetch_instruments(&self) -> Result<Vec<Instrument>, VenueError>;

    async fn connect(&mut self) -> Result<(), VenueError>;

    async fn subscribe(&mut self, subscriptions: Vec<Subscription>) -> Result<(), VenueError>;

    async fn disconnect(&mut self) -> Result<(), VenueError>;
}
