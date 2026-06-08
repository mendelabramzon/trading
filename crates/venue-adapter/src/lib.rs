

use venue_core::{Event, VenueId, InstrumentId, Instrument};
use async_trait::async_trait;

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

#[async_trait]
pub trait EventSink: Send + Sync + Clone + 'static {
    async fn send(&self, event: Event) -> Result<(), EventSinkError>;
}

#[derive(Debug)]
pub enum EventSinkError {
    Closed,
    Full,
}

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

    async fn subscribe(&mut self, subscriptions: Vec<Subscription>) ->
Result<(), VenueError>;

    async fn disconnect(&mut self) -> Result<(), VenueError>;
}