use serde::{Deserialize, Serialize};

use crate::{InstrumentId, Nanos, Payload, Sequence, VenueId};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub venue: VenueId,
    pub instrument: Option<InstrumentId>,
    pub venue_ts: Option<Nanos>,
    pub local_ts: Option<Nanos>,
    pub payload: Payload,
    pub sequence: Option<Sequence>,
}

