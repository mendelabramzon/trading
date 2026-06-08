use venue_core::Event;

#[derive(Debug)]
pub enum WireError {
    Encode(String),
    Decode(String),
    InsufficientData,
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Encode(msg) => write!(f, "encode error: {msg}"),
            WireError::Decode(msg) => write!(f, "decode error: {msg}"),
            WireError::InsufficientData => write!(f, "insufficient data"),
        }
    }
}

impl std::error::Error for WireError {}

/// Encode an Event into a length-prefixed frame: [len: u32][msgpack bytes]
pub fn encode(event: &Event, buf: &mut Vec<u8>) -> Result<(), WireError> {
    let payload = rmp_serde::to_vec(event).map_err(|e| WireError::Encode(e.to_string()))?;
    let len = payload.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(())
}

/// Decode an Event from a raw msgpack payload (without length prefix).
pub fn decode_payload(payload: &[u8]) -> Result<Event, WireError> {
    rmp_serde::from_slice(payload).map_err(|e| WireError::Decode(e.to_string()))
}

/// Decode a length-prefixed frame from a byte slice.
/// Returns the Event and the number of bytes consumed.
pub fn decode(buf: &[u8]) -> Result<(Event, usize), WireError> {
    if buf.len() < 4 {
        return Err(WireError::InsufficientData);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if buf.len() < 4 + len {
        return Err(WireError::InsufficientData);
    }
    let event =
        rmp_serde::from_slice(&buf[4..4 + len]).map_err(|e| WireError::Decode(e.to_string()))?;
    Ok((event, 4 + len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use venue_core::*;

    fn make_book_ticker_event() -> Event {
        Event {
            venue: VenueId {
                value: "binance".into(),
            },
            instrument: Some(InstrumentId {
                value: "btcusdt".into(),
            }),
            venue_ts: Some(1_700_000_000_000_000_000),
            local_ts: Some(1_700_000_000_100_000_000),
            payload: Payload::MarketData(MarketDataPayload::BookTicker {
                best_bid: Level {
                    price: dec!(50000.50),
                    qty: dec!(1.5),
                },
                best_ask: Level {
                    price: dec!(50001.00),
                    qty: dec!(2.0),
                },
            }),
            sequence: Some(42),
        }
    }

    fn make_trade_event() -> Event {
        Event {
            venue: VenueId {
                value: "binance".into(),
            },
            instrument: Some(InstrumentId {
                value: "ethusdt".into(),
            }),
            venue_ts: Some(1_700_000_001_000_000_000),
            local_ts: Some(1_700_000_001_100_000_000),
            payload: Payload::MarketData(MarketDataPayload::Trades {
                trades: vec![Trade {
                    price: dec!(2000.25),
                    qty: dec!(10.0),
                    aggressor_side: AggressorSide::Buy,
                }],
            }),
            sequence: Some(43),
        }
    }

    #[test]
    fn test_roundtrip_book_ticker() {
        let event = make_book_ticker_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        let (decoded, consumed) = decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.venue.value.as_ref(), "binance");
        assert_eq!(
            decoded.instrument.as_ref().unwrap().value.as_ref(),
            "btcusdt"
        );
        assert_eq!(decoded.venue_ts, Some(1_700_000_000_000_000_000));
        assert_eq!(decoded.local_ts, Some(1_700_000_000_100_000_000));
        assert_eq!(decoded.sequence, Some(42));

        match &decoded.payload {
            Payload::MarketData(MarketDataPayload::BookTicker { best_bid, best_ask }) => {
                assert_eq!(best_bid.price, dec!(50000.50));
                assert_eq!(best_bid.qty, dec!(1.5));
                assert_eq!(best_ask.price, dec!(50001.00));
                assert_eq!(best_ask.qty, dec!(2.0));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn test_roundtrip_trade() {
        let event = make_trade_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        let (decoded, consumed) = decode(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded.sequence, Some(43));

        match &decoded.payload {
            Payload::MarketData(MarketDataPayload::Trades { trades }) => {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].price, dec!(2000.25));
                assert_eq!(trades[0].qty, dec!(10.0));
                assert!(matches!(trades[0].aggressor_side, AggressorSide::Buy));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn test_decode_payload() {
        let event = make_book_ticker_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        // Strip the 4-byte length prefix
        let payload_bytes = &buf[4..];
        let decoded = decode_payload(payload_bytes).unwrap();
        assert_eq!(decoded.venue.value.as_ref(), "binance");
    }

    #[test]
    fn test_insufficient_data_empty() {
        assert!(matches!(decode(&[]), Err(WireError::InsufficientData)));
    }

    #[test]
    fn test_insufficient_data_short_header() {
        assert!(matches!(
            decode(&[0, 0, 0]),
            Err(WireError::InsufficientData)
        ));
    }

    #[test]
    fn test_insufficient_data_truncated_payload() {
        let event = make_book_ticker_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        // Truncate the buffer (keep header but only part of payload)
        let truncated = &buf[..buf.len() - 5];
        assert!(matches!(
            decode(truncated),
            Err(WireError::InsufficientData)
        ));
    }
}
