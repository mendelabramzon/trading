use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::Read;
use venue_core::Event;

/// Frame layout v1 (13-byte header):
///
/// ```text
/// [magic: u32 LE = b"WAL1"][version: u8 = 1][len: u32 LE][crc32: u32 LE][payload: len bytes]
/// ```
///
/// The CRC32 covers `version ‖ len ‖ payload`. MAGIC is frozen across format
/// versions — the version byte is the only evolution mechanism. The payload is
/// rmp-serde (MessagePack): structs encode positionally (field order and arity
/// are load-bearing; field names are not), enum variants encode by name
/// (variant names are load-bearing; adding variants is the only additive
/// channel). See the `encoding_probe` tests, which pin these properties.
pub const MAGIC: [u8; 4] = *b"WAL1";
pub const WIRE_VERSION: u8 = 1;
pub const HEADER_LEN: usize = 13;
pub const MAX_FRAME_LEN: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub enum WireError {
    Encode(String),
    Decode(String),
    /// Slice does not contain a complete frame (short header or short payload).
    InsufficientData,
    BadMagic {
        offset: u64,
    },
    /// Version byte differs from `WIRE_VERSION`. Never resynced past: a
    /// version mismatch means the whole file needs a different decoder, not
    /// corruption recovery.
    BadVersion(u8),
    BadCrc,
    FrameTooLarge {
        len: usize,
    },
    Io(std::io::Error),
}

impl std::fmt::Display for WireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WireError::Encode(msg) => write!(f, "encode error: {msg}"),
            WireError::Decode(msg) => write!(f, "decode error: {msg}"),
            WireError::InsufficientData => write!(f, "insufficient data"),
            WireError::BadMagic { offset } => write!(f, "bad magic at offset {offset}"),
            WireError::BadVersion(v) => write!(f, "unsupported wire version {v}"),
            WireError::BadCrc => write!(f, "crc mismatch"),
            WireError::FrameTooLarge { len } => write!(f, "frame length {len} exceeds maximum"),
            WireError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for WireError {}

impl From<std::io::Error> for WireError {
    fn from(e: std::io::Error) -> Self {
        WireError::Io(e)
    }
}

/// Encode any serializable value into a v1 frame. `.wal` files hold `Event`
/// frames, `.rawwal` files hold `RawFrame`s — same framing, distinguished by
/// file extension only.
pub fn encode_frame<T: Serialize>(value: &T, buf: &mut Vec<u8>) -> Result<(), WireError> {
    let payload = rmp_serde::to_vec(value).map_err(|e| WireError::Encode(e.to_string()))?;
    if payload.len() > MAX_FRAME_LEN {
        return Err(WireError::FrameTooLarge { len: payload.len() });
    }
    let len = (payload.len() as u32).to_le_bytes();
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[WIRE_VERSION]);
    hasher.update(&len);
    hasher.update(&payload);
    let crc = hasher.finalize();

    buf.extend_from_slice(&MAGIC);
    buf.push(WIRE_VERSION);
    buf.extend_from_slice(&len);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(())
}

/// Decode one frame from the front of a slice (strict: no resync).
/// Returns the value and the number of bytes consumed.
pub fn decode_frame<T: DeserializeOwned>(buf: &[u8]) -> Result<(T, usize), WireError> {
    if buf.len() < HEADER_LEN {
        return Err(WireError::InsufficientData);
    }
    if buf[0..4] != MAGIC {
        return Err(WireError::BadMagic { offset: 0 });
    }
    let version = buf[4];
    if version != WIRE_VERSION {
        return Err(WireError::BadVersion(version));
    }
    let len = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;
    if len > MAX_FRAME_LEN {
        return Err(WireError::FrameTooLarge { len });
    }
    if buf.len() < HEADER_LEN + len {
        return Err(WireError::InsufficientData);
    }
    let stored_crc = u32::from_le_bytes(buf[9..13].try_into().unwrap());
    let payload = &buf[HEADER_LEN..HEADER_LEN + len];
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[version]);
    hasher.update(&buf[5..9]);
    hasher.update(payload);
    if hasher.finalize() != stored_crc {
        return Err(WireError::BadCrc);
    }
    let value = rmp_serde::from_slice(payload).map_err(|e| WireError::Decode(e.to_string()))?;
    Ok((value, HEADER_LEN + len))
}

/// Encode an Event into a v1 frame.
pub fn encode(event: &Event, buf: &mut Vec<u8>) -> Result<(), WireError> {
    encode_frame(event, buf)
}

/// Decode one Event frame from the front of a slice (strict: no resync).
pub fn decode(buf: &[u8]) -> Result<(Event, usize), WireError> {
    decode_frame(buf)
}

#[derive(Debug, Default, Clone)]
pub struct FrameReaderStats {
    /// Frames decoded successfully.
    pub frames_ok: u64,
    /// Bytes discarded while scanning for the next MAGIC.
    pub skipped_bytes: u64,
    /// Corruption events that triggered a resync scan.
    pub resyncs: u64,
    /// CRC-valid frames whose payload failed rmp decode (e.g. written by a
    /// newer binary with additional payload variants). Skipped exactly, no
    /// resync.
    pub undecodable_frames: u64,
    /// File ended mid-frame (partial header or payload outside a resync scan).
    pub truncated_tail: bool,
}

/// Streaming, self-healing frame reader.
///
/// Recovery policy:
/// - Bad magic / bad CRC / absurd length → resync: scan forward for the next
///   MAGIC starting at `bad_frame_offset + 1` (a corrupted `len` must never
///   decide the skip), validating candidate headers fully before trusting them.
/// - CRC-valid frame that fails rmp decode → skip exactly that frame.
/// - `BadVersion` outside a resync scan → fatal, sticky: the file belongs to a
///   different decoder (P1). During resync, a wrong version byte just means a
///   false-positive MAGIC inside payload bytes and scanning continues.
pub struct FrameReader<R: Read> {
    inner: R,
    buf: Vec<u8>,
    start: usize,
    eof: bool,
    in_resync: bool,
    fatal_version: Option<u8>,
    stats: FrameReaderStats,
}

const FILL_CHUNK: usize = 256 * 1024;

impl<R: Read> FrameReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(FILL_CHUNK),
            start: 0,
            eof: false,
            in_resync: false,
            fatal_version: None,
            stats: FrameReaderStats::default(),
        }
    }

    pub fn stats(&self) -> &FrameReaderStats {
        &self.stats
    }

    fn fill(&mut self) -> Result<(), WireError> {
        if self.start > 0 {
            self.buf.drain(..self.start);
            self.start = 0;
        }
        let mut chunk = [0u8; FILL_CHUNK];
        let n = self.inner.read(&mut chunk)?;
        if n == 0 {
            self.eof = true;
        } else {
            self.buf.extend_from_slice(&chunk[..n]);
        }
        Ok(())
    }

    fn enter_resync(&mut self) {
        if !self.in_resync {
            self.in_resync = true;
            self.stats.resyncs += 1;
        }
    }

    /// Discard `n` bytes as unrecoverable.
    fn skip(&mut self, n: usize) {
        self.start += n;
        self.stats.skipped_bytes += n as u64;
    }

    /// Read the next frame, healing past corruption. `Ok(None)` = end of file.
    pub fn next_frame<T: DeserializeOwned>(&mut self) -> Result<Option<T>, WireError> {
        loop {
            if let Some(v) = self.fatal_version {
                return Err(WireError::BadVersion(v));
            }
            let avail = self.buf.len() - self.start;

            if avail < HEADER_LEN {
                if !self.eof {
                    self.fill()?;
                    continue;
                }
                if avail > 0 {
                    if !self.in_resync {
                        self.stats.truncated_tail = true;
                    }
                    self.skip(avail);
                }
                return Ok(None);
            }

            if self.buf[self.start..self.start + 4] != MAGIC {
                self.enter_resync();
                // Scan for the next MAGIC candidate past the current byte.
                let window = &self.buf[self.start + 1..];
                match window.windows(4).position(|w| w == MAGIC) {
                    Some(rel) => self.skip(rel + 1),
                    None => {
                        // Keep up to 3 trailing bytes — a MAGIC may straddle the
                        // fill boundary.
                        let keep = window.len().min(3);
                        self.skip(1 + window.len() - keep);
                        if self.eof {
                            self.skip(keep);
                            return Ok(None);
                        }
                        self.fill()?;
                    }
                }
                continue;
            }

            let version = self.buf[self.start + 4];
            if version != WIRE_VERSION {
                if self.in_resync {
                    // False-positive MAGIC inside payload bytes; keep scanning.
                    self.skip(1);
                    continue;
                }
                self.fatal_version = Some(version);
                return Err(WireError::BadVersion(version));
            }

            let len =
                u32::from_le_bytes(self.buf[self.start + 5..self.start + 9].try_into().unwrap())
                    as usize;
            if len > MAX_FRAME_LEN {
                self.enter_resync();
                self.skip(1);
                continue;
            }

            if avail < HEADER_LEN + len {
                if !self.eof {
                    self.fill()?;
                    continue;
                }
                if self.in_resync {
                    // Unverifiable candidate at EOF — could be payload bytes.
                    self.skip(1);
                    continue;
                }
                self.stats.truncated_tail = true;
                self.skip(avail);
                return Ok(None);
            }

            let stored_crc = u32::from_le_bytes(
                self.buf[self.start + 9..self.start + 13]
                    .try_into()
                    .unwrap(),
            );
            let payload = &self.buf[self.start + HEADER_LEN..self.start + HEADER_LEN + len];
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&[version]);
            hasher.update(&self.buf[self.start + 5..self.start + 9]);
            hasher.update(payload);
            if hasher.finalize() != stored_crc {
                self.enter_resync();
                self.skip(1);
                continue;
            }

            // Frame boundary is now trusted (CRC-valid).
            match rmp_serde::from_slice::<T>(payload) {
                Ok(value) => {
                    self.start += HEADER_LEN + len;
                    self.stats.frames_ok += 1;
                    self.in_resync = false;
                    return Ok(Some(value));
                }
                Err(_) => {
                    self.start += HEADER_LEN + len;
                    self.stats.undecodable_frames += 1;
                    self.in_resync = false;
                    continue;
                }
            }
        }
    }

    /// Typed convenience for `.wal` files.
    pub fn next_event(&mut self) -> Result<Option<Event>, WireError> {
        self.next_frame()
    }
}

/// Pins the rmp-serde wire properties the whole system depends on:
/// structs are positional (field order/arity load-bearing), enum variants are
/// tagged by name (variant names load-bearing, adding variants is additive).
/// If any of these assertions ever fail after a dependency bump, the wire
/// format changed and `WIRE_VERSION` must be bumped.
#[cfg(test)]
mod encoding_probe {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    enum E {
        A { x: u32, y: u32 },
        B { x: u32, y: u32 },
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct S {
        a: u32,
        b: u32,
    }

    // Mirror of S with fields swapped — same wire bytes iff structs are positional.
    #[derive(Serialize, Deserialize, Debug)]
    struct SSwapped {
        b: u32,
        a: u32,
    }

    // Mirror of E with variants reordered — decode succeeds iff variants are by name.
    #[derive(Serialize, Deserialize, Debug)]
    enum EReordered {
        B { x: u32, y: u32 },
        A { x: u32, y: u32 },
    }

    #[test]
    fn probe_rmp_serde_layout() {
        // Enum variants are tagged by NAME: the bytes contain ASCII "B", and
        // decoding into a variant-reordered enum still yields B.
        let b = rmp_serde::to_vec(&E::B { x: 7, y: 9 }).unwrap();
        assert!(
            b.contains(&0x42),
            "variant name 'B' not on the wire — enum tagging changed"
        );
        let decoded: EReordered = rmp_serde::from_slice(&b).unwrap();
        assert!(
            matches!(decoded, EReordered::B { x: 7, y: 9 }),
            "variant-reordered decode mismatch: {decoded:?}"
        );

        // Structs are POSITIONAL tuples: no field names on the wire, and a
        // field-swapped mirror silently swaps the values.
        let s = rmp_serde::to_vec(&S { a: 1, b: 2 }).unwrap();
        assert_eq!(s, vec![0x92, 0x01, 0x02], "struct layout changed");
        let swapped: SSwapped = rmp_serde::from_slice(&s).unwrap();
        assert!(
            swapped.a == 2 && swapped.b == 1,
            "structs no longer positional: {swapped:?}"
        );
    }
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
            local_ts: 1_700_000_000_100_000_000,
            source: SourceId(1),
            provenance: None,
            payload: Payload::Market(MarketPayload::BookTicker {
                best_bid: Level {
                    price: dec!(50000.50),
                    qty: dec!(1.5),
                },
                best_ask: Level {
                    price: dec!(50001.00),
                    qty: dec!(2.0),
                },
                update_id: 400_900_217,
            }),
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
            local_ts: 1_700_000_001_100_000_000,
            source: SourceId(1),
            provenance: None,
            payload: Payload::Market(MarketPayload::Trades {
                trades: vec![Trade {
                    id: "26129".into(),
                    price: dec!(2000.25),
                    qty: dec!(10.0),
                    aggressor_side: AggressorSide::Buy,
                    kind: Some("MARKET".into()),
                }],
            }),
        }
    }

    fn make_liquidation_event() -> Event {
        Event {
            venue: VenueId {
                value: "binance".into(),
            },
            instrument: Some(InstrumentId {
                value: "btcusdt".into(),
            }),
            venue_ts: Some(1_700_000_002_000_000_000),
            local_ts: 1_700_000_002_100_000_000,
            source: SourceId(2),
            provenance: None,
            payload: Payload::Market(MarketPayload::Liquidation {
                side: AggressorSide::Sell,
                price: dec!(9910),
                qty: dec!(0.014),
                filled_qty: Some(dec!(0.014)),
                avg_price: Some(dec!(9910)),
                order_status: Some("FILLED".into()),
            }),
        }
    }

    fn make_control_event() -> Event {
        Event {
            venue: VenueId {
                value: "binance".into(),
            },
            instrument: None,
            venue_ts: None,
            local_ts: 1_700_000_003_000_000_000,
            source: SourceId(1),
            provenance: None,
            payload: Payload::Control(ControlPayload::ConnDown {
                label: "ws-1".into(),
                reason: "read error".into(),
            }),
        }
    }

    fn encode_all(events: &[Event]) -> Vec<u8> {
        let mut buf = Vec::new();
        for e in events {
            encode(e, &mut buf).unwrap();
        }
        buf
    }

    fn read_all(buf: &[u8]) -> (Vec<Event>, FrameReaderStats) {
        let mut reader = FrameReader::new(buf);
        let mut out = Vec::new();
        while let Some(e) = reader.next_event().unwrap() {
            out.push(e);
        }
        (out, reader.stats().clone())
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
        assert_eq!(decoded.local_ts, 1_700_000_000_100_000_000);
        assert_eq!(decoded.source, SourceId(1));
        assert_eq!(decoded.provenance, None);

        match &decoded.payload {
            Payload::Market(MarketPayload::BookTicker {
                best_bid,
                best_ask,
                update_id,
            }) => {
                assert_eq!(best_bid.price, dec!(50000.50));
                assert_eq!(best_bid.qty, dec!(1.5));
                assert_eq!(best_ask.price, dec!(50001.00));
                assert_eq!(best_ask.qty, dec!(2.0));
                assert_eq!(*update_id, 400_900_217);
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
        assert_eq!(decoded.source, SourceId(1));

        match &decoded.payload {
            Payload::Market(MarketPayload::Trades { trades }) => {
                assert_eq!(trades.len(), 1);
                assert_eq!(trades[0].id.as_ref(), "26129");
                assert_eq!(trades[0].price, dec!(2000.25));
                assert_eq!(trades[0].qty, dec!(10.0));
                assert!(matches!(trades[0].aggressor_side, AggressorSide::Buy));
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn test_roundtrip_liquidation() {
        let event = make_liquidation_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        let (decoded, _) = decode(&buf).unwrap();
        assert_eq!(decoded.payload, event.payload);
    }

    #[test]
    fn test_roundtrip_control() {
        let event = make_control_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        let (decoded, _) = decode(&buf).unwrap();
        assert_eq!(decoded.instrument, None);
        assert_eq!(decoded.venue_ts, None);
        assert_eq!(decoded.payload, event.payload);
    }

    #[test]
    fn test_header_layout() {
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();
        assert_eq!(&buf[0..4], &MAGIC);
        assert_eq!(buf[4], WIRE_VERSION);
        let len = u32::from_le_bytes(buf[5..9].try_into().unwrap()) as usize;
        assert_eq!(HEADER_LEN + len, buf.len());
    }

    #[test]
    fn test_insufficient_data_empty() {
        assert!(matches!(decode(&[]), Err(WireError::InsufficientData)));
    }

    #[test]
    fn test_insufficient_data_short_header() {
        assert!(matches!(decode(b"WAL"), Err(WireError::InsufficientData)));
    }

    #[test]
    fn test_insufficient_data_truncated_payload() {
        let event = make_book_ticker_event();
        let mut buf = Vec::new();
        encode(&event, &mut buf).unwrap();

        let truncated = &buf[..buf.len() - 5];
        assert!(matches!(
            decode(truncated),
            Err(WireError::InsufficientData)
        ));
    }

    #[test]
    fn test_decode_bad_magic() {
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();
        buf[0] = b'X';
        assert!(matches!(decode(&buf), Err(WireError::BadMagic { .. })));
    }

    #[test]
    fn test_decode_bad_version() {
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();
        buf[4] = 9;
        assert!(matches!(decode(&buf), Err(WireError::BadVersion(9))));
    }

    #[test]
    fn test_decode_bad_crc() {
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert!(matches!(decode(&buf), Err(WireError::BadCrc)));
    }

    #[test]
    fn test_reader_clean_stream() {
        let events = vec![
            make_book_ticker_event(),
            make_trade_event(),
            make_book_ticker_event(),
        ];
        let buf = encode_all(&events);
        let (out, stats) = read_all(&buf);
        assert_eq!(out.len(), 3);
        assert_eq!(stats.frames_ok, 3);
        assert_eq!(stats.resyncs, 0);
        assert_eq!(stats.skipped_bytes, 0);
        assert!(!stats.truncated_tail);
    }

    #[test]
    fn test_reader_empty_input() {
        let (out, stats) = read_all(&[]);
        assert!(out.is_empty());
        assert!(!stats.truncated_tail);
    }

    #[test]
    fn test_reader_corrupt_frame_resyncs() {
        let events = vec![
            make_book_ticker_event(),
            make_trade_event(),
            make_book_ticker_event(),
        ];
        let mut buf = encode_all(&events);
        // Corrupt one payload byte of the middle frame → CRC mismatch.
        let mut one = Vec::new();
        encode(&events[0], &mut one).unwrap();
        buf[one.len() + HEADER_LEN + 3] ^= 0xFF;

        let (out, stats) = read_all(&buf);
        assert_eq!(out.len(), 2, "first and third frames survive");
        assert_eq!(stats.resyncs, 1);
        assert!(stats.skipped_bytes > 0);
        assert!(matches!(
            &out[1].payload,
            Payload::Market(MarketPayload::BookTicker { .. })
        ));
    }

    #[test]
    fn test_reader_resync_skips_magic_inside_payload() {
        // A frame whose payload contains the literal MAGIC bytes, corrupted so
        // the reader must resync across it without trusting the embedded magic.
        let mut decoy = make_trade_event();
        decoy.instrument = Some(InstrumentId {
            value: "WAL1WAL1WAL1".into(),
        });
        let events = vec![make_book_ticker_event(), decoy, make_trade_event()];
        let mut buf = encode_all(&events);
        let mut one = Vec::new();
        encode(&events[0], &mut one).unwrap();
        // Corrupt the decoy's CRC (header byte 9) so its boundary is untrusted.
        buf[one.len() + 9] ^= 0xFF;

        let (out, stats) = read_all(&buf);
        assert_eq!(out.len(), 2, "first and third frames survive");
        assert_eq!(stats.resyncs, 1);
        match &out[1].payload {
            Payload::Market(MarketPayload::Trades { trades }) => {
                assert_eq!(trades.len(), 1);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn test_reader_truncated_tail() {
        let events = vec![make_book_ticker_event(), make_trade_event()];
        let mut buf = encode_all(&events);
        let mut tail = Vec::new();
        encode(&make_book_ticker_event(), &mut tail).unwrap();
        buf.extend_from_slice(&tail[..tail.len() / 2]);

        let (out, stats) = read_all(&buf);
        assert_eq!(out.len(), 2);
        assert!(stats.truncated_tail);
        assert_eq!(stats.resyncs, 0);
    }

    #[test]
    fn test_reader_bad_version_aborts_sticky() {
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();

        // Append a structurally valid frame with version 2 (CRC recomputed).
        let payload = rmp_serde::to_vec(&make_trade_event()).unwrap();
        let len = (payload.len() as u32).to_le_bytes();
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&[2u8]);
        hasher.update(&len);
        hasher.update(&payload);
        let crc = hasher.finalize();
        buf.extend_from_slice(&MAGIC);
        buf.push(2u8);
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&payload);
        // A good frame after it must NOT be reached (no resync past versions).
        encode(&make_trade_event(), &mut buf).unwrap();

        let mut reader = FrameReader::new(&buf[..]);
        assert!(reader.next_event().unwrap().is_some());
        assert!(matches!(reader.next_event(), Err(WireError::BadVersion(2))));
        // Sticky: subsequent calls keep failing rather than resyncing.
        assert!(matches!(reader.next_event(), Err(WireError::BadVersion(2))));
        assert_eq!(reader.stats().frames_ok, 1);
    }

    #[test]
    fn test_reader_frame_too_large_resyncs() {
        // Header claiming an absurd length, followed by a good frame.
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC);
        buf.push(WIRE_VERSION);
        buf.extend_from_slice(&(u32::MAX).to_le_bytes());
        buf.extend_from_slice(&[0u8; 4]);
        encode(&make_trade_event(), &mut buf).unwrap();

        let (out, stats) = read_all(&buf);
        assert_eq!(out.len(), 1);
        assert_eq!(stats.resyncs, 1);
    }

    #[test]
    fn test_reader_undecodable_payload_skipped_exactly() {
        // A CRC-valid frame whose payload is not a valid Event (simulates a
        // frame written by a newer binary with additional payload variants).
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();
        encode_frame(&("not", "an", "event"), &mut buf).unwrap();
        encode(&make_trade_event(), &mut buf).unwrap();

        let (out, stats) = read_all(&buf);
        assert_eq!(out.len(), 2);
        assert_eq!(stats.undecodable_frames, 1);
        assert_eq!(stats.resyncs, 0, "no resync scan for a CRC-valid frame");
        assert_eq!(stats.skipped_bytes, 0);
    }

    /// Golden bytes for one fixed Event. Freezes the full frame layout —
    /// header and rmp payload (positional fields, named variants). If this
    /// test fails, the wire format changed: either revert the change or bump
    /// `WIRE_VERSION` and document the migration (additive enum variants do
    /// NOT trip this test; field reorders/insertions do).
    #[test]
    fn test_golden_frame_bytes() {
        let mut buf = Vec::new();
        encode(&make_book_ticker_event(), &mut buf).unwrap();
        let hex: String = buf.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, GOLDEN_BOOK_TICKER_FRAME_HEX);
    }

    const GOLDEN_BOOK_TICKER_FRAME_HEX: &str = "57414c31015d000000cce2d19c9791a762696e616e636591a762746375736474cf17979cfe362a0000cf17979cfe3c1fe10001c081a64d61726b657481aa426f6f6b5469636b65729392a835303030302e3530a3312e3592a835303030312e3030a3322e30ce17e54079";
}
