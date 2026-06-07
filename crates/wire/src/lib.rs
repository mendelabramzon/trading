use venue_core::Event;

#[derive(Debug)]
pub enum WireError {
    Encode(String),
    Decode(String),
    InsufficientData,
}

/// Encode an Event into a length-prefixed frame: [len: u32][bincode bytes]
pub fn encode(event: &Event, buf: &mut Vec<u8>) -> Result<(), WireError> {
    let payload = rmp_serde::to_vec(event)
        .map_err(|e| WireError::Encode(e.to_string()))?;
    let len = payload.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(())
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
    let event = rmp_serde::from_slice(&buf[4..4 + len])
        .map_err(|e| WireError::Decode(e.to_string()))?;
    Ok((event, 4 + len))
}