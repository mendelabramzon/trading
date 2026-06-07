use std::fs;                                              

  fn main() {
      let data = fs::read("data/wal/binance/2026-06-05.wal")
          .expect("failed to read WAL file");

      let mut offset = 0;
      let mut count = 0;

      while offset < data.len() {
          match wire::decode(&data[offset..]) {
              Ok((event, consumed)) => {
                  count += 1;
                  if count <= 5 {
                      println!("{count}: {event:?}");
                  }
                  offset += consumed;
              }
              Err(e) => {
                  println!("decode error at offset {offset}: {e:?}");
                  break;
              }
          }
      }

      println!("\nTotal events decoded: {count}");
  }
