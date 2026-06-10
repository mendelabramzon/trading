use std::fs::File;
use std::io::BufReader;

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: read_wal <path-to-wal-file>");
    let file = File::open(&path).expect("failed to open WAL file");
    let mut reader = wire::FrameReader::new(BufReader::new(file));

    let mut count = 0u64;
    loop {
        match reader.next_event() {
            Ok(Some(event)) => {
                count += 1;
                if count <= 5 {
                    println!("{count}: {event:?}");
                }
            }
            Ok(None) => break,
            Err(e) => {
                println!("fatal decode error after {count} events: {e}");
                break;
            }
        }
    }

    println!("\nTotal events decoded: {count}");
    println!("Reader stats: {:?}", reader.stats());
}
