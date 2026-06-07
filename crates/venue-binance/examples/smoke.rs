use venue_adapter::{VenueAdapter, Subscription, DataType};
  use venue_core::{Event, InstrumentId};
  use tokio::sync::mpsc;
  use recorder::WalWriter;
  use std::path::PathBuf;

  #[tokio::main]
  async fn main() {
      let (tx, mut rx) = mpsc::channel::<Event>(1000);
      let mut adapter = venue_binance::BinanceAdapter::new(tx);

      // start the WAL writer on its own thread
      let wal = WalWriter::new(PathBuf::from("data/wal"));

      adapter.connect().await.expect("connect failed");

      let subs = vec![
          Subscription {
              instrument: InstrumentId { value: "btcusdt".into() },
              data_type: vec![DataType::BookTicker, DataType::Trade],
          },
          Subscription {
              instrument: InstrumentId { value: "ethusdt".into() },
              data_type: vec![DataType::BookTicker],
          },
      ];

      adapter.subscribe(subs).await.expect("subscribe failed");

      while let Some(event) = rx.recv().await {
          wal.send(&event);
          println!("{:?}", event);
      }
  }
