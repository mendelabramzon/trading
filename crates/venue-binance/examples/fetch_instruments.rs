use tokio::sync::mpsc;
use venue_adapter::EventSink;
use venue_adapter::VenueAdapter;
use venue_core::Event;

#[tokio::main]
async fn main() {
    let (tx, _rx) = mpsc::channel::<Event>(100);
    let adapter = venue_binance::BinanceAdapter::new(tx);

    match adapter.fetch_instruments().await {
        Ok(instruments) => {
            println!("Found {} instruments", instruments.len());
            for i in instruments.iter().take(10) {
                println!("  {:?} - {}/{} ({:?})", i.id.value, i.base, i.quote, i.kind);
            }
        }
        Err(e) => println!("Error: {:?}", e),
    }
}
