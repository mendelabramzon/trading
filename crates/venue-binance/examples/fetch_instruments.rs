use tokio::sync::mpsc;
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
                println!(
                    "  {:?} - {}/{} ({:?}) tick={:?} lot={:?}",
                    i.id.value, i.base.0, i.quote.0, i.class, i.tick_size, i.lot_size
                );
            }
        }
        Err(e) => println!("Error: {:?}", e),
    }
}
