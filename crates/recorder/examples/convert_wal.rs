use std::path::Path;                                                            
use recorder::parquet_converter::convert_wal;                                   
                                                          
fn main() {
    let wal_path = Path::new("data/wal/binance/2026-06-05.wal");
    let output_dir = Path::new("data/parquet/binance/2026-06-05");

    match convert_wal(wal_path, output_dir) {
        Ok(()) => {
            println!("Conversion complete.");
            // list generated files
            for entry in std::fs::read_dir(output_dir).unwrap() {
                let entry = entry.unwrap();
                let size = entry.metadata().unwrap().len();
                println!("  {} ({} bytes)", entry.file_name().to_string_lossy(),
 size);
            }
        }
        Err(e) => println!("Error: {e}"),
    }
}