use recorder::parquet_converter::convert_wal;
use std::path::Path;

fn main() {
    tracing_subscriber::fmt().init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: convert_wal <wal-file> <output-dir>");
        std::process::exit(2);
    }
    let wal_path = Path::new(&args[1]);
    let output_dir = Path::new(&args[2]);

    match convert_wal(wal_path, output_dir) {
        Ok(()) => {
            println!("Conversion complete.");
            // list generated files
            for entry in std::fs::read_dir(output_dir).unwrap() {
                let entry = entry.unwrap();
                let size = entry.metadata().unwrap().len();
                println!("  {} ({} bytes)", entry.file_name().to_string_lossy(), size);
            }
        }
        Err(e) => println!("Error: {e}"),
    }
}
