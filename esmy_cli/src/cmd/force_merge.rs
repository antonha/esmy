use std::path::PathBuf;

use docopt::Docopt;

use esmy::index::IndexBuilder;
use esmy::Error;

static USAGE: &'static str = concat!(
    "
Searches and prints all results from an esmy index.

Usage:
    esmy force-merge <query> [options]
    esmy force-merge --help

Options::
    -p, --path <path>           Path to index to
    -h, --help                  Show this message
"
);

#[derive(Deserialize)]
struct Args {
    flag_path: String,
}

pub fn run(argv: &[&str]) -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(argv.iter().map(|&x| x)).deserialize())
        .unwrap_or_else(|e| e.exit());
    let index_path = PathBuf::from(args.flag_path.clone());

    let index_manager = IndexBuilder::new().open(index_path)?;
    index_manager.force_merge()
}
