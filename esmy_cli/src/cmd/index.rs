use docopt::Docopt;
use esmy::doc::Doc;
use esmy::index::IndexBuilder;
use esmy::Error;
use serde_json;
use std::io;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

static USAGE: &'static str = concat!(
    "
Index input json data from standard input to an esmy index.

Usage:
    esmy index [options]
    esmy index --help

Options::
    -p, --path <path>    Path to index to
    -h, --help          Show this message
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
    let index_path = PathBuf::from(args.flag_path);
    let index_manager = IndexBuilder::new().open(index_path.clone())?;
    let (sender, receiver) = mpsc::sync_channel(100_000);
    thread::spawn(move || {
        let stream =
            serde_json::Deserializer::from_reader(BufReader::new(io::stdin())).into_iter::<Doc>();
        for doc in stream {
            sender.send(doc).unwrap();
        }
    });

    for doc in receiver {
        index_manager.add_doc(doc.unwrap())?;
    }
    index_manager.commit()?;
    Ok(())
}
