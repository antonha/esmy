use docopt::Docopt;
use esmy::doc::Doc;
use esmy::index::IndexBuilder;
use esmy::Error;
use serde_json;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::sync::mpsc;
use std::thread;

static USAGE: &'static str = concat!(
    "
Index input json data from standard input to an esmy index.

Usage:
    esmy index [options]
    esmy index --help

Options::
    -p, --path <path>   Path to index to
    -f, --file <path>   File to index from 
    --no-merge          Do not enable merges 
    -h, --help          Show this message
"
);

#[derive(Deserialize)]
struct Args {
    flag_path: String,
    flag_file: Option<String>,
    flag_no_merge: bool,
}

pub fn run(argv: &[&str]) -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(argv.iter().map(|&x| x)).deserialize())
        .unwrap_or_else(|e| e.exit());
    let index_manager = IndexBuilder::new()
        .auto_merge(!args.flag_no_merge)
        .open(::std::fs::canonicalize(args.flag_path).unwrap())?;
    let (sender, receiver) = mpsc::sync_channel(20000);
    let flag_file = args.flag_file.clone();
    thread::Builder::new()
        .name("esmy-reader-thread-1".to_owned())
        .spawn(move || -> Result<(), Error> {
            match flag_file {
                Some(file) => {
                    let stream =
                        serde_json::Deserializer::from_reader(BufReader::new(File::open(file)?))
                            .into_iter::<Doc>();
                    for doc in stream {
                        sender.send(doc).unwrap();
                    }
                    Ok(())
                }
                None => {
                    let stream = serde_json::Deserializer::from_reader(BufReader::new(io::stdin()))
                        .into_iter::<Doc>();
                    for doc in stream {
                        sender.send(doc).unwrap();
                    }
                    Ok(())
                }
            }
        }).unwrap();

    for doc in receiver {
        index_manager.add_doc(doc.unwrap())?;
    }
    index_manager.commit()?;
    Ok(())
}
