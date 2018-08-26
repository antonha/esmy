extern crate clap;
extern crate esmy;
extern crate serde;
extern crate serde_json;
extern crate time;

use clap::{App, Arg, SubCommand};
use esmy::analyzis::Analyzer;
use esmy::doc::Doc;
use esmy::index::read_index_meta;
use esmy::index::write_index_meta;
use esmy::index::IndexBuilder;
use esmy::index::IndexMeta;
use esmy::search;
use esmy::search::Collector;
use esmy::seg::FeatureMeta;
use esmy::seg::SegmentReader;
use esmy::Error;
use std::collections::HashMap;
use std::ops::Sub;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;

fn main() {
    match esmy_main() {
        Ok(()) => (),
        Err(e) => eprint!("Failed: {:?}", e),
    }
}

fn esmy_main() -> Result<(), Error> {
    let matches =
        App::new("esmy")
            .version("0.1.0")
            .author("Anton Hägerstrand <anton.hagerstrand@gmail.com>")
            .about("CLI interface for Esmy")
            .subcommand(
                SubCommand::with_name("index")
                    .about("Indexes documents.")
                    .arg(
                        Arg::with_name("path").short("p").default_value(".").help(
                            "The path to index at. Defaults to the current working directory.",
                        ),
                    )
                    .arg(
                        Arg::with_name("clear")
                            .short("c")
                            .help("If the index path should be cleared before indexing.."),
                    )
                    .arg(
                        Arg::with_name("v")
                            .short("v")
                            .multiple(true)
                            .help("Sets the level of verbosity"),
                    ),
            )
            .subcommand(
                SubCommand::with_name("list")
                    .about("Lists all documents matching a query.")
                    .arg(
                        Arg::with_name("path").short("p").default_value(".").help(
                            "The path to index at. Defaults to the current working directory.",
                        ),
                    )
                    .arg(
                        Arg::with_name("QUERY")
                            .required(true)
                            .index(1)
                            .help("If the index path should be cleared before indexing.."),
                    )
                    .arg(
                        Arg::with_name("analyzer")
                            .default_value("noop")
                            .short("a")
                            .help("Which analyzer to use."),
                    ),
            )
            .subcommand(
                SubCommand::with_name("read-template").about("foo").arg(
                    Arg::with_name("path")
                        .short("p")
                        .default_value(".")
                        .help("The path to index at. Defaults to the current working directory."),
                ),
            )
            .subcommand(
                SubCommand::with_name("write-template").arg(
                    Arg::with_name("path")
                        .short("p")
                        .default_value(".")
                        .help("The path to index at. Defaults to the current working directory."),
                ),
            )
            .get_matches();

    if let Some(matches) = matches.subcommand_matches("index") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let verbose = matches.occurrences_of("v") > 0;
        let clear = matches.is_present("clear");

        if verbose {
            eprintln!("Will index into '{:?}'", &index_path);
        }
        if clear && index_path.exists() {
            std::fs::remove_dir_all(&index_path)?;
        }
        if !index_path.exists() {
            std::fs::create_dir_all(&index_path)?
        }
        let mut index_manager = IndexBuilder::new().open(index_path.clone())?;
        let start_index = time::now();

        let (sender, receiver) = mpsc::sync_channel(100_000);
        thread::spawn(move || {
            let stream = serde_json::Deserializer::from_reader(std::io::BufReader::new(
                std::io::stdin(),
            )).into_iter::<Doc>();
            for doc in stream {
                sender.send(doc).unwrap();
            }
        });

        let mut i = 0i64;
        for doc in receiver {
            index_manager.add_doc(doc.unwrap())?;
            i += 1;
            if verbose && i % 50000 == 0 {
                let used = time::now().sub(start_index).num_milliseconds();
                eprintln!(
                    "Written: {} took: {}, dps: {}",
                    i,
                    used,
                    (i) / (used / 1000)
                );
            }
        }
        index_manager.commit()?;
        if verbose {
            eprintln!(
                "Indexing took: {0}",
                time::now().sub(start_index).num_milliseconds()
            );
        }
    }
    if let Some(matches) = matches.subcommand_matches("list") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let analyzer_string = matches.value_of("analyzer").unwrap();
        let analyzer = Analyzer::for_name(analyzer_string);
        let query_string = matches.value_of("QUERY").unwrap();

        let index_manager = IndexBuilder::new().open(index_path)?;
        let index_reader = index_manager.open_reader()?;
        let query = search::TextQuery::new("body", &query_string, analyzer.as_ref());
        let mut collector = PrintAllCollector::new();
        search::search(&index_reader, &query, &mut collector)?;
    }
    if let Some(matches) = matches.subcommand_matches("read-template") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let meta = read_index_meta(&index_path)?;
        serde_json::to_writer(std::io::stdout(), &meta.feature_template_metas).unwrap();
    }
    if let Some(matches) = matches.subcommand_matches("write-template") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let feature_template_metas: HashMap<String, FeatureMeta> =
            serde_json::from_reader(std::io::stdin()).unwrap();
        write_index_meta(
            &index_path,
            &IndexMeta {
                feature_template_metas,
            },
        )?
    }
    Ok(())
}

struct PrintAllCollector {}

impl PrintAllCollector {
    pub fn new() -> PrintAllCollector {
        PrintAllCollector {}
    }
}

impl Collector for PrintAllCollector {
    fn collect(&mut self, reader: &SegmentReader, doc_id: u64) -> Result<(), Error> {
        let doc = reader.full_doc().unwrap().read_doc(doc_id)?;
        //TODO error handling could be better
        match serde_json::to_writer(std::io::stdout(), &doc){
            _ => ()
        }
        println!();
        Ok(())
    }
}
