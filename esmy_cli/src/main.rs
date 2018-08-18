extern crate clap;
extern crate esmy;
extern crate serde;
extern crate serde_json;
extern crate time;

use clap::{App, Arg, SubCommand};
use esmy::analyzis::UAX29Analyzer;
use esmy::analyzis::WhiteSpaceAnalyzer;
use esmy::index_manager::IndexManagerBuilder;
use esmy::search;
use esmy::search::Collector;
use std::ops::Sub;
use std::path::PathBuf;
use esmy::full_doc::FullDoc;
use esmy::string_index::StringIndex;
use esmy::seg::Feature;
use esmy::seg;
use esmy::doc::Doc;
use esmy::seg::SegmentReader;

fn main() {
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
                    ).arg(
                        Arg::with_name("clear")
                            .short("c")
                            .help("If the index path should be cleared before indexing.."),
                    ).arg(
                        Arg::with_name("v")
                            .short("v")
                            .multiple(true)
                            .help("Sets the level of verbosity"),
                    ),
            ).subcommand(
                SubCommand::with_name("list")
                    .about("Lists all documents matching a query.")
                    .arg(
                        Arg::with_name("path").short("p").default_value(".").help(
                            "The path to index at. Defaults to the current working directory.",
                        ),
                    ).arg(
                        Arg::with_name("QUERY")
                            .required(true)
                            .index(1)
                            .help("If the index path should be cleared before indexing.."),
                    ),
            ).get_matches();

    if let Some(matches) = matches.subcommand_matches("index") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let verbose = matches.occurrences_of("v") > 0;
        let clear = matches.is_present("clear");

        if verbose {
            eprintln!("Will index into '{:?}'", &index_path);
        }
        if clear && index_path.exists() {
            std::fs::remove_dir_all(&index_path).unwrap();
        }
        if !index_path.exists() {
            std::fs::create_dir_all(&index_path).unwrap()
        }
        let features: Vec<Box<Feature>> = vec![
            Box::new(StringIndex::new("body", Box::from(WhiteSpaceAnalyzer {}))),
            Box::new(FullDoc::new()),
        ];
        let index = seg::Index::new(seg::SegmentSchema { features }, index_path);
        let mut index_manager = IndexManagerBuilder::new().open(index).unwrap();
        let start_index = time::now();
        let stream =
            serde_json::Deserializer::from_reader(std::io::BufReader::new(std::io::stdin()))
                .into_iter::<Doc>();
        let mut i = 0i64;
        for doc in stream {
            index_manager.add_doc(doc.unwrap());
            i += 1;
            if verbose && i % 50000 == 0 {
                let used = time::now().sub(start_index).num_milliseconds();
                eprintln!(
                    "Written: {} took: {}, dps: {}",
                    i,
                    used,
                    (i) / (1 + used / 1000)
                );
            }
        }
        index_manager.commit().unwrap();
        if verbose {
            eprintln!(
                "Indexing took: {0}",
                time::now().sub(start_index).num_milliseconds()
            );
        }
    }
    if let Some(matches) = matches.subcommand_matches("list") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let query_string = matches.value_of("QUERY").unwrap();

        let features: Vec<Box<seg::Feature>> = vec![
            Box::new(StringIndex::new("body", Box::from(UAX29Analyzer {}))),
            Box::new(FullDoc::new()),
        ];
        let index = seg::Index::new(seg::SegmentSchema { features }, index_path);
        let index_manager =
            IndexManagerBuilder::new().open(index)
                .unwrap();
        let index_reader = index_manager.open_reader();
        let analyzer = esmy::analyzis::UAX29Analyzer {};
        let query = search::TextQuery::new("body", &query_string, &analyzer);
        let mut collector = PrintAllCollector::new();
        search::search(&index_reader, &query, &mut collector).unwrap();
    }
}

struct PrintAllCollector {}

impl PrintAllCollector {
    pub fn new() -> PrintAllCollector {
        PrintAllCollector {}
    }
}

impl Collector for PrintAllCollector {
    fn collect(&mut self, reader: &SegmentReader, doc_id: u64) {
        let doc = reader.full_doc().unwrap().read_doc(doc_id).unwrap();
        serde_json::to_writer(std::io::stdout(), &doc).unwrap();
        println!();
    }
}
