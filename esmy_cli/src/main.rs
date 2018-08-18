extern crate clap;
extern crate esmy;
extern crate serde;
extern crate serde_json;
extern crate time;

use clap::{App, Arg, SubCommand};
use esmy::analyzis::Analyzer;
use esmy::doc::Doc;
use esmy::index_manager::IndexManagerBuilder;
use esmy::search;
use esmy::search::Collector;
use esmy::seg;
use esmy::seg::FeatureMeta;
use esmy::seg::IndexMeta;
use esmy::seg::SegmentReader;
use std::collections::HashMap;
use std::ops::Sub;
use std::path::PathBuf;

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
                    ).arg(
                        Arg::with_name("analyzer")
                            .default_value("noop")
                            .short("a")
                            .help("Which analyzer to use."),
                    ),
            ).subcommand(
                SubCommand::with_name("read-template").about("foo").arg(
                    Arg::with_name("path")
                        .short("p")
                        .default_value(".")
                        .help("The path to index at. Defaults to the current working directory."),
                ),
            ).subcommand(
                SubCommand::with_name("write-template").arg(
                    Arg::with_name("path")
                        .short("p")
                        .default_value(".")
                        .help("The path to index at. Defaults to the current working directory."),
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
        let schema = seg::schema_from_metas(
            seg::read_index_meta(&index_path)
                .unwrap()
                .feature_template_metas,
        );
        let index = seg::Index::new(schema, index_path);
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
        let analyzer_string = matches.value_of("analyzer").unwrap();
        let analyzer = Analyzer::for_name(analyzer_string);
        let query_string = matches.value_of("QUERY").unwrap();

        let schema = seg::schema_from_metas(
            seg::read_index_meta(&index_path)
                .unwrap()
                .feature_template_metas,
        );
        let index = seg::Index::new(schema, index_path);
        let index_manager = IndexManagerBuilder::new().open(index).unwrap();
        let index_reader = index_manager.open_reader();
        let query = search::TextQuery::new("body", &query_string, analyzer.as_ref());
        let mut collector = PrintAllCollector::new();
        search::search(&index_reader, &query, &mut collector).unwrap();
    }
    if let Some(matches) = matches.subcommand_matches("read-template") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let meta = seg::read_index_meta(&index_path).unwrap();
        serde_json::to_writer(std::io::stdout(), &meta.feature_template_metas).unwrap();
    }
    if let Some(matches) = matches.subcommand_matches("write-template") {
        let index_path = PathBuf::from(matches.value_of("path").unwrap());
        let feature_template_metas: HashMap<String, FeatureMeta> =
            serde_json::from_reader(std::io::stdin()).unwrap();
        seg::write_index_meta(
            &index_path,
            &IndexMeta {
                feature_template_metas,
            },
        ).unwrap();
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
