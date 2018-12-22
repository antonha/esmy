use std::io;
use std::path::PathBuf;

use docopt::Docopt;
use serde_json;

use esmy::analyzis::Analyzer;
use esmy::doc_iter::DocIter;
use esmy::Error;
use esmy::index::IndexBuilder;
use esmy::search;
use esmy::search::Collector;
use esmy::search::TextQuery;
use esmy::search::TopDocsCollector;
use esmy::seg::SegmentReader;

static USAGE: &'static str = concat!(
    "
Searches and prints all results from an esmy sorted by score.

Usage:
    esmy list <query> [options]
    esmy list --help

Options::
    -p, --path <path>           Path to index to
    -a, --analyzer <analyzer>   Path to index to
    -h, --help                  Show this message
"
);

#[derive(Deserialize)]
struct Args {
    arg_query: String,
    flag_path: String,
    flag_analyzer: Option<String>,
}

pub fn run(argv: &[&str]) -> Result<(), Error> {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.argv(argv.iter().map(|&x| x)).deserialize())
        .unwrap_or_else(|e| e.exit());
    let index_path = PathBuf::from(args.flag_path.clone());
    let analyzer = Analyzer::for_name(&args.flag_analyzer.unwrap());
    let query_string = args.arg_query;
    let query = parse_query(&query_string, analyzer);

    let index_manager = IndexBuilder::new().open(index_path)?;
    let index_reader = index_manager.open_reader()?;
    let mut collector = TopDocsCollector::new(10);
    search::search(&index_reader, &query, &mut collector)?;
    for doc in collector.docs() {
        match serde_json::to_writer(io::stdout(), &doc.doc()) {
            _ => (),
        }
        println!();
    }
    Ok(())
}

fn parse_query(query_string: &str, analyzer: Box<Analyzer>) -> TextQuery {
    let split: Vec<&str> = query_string.split(':').collect();
    TextQuery::new(split[0].to_string(), split[1].to_string(), analyzer)
}

