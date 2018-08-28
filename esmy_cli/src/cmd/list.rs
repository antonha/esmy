use docopt::Docopt;
use esmy::analyzis::Analyzer;
use esmy::index::IndexBuilder;
use esmy::search;
use esmy::search::Collector;
use esmy::search::TextQuery;
use esmy::seg::SegmentReader;
use esmy::Error;
use serde_json;
use std::io;
use std::path::PathBuf;

static USAGE: &'static str = concat!(
    "
Searches and prints all results from an esmy index.

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
    let query = parse_query(&query_string, &*analyzer);

    let index_manager = IndexBuilder::new().open(index_path)?;
    let index_reader = index_manager.open_reader()?;
    let mut collector = PrintAllCollector::new();
    search::search(&index_reader, &query, &mut collector)?;
    Ok(())
}

fn parse_query<'a>(query_string: &'a str, analyzer: &'a Analyzer) -> TextQuery<'a> {
    let split: Vec<&str> = query_string.split(":").collect();
    TextQuery::new(split[0], split[1], analyzer)
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
        match serde_json::to_writer(io::stdout(), &doc) {
            _ => (),
        }
        println!();
        Ok(())
    }
}
