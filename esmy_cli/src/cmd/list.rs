use docopt::Docopt;
use esmy::analyzis::Analyzer;
use esmy::full_doc::FullDocCursor;
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
    let query = parse_query(&query_string, analyzer);

    let index_manager = IndexBuilder::new().open(index_path)?;
    let index_reader = index_manager.open_reader()?;
    let mut collector = PrintAllCollector::new();
    search::search(&index_reader, &query, &mut collector)?;
    Ok(())
}

fn parse_query(query_string: &str, analyzer: Box<Analyzer>) -> TextQuery {
    let split: Vec<&str> = query_string.split(':').collect();
    TextQuery::new(split[0].to_string(), split[1].to_string(), analyzer)
}

struct PrintAllCollector {
    doc_cursor: Option<FullDocCursor>,
}

impl PrintAllCollector {
    pub fn new() -> PrintAllCollector {
        PrintAllCollector { doc_cursor: None }
    }
}

impl Collector for PrintAllCollector {
    fn set_reader(&mut self, reader: &SegmentReader) -> Result<(), Error> {
        self.doc_cursor = Some(reader.full_doc().unwrap().cursor()?);
        Ok(())
    }

    fn collect(&mut self, doc_id: u64) -> Result<(), Error> {
        match &mut self.doc_cursor {
            Some(curs) => {
                let doc = curs.read_doc(doc_id)?;
                //TODO error handling could be better
                match serde_json::to_writer(io::stdout(), &doc) {
                    _ => (),
                }
                println!();
            }
            None => {}
        }
        Ok(())
    }
}
