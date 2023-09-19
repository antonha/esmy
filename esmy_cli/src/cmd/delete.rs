use std::path::PathBuf;

use docopt::Docopt;

use esmy::analyzis::Analyzer;
use esmy::index::IndexBuilder;
use esmy::search::TextQuery;
use esmy::Error;

static USAGE: &'static str = concat!(
    "
Searches and prints all results from an esmy index.

Usage:
    esmy delete <query> [options]
    esmy delete --help

Options::
    -p, --path <path>           Path to index to
    -a, --analyzer <analyzer>   Analyzer to use for query
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
    let analyzer = <dyn Analyzer>::for_name(&args.flag_analyzer.unwrap());
    let query_string = args.arg_query;
    let query = parse_query(&query_string, analyzer);

    let index_manager = IndexBuilder::new().open(index_path)?;
    index_manager.delete(&query)?;
    Ok(())
}

fn parse_query(query_string: &str, analyzer: Box<dyn Analyzer>) -> TextQuery {
    let split: Vec<&str> = query_string.split(':').collect();
    TextQuery::new(split[0].to_string(), split[1].to_string(), analyzer)
}
