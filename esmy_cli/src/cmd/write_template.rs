use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use docopt::Docopt;
use serde_json;

use esmy::index::write_index_meta;
use esmy::index::IndexMeta;
use esmy::seg::FeatureMeta;
use esmy::Error;

static USAGE: &'static str = concat!(
    "
Writes a template to an index.

Usage:
    esmy write-template [options]
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
    let feature_template_metas: HashMap<String, FeatureMeta> =
        serde_json::from_reader(io::stdin()).unwrap();
    write_index_meta(
        &index_path,
        &IndexMeta {
            feature_template_metas,
        },
    )
}
