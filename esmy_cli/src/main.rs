extern crate docopt;
extern crate esmy;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
extern crate time;

use docopt::Docopt;
use esmy::Error;
use std::env;
mod cmd;

static USAGE: &'static str = concat!(
    "
Usage:
    esmy <command> [<args>...]
    esmy [options]

Options:
    -h, --help  Show this message
    command -h  Show command message

Commands: 
    index               Indexes content
    list                Lists content matching a query
    write-template      Writes template to index
    read-template       Reads template from path 

"
);

#[derive(Deserialize)]
struct Args {
    arg_command: Option<Command>,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.options_first(true).deserialize())
        .unwrap_or_else(|e| e.exit());
    match args.arg_command {
        None => {
            eprintln!("No subcommand selected");
        }
        Some(cmd) => cmd.run().unwrap(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Command {
    Index,
    List,
    Delete,
    ForceMerge,
    WriteTemplate,
    ReadTemplate,
}

impl Command {
    fn run(self) -> Result<(), Error> {
        let argv: Vec<_> = env::args().map(|v| v.to_owned()).collect();
        let argv: Vec<_> = argv.iter().map(|s| &**s).collect();
        let argv = &*argv;
        match self {
            Command::Index => cmd::index::run(argv),
            Command::List => cmd::list::run(argv),
            Command::Delete => cmd::delete::run(argv),
            Command::ForceMerge => cmd::force_merge::run(argv),
            Command::ReadTemplate => cmd::read_template::run(argv),
            Command::WriteTemplate => cmd::write_template::run(argv),
        }
    }
}
