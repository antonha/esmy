extern crate esmy;
extern crate time;
extern crate quick_xml;
extern crate bzip2;
//extern crate serde;
//extern crate serde_json;

#[macro_use]
extern crate serde_derive;
extern crate jobsteal;

use bzip2::read::{BzDecoder, BzEncoder};
use esmy::analyzis::{Analyzer, UAX29Analyzer, WhiteSpaceAnalyzer};
use esmy::search;
use esmy::seg::{self, Field, StringIndex, StringValues};
use std::env;
use std::io::BufRead;
use std::ops::Sub;
use std::path::Path;
use std::str;
use std::sync::mpsc;
//use serde_json::{Deserializer, Value};
use std::sync::mpsc::{Receiver, Sender};


fn main() {

    let index_path = env::current_dir().unwrap().join(
        &Path::new("tmp/tests/index"),
    );
    println!("{:?}", index_path);
    if !index_path.exists() {
        std::fs::create_dir_all(&index_path).unwrap()
    }
    let f = std::fs::File::open(env::current_dir().unwrap().join("rows.out")).unwrap();
    let file = std::io::BufReader::new(&f);

    let mut pool = jobsteal::make_pool(4).unwrap();

    let start_index = time::now();

    let reader = std::io::BufReader::new(
        std::fs::File::open("/home/anton/dev/off/wiki_dump/bodies.txt").unwrap(),
    );
    let mut i = 0i64;
    let mut bodies = vec![];
    pool.scope(|scope| {
        for line in reader.lines().take(300000) {
            bodies.push(line.unwrap());
            i += 1;
            if i % 5000 == 0 {
                let p = index_path.clone();
                let features: Vec<Box<seg::Feature>> =
                    vec![
                    Box::new(StringIndex::new("text", Box::from(WhiteSpaceAnalyzer {}))),
               //     Box::new(StringValues::new("text")),
                    ];
                scope.submit(move || {
                    let index = seg::Index::new(seg::SegmentSchema { features }, &p);
                    let mut builder = index.new_segment();
                    for body in bodies {
                        builder.add_doc(vec![
                            seg::Field {
                                name: "text",
                                value: seg::FieldValue::StringField(vec![body]),
                            },
                        ]);
                    }
                    builder.commit().unwrap();
                    let used = time::now().sub(start_index).num_milliseconds();
                    println!(
                        "Written: {} took: {}, dps: {}",
                        i,
                        used,
                        (i) / (1 + used / 1000)
                    );
                    println!("Done writing: {}", builder.name());
                });
                bodies = vec![];
            }
        }
        let p = index_path.clone();
        let features: Vec<Box<seg::Feature>> =
            vec![
                    Box::new(StringIndex::new("text", Box::from(WhiteSpaceAnalyzer {}))),
               //     Box::new(StringValues::new("text")),
                    ];
        scope.submit(move || {
            let index = seg::Index::new(seg::SegmentSchema { features }, &p);
            let mut builder = index.new_segment();
            for body in bodies {
                builder.add_doc(vec![
                    seg::Field {
                        name: "text",
                        value: seg::FieldValue::StringField(vec![body]),
                    },
                ]);
            }
            builder.commit().unwrap();
            let used = time::now().sub(start_index).num_milliseconds();
            println!(
                "Written: {} took: {}, dps: {}",
                i,
                used,
                (i) / (1 + used / 1000)
            );
            println!("Done writing: {}", builder.name());
        });
        bodies = vec![];
    });
    println!(
        "Indexing took: {0}",
        time::now().sub(start_index).num_milliseconds()
    );
    let features: Vec<Box<seg::Feature>> =
        vec![
        Box::new(StringIndex::new("text", Box::from(WhiteSpaceAnalyzer {}))),
        //     Box::new(StringValues::new("text")),
        ];
    let index = seg::Index::new(seg::SegmentSchema { features }, &index_path);
    println!("WOOP");
    let index_reader = index.open_reader();
    let f2 = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file2 = std::io::BufReader::new(&f2);
    let lines = file2.lines().take(100000).map(|l| l.unwrap());
    let analyzer = esmy::analyzis::WhiteSpaceAnalyzer {};
    for line in lines {
        let start_search = time::now();
        let mut collector = search::CountCollector::new();
        println!("Looking at '{}'", line);
        search::search(
            &index_reader,
            &search::TextQuery::new("text", &line, &analyzer),
            &mut collector,
        ).unwrap();
        println!(
            "Word '{}' had {} matches, took {} ms",
            line,
            collector.total_count(),
            time::now().sub(start_search).num_milliseconds()
        );
    }
}
