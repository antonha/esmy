extern crate bzip2;
extern crate esmy;
extern crate quick_xml;
extern crate time;

extern crate jobsteal;

use esmy::analyzis::WhiteSpaceAnalyzer;
use esmy::search;
use esmy::seg::{self, StringIndex};
use std::collections::HashMap;
use std::env;
use std::io::BufRead;
use std::ops::Sub;
use std::path::Path;

fn main() {
    let index_path = env::current_dir()
        .unwrap()
        .join(&Path::new("tmp/tests/index"));
    println!("{:?}", index_path);
    if !index_path.exists() {
        std::fs::create_dir_all(&index_path).unwrap()
    }
    let start_index = time::now();
    let reader = std::io::BufReader::new(
        std::fs::File::open("/home/anton/dev/off/wiki_dump/bodies.txt").unwrap(),
    );
    let features: Vec<Box<seg::Feature>> = vec![
        Box::new(StringIndex::new("text", Box::from(WhiteSpaceAnalyzer {}))),
    ];
    let index = seg::Index::new(seg::SegmentSchema { features }, &index_path);
    let mut i = 0i64;
    let mut builder = index.new_segment();
    for line in reader.lines().take(30_000_000) {
        let body = line.unwrap();
        let mut doc = HashMap::new();
        doc.insert("text".to_owned(), seg::FieldValue::String(body));
        builder.add_doc(doc);
        i += 1;
        if i % 50000 == 0 {
            builder.commit().unwrap();
            let used = time::now().sub(start_index).num_milliseconds();
            println!(
                "Written: {} took: {}, dps: {}",
                i,
                used,
                (i) / (1 + used / 1000)
            );
            builder = index.new_segment();
        }
    }
    builder.commit().unwrap();
    println!(
        "Indexing took: {0}",
        time::now().sub(start_index).num_milliseconds()
    );

    println!("Startnig merging");
    let start_merge = time::now();
    index.merge(&index.list_segments()).unwrap();
    println!(
        "Merging took: {0}",
        time::now().sub(start_merge).num_milliseconds()
    );

    let index_reader = index.open_reader();
    let f2 = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file2 = std::io::BufReader::new(&f2);
    let lines = file2.lines().take(100000).map(|l| l.unwrap());
    let analyzer = esmy::analyzis::UAX29Analyzer {};
    for line in lines.take(100) {
        let start_search = time::now();
        let mut collector = search::CountCollector::new();
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
