extern crate esmy;
extern crate time;

use esmy::analyzis::WhiteSpaceAnalyzer;
use esmy::search;
use esmy::index_manager::IndexManager;
use esmy::seg::{self, FullDoc, StringIndex};
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
    } else {
        std::fs::remove_dir_all(&index_path).unwrap()
    }
    let start_index = time::now();
    let reader = std::io::BufReader::new(
        std::fs::File::open("/home/anton/dev/off/wiki_dump/bodies.txt").unwrap(),
    );
    let features: Vec<Box<seg::Feature>> = vec![
        Box::new(StringIndex::new("text", Box::from(WhiteSpaceAnalyzer {}))),
        Box::new(FullDoc::new()),
    ];
    let index = seg::Index::new(seg::SegmentSchema { features }, index_path);
    let mut index_manager = IndexManager::new(index);
    let mut i = 0i64;
    for line in reader.lines() {
        let body = line.unwrap();
        let mut doc = HashMap::new();
        doc.insert("text".to_owned(), seg::FieldValue::String(body));
        index_manager.add_doc(doc);
        i+=1;
        if i % 50000 == 0 {
            let used = time::now().sub(start_index).num_milliseconds();
            println!(
                "Written: {} took: {}, dps: {}",
                i,
                used,
                (i) / (1 + used / 1000)
            );
        }
    }
    println!(
        "Indexing took: {0}",
        time::now().sub(start_index).num_milliseconds()
    );

    /*
    println!("Startnig merging");
    let start_merge = time::now();
    index.merge(&index.list_segments()).unwrap();
    println!(
        "Merging took: {0}",
        time::now().sub(start_merge).num_milliseconds()
    );*/

    let index_reader = index_manager.open_reader();
    let f2 = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file2 = std::io::BufReader::new(&f2);
    let lines = file2.lines().take(100000).map(|l| l.unwrap());
    let analyzer = esmy::analyzis::UAX29Analyzer {};
    for line in lines.take(1000) {
        let start_search = time::now();
        let mut collector = search::AllDocsCollector::new();
        let query = search::TextQuery::new("text", &line, &analyzer);
        search::search(&index_reader, &query, &mut collector).unwrap();
        println!(
            "Word '{}' had {} matches, took {} ms",
            line,
            collector.docs().len(),
            time::now().sub(start_search).num_milliseconds() as f32
        );
        for doc in collector.docs().iter().take(5) {
            println!("{:?}", doc)
        }
    }
}
