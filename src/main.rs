extern crate esmy;
extern crate time;

use esmy::seg::{self, Field, StringIndex, StringValues};
use std::env;
use std::io::BufRead;
use std::ops::Sub;
use std::path::Path;

fn main() {
    let index_path = env::current_dir().unwrap().join(
        &Path::new("tmp/tests/index"),
    );
    println!("{:?}", index_path);
    if !index_path.exists() {
        std::fs::create_dir_all(&index_path).unwrap()
    }
    let f = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file = std::io::BufReader::new(&f);
    let words = file.lines().map(|l| l.unwrap());

    let start_index = time::now();
    let features: Vec<Box<seg::Feature>> = vec![
        Box::new(StringIndex::new("value")),
        Box::new(StringValues::new("key")),
    ];
    let index = seg::Index::new(seg::SegmentSchema { features }, &index_path);
    let mut builder = index.new_segment();
    for word in words {
        builder.add_doc(vec![
            Field {
                name: "f",
                value: seg::FieldValue::StringField(vec![word]),
            },
        ]);
    }
    println!(
        "Indexing took: {0}",
        time::now().sub(start_index).num_milliseconds()
    );
    builder.commit().unwrap();

    let index_reader = index.open_reader();
    let readers = &index_reader.segment_readers();
    let ref reader = &readers[0];

    let f2 = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file2 = std::io::BufReader::new(&f2);
    let words2 = file2.lines().take(100000).map(|l| l.unwrap());
    let start_search = time::now();
    let mut i: u32 = 0;
    for w2 in words2 {
        i += 1;
        for doc in reader
            .string_index("value")
            .unwrap()
            .doc_iter("value", &w2)
            .unwrap()
        {
            let _docid = doc.unwrap();
        }
    }
    println!(
        "Searching took ({0}): {1}",
        i,
        time::now().sub(start_search).num_milliseconds()
    );
}
