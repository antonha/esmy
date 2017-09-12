extern crate esmy;
extern crate time;

use esmy::seg;
use std::str;
use std::io::BufRead;
use std::ops::Sub;

fn file_starts_with(path: &std::path::Path, prefix: &str) -> bool {
    match path.file_name() {
        Some(name) => {
            match name.to_str() {
                Some(n2) => n2.starts_with(prefix),
                _ => false,
            }
        }
        _ => false,
    }
}

fn main() {
    let index_path = std::path::Path::new("/home/anton/dev/off/esmy/foo/index");
    if !index_path.exists() {
        std::fs::create_dir(&index_path).unwrap()
    }
    for file in std::fs::read_dir(index_path).unwrap() {
        let path = file.unwrap().path();
        std::fs::remove_file(path).unwrap();
    }
    let f = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file = std::io::BufReader::new(&f);
    let words = file.lines().map(|l| l.unwrap());

    let start_index = time::now();
    let index = seg::Index::new(index_path);
    let mut builder = index.new_segment();
    for word in words {
        builder.add_doc(vec![seg::Field {
                                 name: "f",
                                 values: vec![seg::FieldValue::StringField(word)],
                             }]);
    }
    println!("Indexing took: {0}",
             time::now().sub(start_index).num_milliseconds());
    builder.commit().unwrap();

    let readers = index.segment_readers();
    let ref reader = &readers[0];

    let f2 = std::fs::File::open("/usr/share/dict/american-english").unwrap();
    let file2 = std::io::BufReader::new(&f2);
    let words2 = file2.lines().take(100000).map(|l| l.unwrap());
    let start_search = time::now();
    for w2 in words2 {
        for doc in reader.doc_iter("f", w2.as_bytes()).unwrap() {
            let _docid = doc.unwrap();
        }
    }
    println!("Searching took: {0}",
             time::now().sub(start_search).num_milliseconds());
}
