#[macro_use] extern crate quickcheck;
extern crate esmy;

use std::collections::HashMap;
use esmy::seg;


quickcheck! {
    fn prop_index_(xs: Vec<Vec<u8>>) -> bool {
        let index_path = std::path::Path::new("/Users/anton/dev/off/esmy/foo/index");
        if !index_path.exists() {
            std::fs::create_dir(&index_path).unwrap()
        }
        let docs : Vec<[seg::FieldToStore; 1]> = xs.iter().map({|val|[seg::FieldToStore::new("f", &[val as &[u8]])]}).collect();
        seg::write_segment(
            index_path, &"foo",
            &[
                &[&seg::FieldToStore::new("f", &[b"abc", b"efg"])],
                &[&seg::FieldToStore::new("f", &[b"abc", b"hjk"])],
            ]
        ).unwrap();
    let segment = seg::SegmentReader::new(index_path, &"foo");
    true
    }
}
