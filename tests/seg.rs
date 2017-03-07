#[macro_use]
extern crate quickcheck;
extern crate esmy;

use std::collections::HashMap;
use esmy::seg;




quickcheck! {
    fn prop_index_(xs: Vec<HashMap<String, Vec<Vec<u8>>>>) -> bool {
        let index_path = std::path::Path::new("/Users/anton/dev/off/esmy/foo/index");
        if !index_path.exists() {
            std::fs::create_dir(&index_path).unwrap()
        }
        seg::write_segment(
            index_path, &"foo",
            xs,
        ).unwrap();
    let segment = seg::SegmentReader::new(index_path, &"foo");
    true
    }
}
