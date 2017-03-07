extern crate byteorder;
extern crate esmy;
extern crate time;

use esmy::seg;
use std::str;

fn file_starts_with(path: &std::path::Path, prefix: &str) -> bool {
    match path.file_name() {
        Some(name) => {
            match name.to_str(){
                Some(n2) => n2.starts_with(prefix),
                _ => false
            }
        }
        _ => false
    }
}

fn main() {
    let index_path = std::path::Path::new("/home/anton/dev/off/esmy/foo/index");
    if !index_path.exists() {
        std::fs::create_dir(&index_path).unwrap()
    }
    for file in std::fs::read_dir(index_path).unwrap(){
        let path = file.unwrap().path();
        if file_starts_with(path.as_ref(), "foo.") {
            std::fs::remove_file(path).unwrap();
        }
    }
    seg::write_segment(
        index_path, &"foo",
        vec![
        vec![
            seg::Field{
                name: &"f",
                values:&[
                    b"fish",
                    b"anton"]
            },
    seg::Field{ name: &"c", values: &[b"dog"]}],
    vec![
    seg::Field{
    name: &"f",
    values: &[b"fish", b"anton"]},
    seg::Field{
        name: &"c",
        values: &[b"cat"]
    }]
    ]
    ).unwrap();
    println!("\nWRITING DONE\n");
    let reader = seg::SegmentReader::new(index_path, &"foo");
    for doc in reader.doc_iter("f", b"fish").unwrap() {
        for val in reader.read_values("c", doc.unwrap()).unwrap() {
            println!("{:?}", str::from_utf8(&val).unwrap());
        }
    }
}
