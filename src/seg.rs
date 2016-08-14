use std::fs::File;
use std::path::Path;
use std::io::Write;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Error;
use std::collections::HashMap;
use std::collections::BTreeMap;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

pub type Term = [u8];

pub type FieldToStore = Vec<Vec<u8>>;

pub enum FieldFlag {
    DocIndex,
     Store
}

pub type Collector = Fn(u32) -> Result<(), Error>;

pub fn write_segment(path: &Path, name: &str, docs: Vec<HashMap<String, FieldToStore>>) -> Result<(), Error> {
    let mut term_index: HashMap<& str, BTreeMap<&Term, Vec<u32>>> = HashMap::new();
    for (docid, doc) in docs.iter().enumerate() {
        for (field_name, field_values) in doc.iter() {
            let mut prop_index = term_index.entry(field_name).or_insert(BTreeMap::new());
            for term in field_values {
                let mut term_index = prop_index.entry(term).or_insert(Vec::new());
                term_index.push(docid as u32);
            }
        }
    }
    try!(write_term_index(path, name, &term_index));
    write_doc_vals(path, name, &docs)
}

pub fn write_doc_vals(path: &Path, name: &str, docs: &Vec<HashMap<String, FieldToStore>>) -> Result<(), Error>{
    let mut di = try!(_create_segment_file(path, name, "di"));
    let mut dv = try!(_create_segment_file(path, name, "dv"));
    let mut offset: u64 = 0;
    for doc in docs{
        try!(di.write_u64::<BigEndian>(offset));

        let ref vals = doc["f"];
        try!(dv.write_u64::<BigEndian>(vals.len() as u64));
        offset += 8;
        for val in vals{
            try!(dv.write_u64::<BigEndian>(val.len() as u64));
            offset += 8;
            try!(dv.write_all(&val));
            offset += val.len() as u64;
        }
    }
    Ok(())
}

pub fn write_term_index(path: &Path,
                        name: &str,
                        listings: &HashMap<&str, BTreeMap<&[u8], Vec<u32>>>)
                        -> Result<(), Error> {
    let mut tid = try!(_create_segment_file(path, name, TERM_ID_LISTING));
    let mut iddoc = try!(_create_segment_file(path, name, ID_DOC_LISTING));
    let mut offset: u64 = 0;
    for (term, ids) in listings["f"].iter() {
        try!(tid.write_u32::<BigEndian>(term.len() as u32));
        try!(tid.write_all(&term));
        try!(tid.write_u64::<BigEndian>(offset));

        try!(iddoc.write_u32::<BigEndian>(ids.len() as u32));
        offset += 4;
        for id in ids.iter() {
            try!(iddoc.write_u32::<BigEndian>(*id));
            offset += 4;
        }
    }
    Ok(())
}

fn _create_segment_file(path: &Path, name: &str, ending: &str) -> Result<File, Error> {
    let name = format!("{}.{}", name, ending);
    let file = path.join(name);
    File::create(file)
}

pub enum Query<'a> {
    Term(&'a Term),
}

pub struct SegmentReader<'a> {
    name: &'a str,
    path: &'a Path,
}


#[derive(Debug)]
pub struct DocIter {
    //TODO: File pointer is open for very long,
    file: File,
    left: u32
}



impl Iterator for DocIter{

    type Item = Result<u32,Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left != 0 {
            self.left -= 1;
            Some(self.file.read_u32::<BigEndian>())
        } else {
            None
        }
    }
}

impl<'a> SegmentReader<'a> {

    pub fn new(path: &'a Path, name: &'a str) -> SegmentReader<'a> {
        SegmentReader {
            name: name,
            path: path,
        }
    }

    pub fn doc_iter(&self, term: & Term) -> Result<DocIter, Error>{
        let offset = try!(self.term_offset(term));
        let mut iddoc = try!(self._open_segment_file(ID_DOC_LISTING));
        try!(iddoc.seek(SeekFrom::Start(offset as u64)));
        let num = try!(iddoc.read_u32::<BigEndian>());
        Ok(DocIter{file: iddoc, left: num})
    }

    fn term_offset(self: &SegmentReader<'a>, term: &Term) -> Result<u64, Error> {
        let wanted_length = term.len();
        let mut terms_list: File = try!(self._open_segment_file(TERM_ID_LISTING));
        loop {
            let term_length = try!(terms_list.read_u32::<BigEndian>()) as usize;
            if term_length == wanted_length {
                let mut buf = [0u8; 3];
                try!(terms_list.read_exact(&mut buf));
                if &buf == term {
                    return terms_list.read_u64::<BigEndian>();
                } else {
                    try!(terms_list.seek(SeekFrom::Current(8i64)));
                }
            } else {
                try!(terms_list.seek(SeekFrom::Current((term_length + 8) as i64)));
            }
        }
    }

    pub fn read_values(self: &SegmentReader<'a>, docid: u32) -> Result<Vec<Vec<u8>>, Error>{
        let mut di = try!(self._open_segment_file("di"));
        try!(di.seek(SeekFrom::Start(docid as u64 * 8)));
        let offset = try!(di.read_u64::<BigEndian>());
        let mut dv = try!(self._open_segment_file("dv"));
        try!(dv.seek(SeekFrom::Start(offset)));

        let num_values = try!(dv.read_u64::<BigEndian>());
        let mut ret = Vec::with_capacity(num_values as usize);
        for _ in 0..num_values {
            let val_length = try!(dv.read_u64::<BigEndian>());
            let mut value = Vec::with_capacity(val_length as usize);
            for _ in 0..val_length{
                let mut buf = [0];
                try!(dv.read_exact(& mut buf));
                value.push(buf[0])
            }
            ret.push(value)
        }
        Ok(ret)
    }

    fn _open_segment_file(self: &SegmentReader<'a>, ending: &str) -> Result<File, Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        File::open(file)
    }
}
