use std::fs::File;
use std::path::Path;
use std::io::Write;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Error;
use std::collections::HashSet;
use std::io::BufWriter;
use std::collections::BTreeMap;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rand::{self, Rng};
use walkdir::{WalkDir, WalkDirIterator};


use fst::{Map, MapBuilder};

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

pub struct Index<'a> {
    path: &'a Path,
}

impl<'a> Index<'a> {
    pub fn new(path: &'a Path) -> Index {
        Index { path: path }
    }

    pub fn new_segment(&self) -> SegmentBuilder {
        SegmentBuilder::new(self.path, random_name())
    }

    pub fn segment_readers(&self) -> Vec<SegmentReader> {
        let walker = WalkDir::new(self.path).min_depth(1).max_depth(1).into_iter();
        let entries = walker.filter_entry(|e| {
            e.file_type().is_dir() ||
            e.file_name()
                .to_str()
                .map(|s| s.ends_with(".seg"))
                .unwrap_or(false)
        });
        entries.map(|e| {
                let name = String::from(e.unwrap()
                                            .file_name()
                                            .to_str()
                                            .unwrap()
                                            .split(".")
                                            .next()
                                            .unwrap());
                SegmentReader::new(self.path, name)
            })
            .collect::<Vec<SegmentReader>>()
    }
}

fn random_name() -> String {
    rand::thread_rng().gen_ascii_chars().take(10).collect()
}


#[derive(Debug)]
pub enum FieldValue {
    StringField(String),
}

#[derive(Debug)]
pub struct Field<'a> {
    pub name: &'a str,
    pub values: Vec<FieldValue>,
}

pub type Doc<'a> = Vec<Field<'a>>;

pub struct SegmentBuilder<'a> {
    path: &'a Path,
    name: String,
    docs: Vec<Doc<'a>>,
}

impl<'a> SegmentBuilder<'a> {
    pub fn new(path: &'a Path, name: String) -> SegmentBuilder<'a> {
        SegmentBuilder {
            path: path,
            name: name,
            docs: Vec::new(),
        }
    }

    pub fn add_doc(&mut self, doc: Doc<'a>) {
        self.docs.push(doc)
    }

    pub fn commit(&self) -> Result<(), Error> {
        let mut field_names: HashSet<&str> = HashSet::new();
        for doc in self.docs.iter() {
            for field in doc.iter() {
                field_names.insert(&field.name);
            }
        }
        for field in field_names {
            self.write_term_index(field)?;
            self.write_doc_vals(field)?;
        }
        try!(self._create_segment_file("seg"));
        Ok(())
    }

    fn write_term_index(&self, field: &str) -> Result<(), Error> {
        let mut value_to_docs: BTreeMap<&String, Vec<u32>> = BTreeMap::new();
        {
            for (doc_id, doc) in self.docs.iter().enumerate() {
                for field_values in doc.iter().filter(|f| f.name == field) {
                    for field_value in field_values.values.iter() {
                        match field_value {
                            &FieldValue::StringField(ref value) => {
                                value_to_docs.entry(&value).or_insert(Vec::new()).push(doc_id as
                                                                                       u32)
                            }
                        };
                    }
                }
            }
        }
        let mut offset: u64 = 0;
        let tid = self._create_segment_file(&format!("{}.{}", field, TERM_ID_LISTING))?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();
        let mut iddoc = self._create_segment_file(&format!("{}.{}", field, ID_DOC_LISTING))?;
        for (term, ids) in value_to_docs.iter() {
            tid_builder.insert(term, offset).unwrap();
            offset += write_vint(&mut iddoc, ids.len() as u32)? as u64;
            for id in ids.iter() {
                offset += write_vint(&mut iddoc, *id as u32)? as u64;
            }
        }
        tid_builder.finish().unwrap();
        iddoc.sync_all()?;
        Ok(())
    }

    fn write_doc_vals(&self, field: &str) -> Result<(), Error> {
        let mut offset: u64 = 0;
        let mut di = self._create_segment_file(&format!("{}.{}", field, "di"))?;
        let mut dv = self._create_segment_file(&format!("{}.{}", field, "dv"))?;
        for doc in self.docs.iter() {
            for doc_field in doc.iter() {
                if doc_field.name == field {
                    di.write_u64::<BigEndian>(offset)?;
                    let ref vals = doc_field.values;
                    dv.write_u64::<BigEndian>(vals.len() as u64)?;
                    offset += 8;
                    for val in vals.iter() {
                        match val {
                            &FieldValue::StringField(ref value) => {
                                dv.write_u64::<BigEndian>(value.len() as u64)?;
                                offset += 8;
                                dv.write((value).as_bytes())?;
                                offset += value.len() as u64;
                            }
                        }
                    }
                }
            }
        }
        di.sync_all()?;
        dv.sync_all()?;
        Ok(())
    }

    fn _create_segment_file(&self, ending: &str) -> Result<File, Error> {
        let name = format!("{}.{}", self.name, ending);
        File::create(self.path.join(name))
    }
}


pub struct SegmentReader<'a> {
    name: String,
    path: &'a Path,
}

#[derive(Debug)]
pub struct DocIter {
    file: File,
    left: u32,
}

impl Iterator for DocIter {
    type Item = Result<u32, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left != 0 {
            self.left -= 1;
            Some(read_vint(&mut self.file))
        } else {
            None
        }
    }
}

impl<'a> SegmentReader<'a> {
    pub fn new(path: &'a Path, name: String) -> SegmentReader<'a> {
        SegmentReader {
            name: name,
            path: path,
        }
    }

    pub fn doc_iter(&self, field: &str, term: &[u8]) -> Result<DocIter, Error> {
        let maybe_offset = self.term_offset(field, term)?;
        let mut iddoc = self._open_segment_file(&format!("{}.{}", field, ID_DOC_LISTING))?;
        match maybe_offset {
            None => {
                Ok(DocIter {
                       file: iddoc,
                       left: 0,
                   })
            }
            Some(offset) => {
                iddoc.seek(SeekFrom::Start(offset as u64))?;
                let num = read_vint(&mut iddoc)?;
                Ok(DocIter {
                       file: iddoc,
                       left: num,
                   })
            }
        }
    }

    fn term_offset(self: &SegmentReader<'a>,
                   field: &str,
                   term: &[u8])
                   -> Result<Option<u64>, Error> {
        let map =
            Map::from_path(self.path.join(format!("{}.{}.{}", self.name, field, TERM_ID_LISTING)))
                .unwrap();
        return Ok(map.get(term));
    }

    pub fn read_values(self: &SegmentReader<'a>,
                       field: &str,
                       docid: u32)
                       -> Result<Vec<Vec<u8>>, Error> {
        let mut di = self._open_segment_file(&format!("{}.{}", field, "di"))?;
        di.seek(SeekFrom::Start(docid as u64 * 8))?;
        let offset = di.read_u64::<BigEndian>()?;
        let mut dv = self._open_segment_file(&format!("{}.{}", field, "dv"))?;
        dv.seek(SeekFrom::Start(offset))?;

        let num_values = dv.read_u64::<BigEndian>()?;

        //TODO: Vector alloc.. meh
        let mut ret = Vec::with_capacity(num_values as usize);
        for _ in 0..num_values {
            let val_length = dv.read_u64::<BigEndian>()?;
            let mut value = Vec::with_capacity(val_length as usize);
            for _ in 0..val_length {
                let mut buf = [0];
                dv.read_exact(&mut buf)?;
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

pub fn write_vint(write: &mut Write, mut value: u32) -> Result<u32, Error> {
    let mut count = 1;
    while (value & !0x7F) != 0 {
        write.write_all(&[((value & 0x7F) | 0x80) as u8])?;
        value >>= 7;
        count += 1;
    }
    write.write(&[(value as u8)])?;
    return Result::Ok((count));
}

pub fn read_vint(read: &mut Read) -> Result<u32, Error> {
    let mut buf = [1];
    read.read_exact(&mut buf)?;
    let mut res: u32 = (buf[0] & 0x7F) as u32;
    let mut shift = 7;
    while (buf[0] & 0x80) != 0 {
        read.read_exact(&mut buf)?;
        res |= ((buf[0] & 0x7F) as u32) << shift;
        shift += 7
    }
    return Ok(res as u32);
}


#[cfg(test)]
mod tests {
    use super::write_vint;
    use super::read_vint;
    use std::io::Cursor;

    #[test]
    fn vint_tests() {
        {
            let mut write = Cursor::new(vec![0 as u8; 100]);
            let num = write_vint(&mut write, 3000).unwrap();
            assert_eq!(1 as u32, num);
            write.set_position(0);
            assert_eq!(3000, read_vint(&mut write).unwrap())
        }
    }
}
