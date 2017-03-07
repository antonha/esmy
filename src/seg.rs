use std::fs::File;
use std::path::Path;
use std::io::Write;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Error;
use std::io::BufWriter;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BTreeMap;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use fst::{Map, MapBuilder};

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

pub type Term = [u8];

#[derive(Debug)]
pub struct Field<'a> {
    pub name: &'a str,
    pub values: &'a [&'a [u8]],
}

pub fn write_vint(write: &mut Write, mut value: u32) -> Result<u32, Error> {
    let mut count = 1;
    while (value & !0x7F) != 0 {
        write.write_all(&[((value & 0x7F) | 0x80) as u8])?;
        value >>= 7;
        count += 1;
    }
    write.write(&[(value as u8)])?;
    return Result::Ok((count))
}

pub fn read_vint(read: &mut Read) -> Result<u32, Error> {
    let mut buf = [1];
    read.read_exact(&mut buf)?;
    let mut res: u32 = (buf[0] & 0x7F) as u32;
    let mut shift = 7;
    while (buf[0] & 0x80) != 0 {
        println!("{0}", buf[0]);
        read.read_exact(&mut buf)?;
        println!("{0}", buf[0]);
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
            let num = write_vint(&mut write, 33).unwrap();
            assert_eq!(1 as u32, num);
            write.set_position(0);
            assert_eq!(33, read_vint(&mut write).unwrap())
        }
    }
}


pub fn write_segment<'a>(path: &'a Path, name: &'a str, docs: Vec<Vec<Field<'a>>>) -> Result<(), Error> {
    let mut term_index: HashMap<&str, BTreeMap<&Term, Vec<u32>>> = HashMap::new();
    for (docid, doc) in docs.iter().enumerate() {
        for field in doc.iter() {
            let mut prop_index = term_index.entry(field.name).or_insert(BTreeMap::new());
            for term in field.values {
                let mut term_index = prop_index.entry(term).or_insert(Vec::new());
                term_index.push(docid as u32);
            }
        }
    }
    write_term_index(path, name, &term_index)?;
    write_doc_vals(path, name, &docs)?;
    Ok(())
}

fn write_term_index(path: &Path, name: &str, listings: &HashMap<&str, BTreeMap<&[u8], Vec<u32>>>) -> Result<(), Error> {
    for (field, field_listing) in listings.iter() {
        let mut offset: u64 = 0;
        let tid = create_segment_file(path, &format!("{}.{}", name, &field), TERM_ID_LISTING)?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();

        let mut iddoc =
        create_segment_file(path, &format!("{}.{}", name, &field), ID_DOC_LISTING)?;
        for (term, ids) in field_listing.iter() {
            tid_builder.insert(term, offset).unwrap();
            //iddoc.write_u32::<BigEndian>(ids.len() as u32)?;

            offset += write_vint(&mut iddoc, ids.len() as u32)? as u64;
            println!("Offset: {0}", offset);
            for id in ids.iter() {
                offset += write_vint(&mut iddoc, *id as u32)? as u64;
                println!("Offset: {0}", offset);
            }
        }
        tid_builder.finish().unwrap();
        iddoc.sync_all()?;
    }
    Ok(())
}

fn write_doc_vals<'a >(path: &'a Path, name: &'a str, docs: &'a Vec<Vec<Field<'a>>>) -> Result<(), Error> {

    let mut field_names : HashSet<&str>= HashSet::new();

    for doc in docs{
        for field in doc{
            field_names.insert(&field.name);
        }
    }

    for field in field_names{
        let mut offset: u64 = 0;
        let mut di = create_segment_file(path, &format!("{}.{}", name, &field), "di")?;
        let mut dv = create_segment_file(path, &format!("{}.{}", name, &field), "dv")?;
        for doc in docs {
            for doc_field in doc{
                if doc_field.name == field {
                    di.write_u64::<BigEndian>(offset)?;
                    let ref vals = doc_field.values;
                    dv.write_u64::<BigEndian>(vals.len() as u64)?;
                    offset += 8;
                    for val in vals.iter() {
                        dv.write_u64::<BigEndian>(val.len() as u64)?;
                        offset += 8;
                        dv.write_all(&val)?;
                        offset += val.len() as u64;
                    }

                }
            }
        }
        di.sync_all()?;
        dv.sync_all()?;
    }
    Ok(())
}

fn create_segment_file(path: &Path, name: &str, ending: &str) -> Result<File, Error> {
    let name = format!("{}.{}", name, ending);
    let file = path.join(name);
    File::create(file)
}

pub struct SegmentReader<'a> {
    name: &'a str,
    path: &'a Path,
}

#[derive(Debug)]
pub struct DocIter {
    // TODO: File pointer is open for very long,
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

    pub fn new(path: &'a Path, name: &'a str) -> SegmentReader<'a> {
        SegmentReader {
            name: name,
            path: path,
        }
    }

    pub fn doc_iter(&self, field: &str, term: &Term) -> Result<DocIter, Error> {
        let maybe_offset = self.term_offset(field, term)?;
        let mut iddoc = self._open_segment_file(&format!("{}.{}", field, ID_DOC_LISTING))?;
        match maybe_offset{
            None => Ok(DocIter{file: iddoc, left: 0}),
            Some(offset) => {
                println!("Offset: {0}", offset);
                iddoc.seek(SeekFrom::Start(offset as u64))?;
                let num = read_vint(&mut iddoc)?;
                Ok(DocIter {
                    file: iddoc,
                    left: num,
                })
            }
        }
    }

    fn term_offset(self: &SegmentReader<'a>, field: &str, term: &Term) -> Result<Option<u64>, Error> {
        let map = Map::from_path(self.path.join(format!("{}.{}.{}", self.name, field, TERM_ID_LISTING))).unwrap();
        return Ok(map.get(term))
    }

    pub fn read_values(self: &SegmentReader<'a>, field: &str, docid: u32) -> Result<Vec<Vec<u8>>, Error> {
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
