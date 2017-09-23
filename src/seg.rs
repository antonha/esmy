use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fst::{Map, MapBuilder};
// use memmap::{Mmap, Protection};
use rand::{self, Rng};
use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use walkdir::{WalkDir, WalkDirIterator};

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

pub trait FeatureReader {
    fn as_any(&self) -> &Any;
}

pub trait Feature: FeatureClone {
    fn as_any(&self) -> &Any;
    fn write_segment(&self, address: &SegmentAddress, docs: &Vec<Doc>) -> Result<(), Error>;
    fn reader<'a>(&self, address: SegmentAddress) -> Box<FeatureReader>;
}

pub trait FeatureClone {
    fn clone_box(&self) -> Box<Feature>;
}
impl<T> FeatureClone for T
where
    T: 'static + Feature + Clone,
{
    fn clone_box(&self) -> Box<Feature> {
        Box::new(self.clone())
    }
}

impl Clone for Box<Feature> {
    fn clone(&self) -> Box<Feature> {
        self.clone_box()
    }
}

#[derive(Clone)]
pub struct SegmentSchema {
    pub features: Vec<Box<Feature>>,
}

#[derive(Clone)]
pub struct SegmentAddress {
    path: PathBuf,
    name: String,
}

pub struct Index<'a> {
    schema_template: SegmentSchema,
    path: &'a Path,
}

impl SegmentAddress {
    fn create_file(&self, ending: &str) -> Result<File, Error> {
        if !self.path.exists() {
            fs::create_dir_all(&self.path).unwrap();
        }
        let name = format!("{}.{}", self.name, ending);
        File::create(self.path.join(name))
    }

    fn open_file(&self, ending: &str) -> Result<File, Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        File::open(file)
    }
}


impl<'a> Index<'a> {
    pub fn new(schema_template: SegmentSchema, path: &'a Path) -> Index<'a> {
        Index {
            schema_template,
            path,
        }
    }

    pub fn new_segment(&self) -> SegmentBuilder {
        SegmentBuilder::new(
            self.schema_template.clone(),
            SegmentAddress {
                path: PathBuf::from(self.path),
                name: self::random_name(),
            },
        )
    }

    pub fn open_reader(&self) -> IndexReader {
        let walker = WalkDir::new(self.path)
            .min_depth(1)
            .max_depth(1)
            .into_iter();
        let entries = walker.filter_entry(|e| {
            e.file_type().is_dir() ||
                e.file_name()
                    .to_str()
                    .map(|s| s.ends_with(".seg"))
                    .unwrap_or(false)
        });
        let segments = entries
            .map(|e| {
                let name = String::from(
                    e.unwrap()
                        .file_name()
                        .to_str()
                        .unwrap()
                        .split(".")
                        .next()
                        .unwrap(),
                );
                let address = SegmentAddress {
                    path: PathBuf::from(self.path),
                    name: name,
                };
                SegmentReader::new(self.schema_template.clone(), address)
            })
            .collect::<Vec<SegmentReader>>();
        IndexReader { segment_readers: segments }
    }
}

pub struct IndexReader {
    segment_readers: Vec<SegmentReader>,
}

impl IndexReader {
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.segment_readers
    }
}


fn random_name() -> String {
    rand::thread_rng().gen_ascii_chars().take(10).collect()
}

#[derive(Debug)]
pub enum FieldValue {
    StringField(Vec<String>),
}


#[derive(Debug)]
pub struct Field<'a> {
    pub name: &'a str,
    pub value: FieldValue,
}

pub type Doc<'a> = Vec<Field<'a>>;

pub struct SegmentBuilder<'a> {
    schema: SegmentSchema,
    address: SegmentAddress,
    docs: Vec<Doc<'a>>,
}

impl<'a> SegmentBuilder<'a> {
    pub fn new(schema: SegmentSchema, address: SegmentAddress) -> SegmentBuilder<'a> {
        SegmentBuilder {
            address,
            schema,
            docs: Vec::new(),
        }
    }

    pub fn add_doc(&mut self, doc: Doc<'a>) {
        self.docs.push(doc)
    }

    pub fn commit(&self) -> Result<(), Error> {
        for feature in &self.schema.features {
            feature.write_segment(&self.address, &self.docs);
        }
        self.address.create_file("seg")?;
        Ok(())
    }
}


pub struct SegmentReader {
    address: SegmentAddress,
    readers: Vec<Box<FeatureReader>>,
}

impl SegmentReader {
    pub fn new(schema: SegmentSchema, address: SegmentAddress) -> SegmentReader {
        SegmentReader {
            address: address.clone(),
            readers: schema
                .features
                .into_iter()
                .map(|feature| feature.reader(address.clone()))
                .collect(),
        }
    }

    pub fn string_index(&self, field_name: &str) -> Option<&StringIndexReader>{
        for reader in self.readers.iter(){
            match reader.as_any().downcast_ref::<StringIndexReader>(){
                Some(reader) => {
                    if reader.feature.field_name == field_name{
                        return Some(reader);
                    }
                }
                None => ()
            }
        }
        return None
    }
    
    pub fn string_values(&self, field_name: &str) -> Option<&StringValueReader>{
        for reader in self.readers.iter(){
            match reader.as_any().downcast_ref::<StringValueReader>(){
                Some(reader) => {
                    if reader.feature.field_name == field_name{
                        return Some(reader);
                    }
                }
                None => ()
            }
        }
        return None
    }
}


#[derive(Clone)]
pub struct StringIndex {
    field_name: String,
}

impl StringIndex{
    pub fn new<T> (field_name: T) -> StringIndex
        where T: Into<String> 
    {
        StringIndex{field_name: field_name.into()}
    }
}

impl<'a> Feature for StringIndex {


    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &Vec<Doc>) -> Result<(), Error> {
        let mut value_to_docs: BTreeMap<&String, Vec<u32>> = BTreeMap::new();
        {
            for (doc_id, doc) in docs.iter().enumerate() {
                for field in doc.iter().filter(|f| f.name == &self.field_name) {
                    match field.value {
                        FieldValue::StringField(ref values) => {
                            for value in values {
                                value_to_docs.entry(&value).or_insert(Vec::new()).push(
                                    doc_id as
                                        u32,
                                );
                            }
                        }
                    };
                }
            }
        }
        let mut offset: u64 = 0;
        let tid = address.create_file(
            &format!("{}.{}", &self.field_name, TERM_ID_LISTING),
        )?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();
        let mut iddoc = address.create_file(
            &format!("{}.{}", self.field_name, ID_DOC_LISTING),
        )?;
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

    fn reader(&self, address: SegmentAddress) -> Box<FeatureReader> {
        let path = address.path.join(format!(
            "{}.{}.{}",
            address.name,
            self.field_name,
            TERM_ID_LISTING
        ));
        Box::new({
            StringIndexReader {
                feature: self.clone(),
                address: address,
                map: Map::from_path(path).unwrap()
            }
        })
    }
}

pub struct StringIndexReader {
    feature: StringIndex,
    address: SegmentAddress,
    map: Map
}

impl FeatureReader for StringIndexReader {
    fn as_any(&self) -> &Any{
        self
    }
}

impl StringIndexReader {
    pub fn doc_iter(&self, field: &str, term: &str) -> Result<DocIter, Error> {
        let maybe_offset = self.term_offset(field, term)?;
        let mut iddoc = self.address.open_file(
            &format!("{}.{}", field, ID_DOC_LISTING),
        )?;
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

    fn term_offset(&self, field: &str, term: &str) -> Result<Option<u64>, Error> {
        return Ok(self.map.get(term));
    }
}

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


#[derive(Clone)]
pub struct StringValues {
    field_name: String,
}

impl StringValues {
    pub fn new<T> (field_name: T) -> StringValues
        where T: Into<String> {
        StringValues{field_name: field_name.into()}
    }
}

impl Feature for StringValues {
    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &Vec<Doc>) -> Result<(), Error> {
        let mut offset: u64 = 0;
        let mut di = address.create_file(
            &format!("{}.{}", self.field_name, "di"),
        )?;
        let mut dv = address.create_file(
            &format!("{}.{}", self.field_name, "dv"),
        )?;
        for doc in docs {
            for doc_field in doc.iter() {
                if doc_field.name == &self.field_name {
                    di.write_u64::<BigEndian>(offset)?;
                    let ref val = doc_field.value;
                    match val {
                        &FieldValue::StringField(ref vals) => {
                            dv.write_u64::<BigEndian>(vals.len() as u64)?;
                            offset += 8;
                            for val in vals.iter() {
                                dv.write_u64::<BigEndian>(val.len() as u64)?;
                                offset += 8;
                                dv.write((val).as_bytes())?;
                                offset += val.len() as u64;
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

    fn reader<'b>(&self, address: SegmentAddress) -> Box<FeatureReader> {
        Box::new({
            StringValueReader {
                feature: self.clone(),
                address: address,
            }
        })
    }
}

pub struct StringValueReader {
    feature: StringValues,
    address: SegmentAddress,
}

impl FeatureReader for StringValueReader {
    fn as_any(&self) -> &Any{
        self
    }
}

impl StringValueReader {
    pub fn read_values(&self, docid: u32) -> Result<Vec<Vec<u8>>, Error> {
        let mut di = self.address.open_file(
            &format!("{}.{}", self.feature.field_name, "di"),
        )?;
        di.seek(SeekFrom::Start(docid as u64 * 8))?;
        let offset = di.read_u64::<BigEndian>()?;

        let mut dv = self.address.open_file(
            &format!("{}.{}", self.feature.field_name, "dv"),
        )?;
        dv.seek(SeekFrom::Start(offset))?;

        let num_values = dv.read_u64::<BigEndian>()?;

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

    use super::read_vint;
    use super::write_vint;
    use std::io::Cursor;

    quickcheck!{
        fn read_write_correct(num1: u32, num2: u32) -> bool {
            let num =  num1 * num2;
            let mut write = Cursor::new(vec![0 as u8; 100]);
            println!("{}", num);
            write_vint(&mut write, num).unwrap();
            write.set_position(0);
            num == read_vint(&mut write).unwrap()
        }
    }
}
