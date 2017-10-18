
use afsort;
use analyzis::Analyzer;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fst::{Map, MapBuilder, Streamer};
use fst::map::OpBuilder;
// use memmap::{Mmap, Protection};
use rand::{self, Rng};
use std::any::Any;
use std::borrow::Cow;
use std::fs::{self, File};
use std::io::{self, BufWriter, BufReader, Error, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use walkdir::{WalkDir, WalkDirIterator};

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

pub trait FeatureReader {
    fn as_any(&self) -> &Any;
}

pub trait Feature: FeatureClone + Sync + Send {
    fn as_any(&self) -> &Any;
    fn write_segment(&self, address: &SegmentAddress, docs: &Vec<Doc>) -> Result<(), Error>;
    fn reader<'a>(&self, address: SegmentAddress) -> Box<FeatureReader>;
    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error>;
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

#[derive(Clone, Debug)]
pub struct SegmentAddress {
    path: PathBuf,
    name: String,
}

#[allow(dead_code)]
pub struct SegmentInfo {
    address: SegmentAddress,
    schema: SegmentSchema,
    doc_count: u64,
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

    fn remove_file(&self, ending: &str) -> Result<(), Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        fs::remove_file(file)
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

    pub fn list_segments(&self) -> Vec<SegmentAddress> {
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
        entries
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
                SegmentAddress {
                    path: PathBuf::from(self.path),
                    name: name,
                }
            })
            .collect::<Vec<SegmentAddress>>()
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

    pub fn merge(&self, addresses: &[SegmentAddress]) -> Result<(), Error> {
        let mut infos: Vec<SegmentInfo> = Vec::with_capacity(addresses.len());
        for address in addresses {
            let mut seg_file = address.open_file("seg")?;
            let doc_count = read_vint(&mut seg_file)?;
            infos.push(SegmentInfo {
                address: address.clone(),
                schema: self.schema_template.clone(),
                doc_count,
            });
        }

        let new_segment_address = SegmentAddress {
            path: PathBuf::from(self.path),
            name: random_name(),
        };
        for feature in self.schema_template.features.iter() {
            feature.merge_segments(&infos, &new_segment_address)?;
        }

        let doc_count: u64 = infos.iter().map(|i| i.doc_count).sum();
        let mut file = new_segment_address.create_file("seg")?;
        write_vint(&mut file, doc_count)?;
        for address in addresses {
            address.remove_file("seg")?;
        }
        Ok(())
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

    pub fn name(&self) -> &str {
        &self.address.name
    }

    pub fn add_doc(&mut self, doc: Doc<'a>) {
        self.docs.push(doc)
    }

    pub fn commit(&self) -> Result<(), Error> {
        if (self.docs.is_empty()) {
            return Ok(());
        }
        for feature in &self.schema.features {
            feature.write_segment(&self.address, &self.docs)?;
        }
        let mut file = self.address.create_file("seg")?;
        write_vint(&mut file, self.docs.len() as u64)?;
        Ok(())
    }
}


#[allow(dead_code)]
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

    pub fn string_index(&self, field_name: &str) -> Option<&StringIndexReader> {
        for reader in self.readers.iter() {
            match reader.as_any().downcast_ref::<StringIndexReader>() {
                Some(reader) => {
                    if reader.feature.field_name == field_name {
                        return Some(reader);
                    }
                }
                None => (),
            }
        }
        return None;
    }

    pub fn string_values(&self, field_name: &str) -> Option<&StringValueReader> {
        for reader in self.readers.iter() {
            match reader.as_any().downcast_ref::<StringValueReader>() {
                Some(reader) => {
                    if reader.feature.field_name == field_name {
                        return Some(reader);
                    }
                }
                None => (),
            }
        }
        return None;
    }
}


#[derive(Clone)]
pub struct StringIndex {
    field_name: String,
    analyzer: Box<Analyzer>,
}

impl StringIndex {
    pub fn new<T>(field_name: T, analyzer: Box<Analyzer>) -> StringIndex
    where
        T: Into<String>,
    {
        StringIndex {
            field_name: field_name.into(),
            analyzer,
        }
    }
}

impl StringIndex {
    fn docs_to_term_map<'a>(&self, docs: &'a Vec<Doc>) -> Vec<(Cow<'a, str>, u64)> {
        let mut terms: Vec<(Cow<'a, str>, u64)> = Vec::new();
        {
            for (doc_id, doc) in docs.iter().enumerate() {
                for field in doc.iter().filter(|f| f.name == &self.field_name) {
                    match field.value {
                        FieldValue::StringField(ref values) => {
                            for value in values {
                                for token in self.analyzer.analyze(&value) {
                                    terms.push((token, doc_id as u64))
                                }
                            }
                        }
                    };
                }
            }
        }
        terms
    }

    fn write_term_map(
        &self,
        address: &SegmentAddress,
        mut term_map: Vec<(Cow<str>, u64)>,
    ) -> Result<(), Error> {
        if term_map.is_empty(){
            return Ok(());
        }


        afsort::sort_unstable_by(&mut term_map, |t| t.0.as_bytes());

        let mut offset: u64 = 0;
        let tid = address.create_file(
            &format!("{}.{}", &self.field_name, TERM_ID_LISTING),
        )?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();
        let mut iddoc = BufWriter::new(address.create_file(
            &format!("{}.{}", self.field_name, ID_DOC_LISTING),
        )?);
        let mut ids = Vec::new();
        let mut last_term = &term_map[0].0;
        for &(ref term, id) in term_map.iter() {
            if term != last_term {
                tid_builder.insert(&last_term.as_bytes(), offset).unwrap();
                offset += write_vint(&mut iddoc, ids.len() as u64)? as u64;
                for id in ids.iter() {
                    offset += write_vint(&mut iddoc, *id)? as u64;
                }
                ids.clear();
            } else {
                ids.push(id)
            }
            last_term = term;
        }
        tid_builder.insert(&last_term.as_bytes(), offset).unwrap();
        offset += write_vint(&mut iddoc, ids.len() as u64)? as u64;
        for id in ids.iter() {
            offset += write_vint(&mut iddoc, *id)? as u64;
        }
        tid_builder.finish().unwrap();
        Ok(())
    }
}

impl<'a> Feature for StringIndex {
    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &Vec<Doc>) -> Result<(), Error> {
        let term_map = self.docs_to_term_map(docs);
        self.write_term_map(address, term_map)
    }

    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error> {
        let mut maps = Vec::with_capacity(old_segments.len());
        let segments_to_merge = old_segments.iter().filter(|s|{
            let path = s.address.path.join(format!(
                "{}.{}.{}",
                s.address.name,
                self.field_name,
                TERM_ID_LISTING
            ));
            path.exists()
        }).collect::<Vec<&SegmentInfo>>();
        for segment in segments_to_merge.iter() {
            let path = segment.address.path.join(format!(
                "{}.{}.{}",
                segment.address.name,
                self.field_name,
                TERM_ID_LISTING
            ));
            maps.push(Map::from_path(path).unwrap());
        }

        let mut op_builder = OpBuilder::new();
        for map in maps.iter() {
            op_builder.push(map.stream());
        }

        let mut source_id_doc_files = Vec::with_capacity(old_segments.len());
        let ending = format!("{}.{}", self.field_name, ID_DOC_LISTING);

        for old_segment in segments_to_merge {
            source_id_doc_files.push(BufReader::new(old_segment.address.open_file(&ending)?));
        }

        let tid = new_segment.create_file(&format!(
            "{}.{}",
            &self.field_name,
            TERM_ID_LISTING
        ))?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();
        let mut iddoc = BufWriter::new(new_segment.create_file(&format!(
            "{}.{}",
            self.field_name,
            ID_DOC_LISTING
        ))?);

        let mut new_offsets = Vec::with_capacity(old_segments.len());
        {
            let mut new_offset = 0;
            for segment in old_segments.iter() {
                new_offsets.push(new_offset);
                new_offset += segment.doc_count;
            }
        }

        let mut union = op_builder.union();
        let mut offset = 0u64;
        while let Some((term, term_offsets)) = union.next() {
            tid_builder.insert(term, offset).unwrap();
            let mut term_doc_counts: Vec<u64> = vec![0; old_segments.len()];
            for term_offset in term_offsets {
                let source_id_doc_file = &mut source_id_doc_files[term_offset.index];
                source_id_doc_file.seek(
                    SeekFrom::Start(term_offset.value as u64),
                )?;
                term_doc_counts[term_offset.index] = read_vint(source_id_doc_file)? as u64;
            }
            let term_doc_count: u64 = term_doc_counts.iter().sum();
            offset += write_vint(&mut iddoc, term_doc_count)? as u64;

            for term_offset in term_offsets.iter() {
                let source_id_doc_file = &mut source_id_doc_files[term_offset.index];
                for _ in 0..term_doc_counts[term_offset.index] {
                    let doc_id = read_vint(source_id_doc_file)?;
                    offset += write_vint(&mut iddoc, new_offsets[term_offset.index] + doc_id)? as
                        u64;
                }
            }
        }
        tid_builder.finish().unwrap();
        //iddoc.sync_all()?;
        Ok(())
    }

    fn reader(&self, address: SegmentAddress) -> Box<FeatureReader> {
        let path = address.path.join(format!(
            "{}.{}.{}",
            address.name,
            self.field_name,
            TERM_ID_LISTING
        ));
        if path.exists() {
            Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address,
                    map: Some(Map::from_path(path).unwrap()),
                }
            })
        }
        else{
            Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address,
                    map: None,
                }
            })
        }
    }
}

pub struct StringIndexReader {
    feature: StringIndex,
    address: SegmentAddress,
    map: Option<Map>,
}

impl FeatureReader for StringIndexReader {
    fn as_any(&self) -> &Any {
        self
    }
}

impl StringIndexReader {
    pub fn doc_iter(&self, field: &str, term: &str) -> Result<Option<DocIter>, Error> {
        let maybe_offset = self.term_offset(term)?;
        match maybe_offset {
            None => {
                Ok(None)
            }
            Some(offset) => {
                let mut iddoc = BufReader::new(self.address.open_file(
                    &format!("{}.{}", field, ID_DOC_LISTING),
                    )?);
                iddoc.seek(SeekFrom::Start(offset as u64))?;
                let num = read_vint(&mut iddoc)?;
                Ok(Some(DocIter {
                    file: iddoc,
                    left: num,
                }))
            }
        }
    }

    fn term_offset(&self, term: &str) -> Result<Option<u64>, Error> {
        Ok(match self.map {
            Some(ref m) => m.get(term),
            None => None
        })
    }
}

pub struct DocIter {
    file: BufReader<File>,
    left: u64,
}

impl Iterator for DocIter {
    type Item = Result<u64, Error>;
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
    pub fn new<T>(field_name: T) -> StringValues
    where
        T: Into<String>,
    {
        StringValues { field_name: field_name.into() }
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
                            offset += write_vint(&mut dv, vals.len() as u64)? as u64;
                            for val in vals.iter() {
                                offset += write_vint(&mut dv, val.len() as u64)? as u64;
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

    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error> {

        let mut target_val_offset_file = new_segment.create_file(
            &format!("{}.{}", &self.field_name, "di"),
        )?;
        let mut target_val_file = new_segment.create_file(
            &format!("{}.{}", &self.field_name, "dv"),
        )?;
        let mut offset = 0u64;
        for segment in old_segments.iter() {
            let mut source_val_file = segment.address.open_file(
                &format!("{}.{}", &self.field_name, "dv"),
            )?;
            let mut source_val_offset_file =
                segment.address.open_file(
                    &format!("{}.{}", &self.field_name, "di"),
                )?;
            loop {
                match source_val_offset_file.read_u64::<BigEndian>() {
                    Ok(source_offset) => {
                        target_val_offset_file.write_u64::<BigEndian>(offset)?;
                        source_val_file.seek(SeekFrom::Start(source_offset))?;
                        let val_count = read_vint(&mut source_val_file)?;
                        offset += write_vint(&mut target_val_file, val_count)? as u64;
                        for _ in 0..val_count {
                            let val_len = read_vint(&mut source_val_file)?;
                            offset += write_vint(&mut target_val_file, val_len)? as u64;
                            for _ in 0..val_len {
                                let mut buf = [0];
                                source_val_file.read_exact(&mut buf)?;
                                target_val_file.write(&buf)?;
                                offset += 1;
                            }
                        }
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            return Err(error);
                        }
                        break;
                    }
                }
            }
        }
        target_val_file.sync_all()?;
        target_val_offset_file.sync_all()?;
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
    fn as_any(&self) -> &Any {
        self
    }
}

impl StringValueReader {
    pub fn read_values(&self, docid: u64) -> Result<Vec<Vec<u8>>, Error> {
        let mut di = self.address.open_file(
            &format!("{}.{}", self.feature.field_name, "di"),
        )?;
        di.seek(SeekFrom::Start(docid * 8))?;
        let offset = di.read_u64::<BigEndian>()?;

        let mut dv = self.address.open_file(
            &format!("{}.{}", self.feature.field_name, "dv"),
        )?;
        dv.seek(SeekFrom::Start(offset))?;

        let num_values = read_vint(&mut dv)?;

        let mut ret = Vec::with_capacity(num_values as usize);
        for _ in 0..num_values {
            let val_length = read_vint(&mut dv)?;
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

pub fn write_vint(write: &mut Write, mut value: u64) -> Result<u32, Error> {
    let mut count = 1;
    while (value & !0x7F) != 0 {
        write.write_all(&[((value & 0x7F) | 0x80) as u8])?;
        value >>= 7;
        count += 1;
    }
    write.write(&[(value as u8)])?;
    return Result::Ok((count));
}

pub fn read_vint(read: &mut Read) -> Result<u64, Error> {
    let mut buf = [1];
    read.read_exact(&mut buf)?;
    let mut res: u64 = (buf[0] & 0x7F) as u64;
    let mut shift = 7;
    while (buf[0] & 0x80) != 0 {
        read.read_exact(&mut buf)?;
        res |= ((buf[0] & 0x7F) as u64) << shift;
        shift += 7
    }
    return Ok(res as u64);
}



#[cfg(test)]
mod tests {

    use super::read_vint;
    use super::write_vint;
    use std::io::Cursor;

    quickcheck!{
        fn read_write_correct(num1: u64, num2: u64) -> bool {
            let num =  num1 * num2;
            let mut write = Cursor::new(vec![0 as u8; 100]);
            write_vint(&mut write, num).unwrap();
            write.set_position(0);
            num == read_vint(&mut write).unwrap()
        }
    }
}
