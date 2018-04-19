use afsort;
use analyzis::Analyzer;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fst::map::OpBuilder;
use fst::{Map, MapBuilder, Streamer};
use rand::{self, Rng};
use rmps::{Deserializer, Serializer};
use serde::{self, Deserialize, Serialize};
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use walkdir::{WalkDir, WalkDirIterator};

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

pub trait FeatureReader {
    fn as_any(&self) -> &Any;
}

pub trait Feature: FeatureClone + Sync + Send {
    fn as_any(&self) -> &Any;
    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error>;
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

pub struct Index {
    schema_template: SegmentSchema,
    path: PathBuf,
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

impl Index {
    pub fn new(schema_template: SegmentSchema, path: PathBuf) -> Index {
        Index {
            schema_template,
            path,
        }
    }

    pub fn new_segment(&self) -> SegmentBuilder {
        SegmentBuilder::new(self.schema_template.clone(), self.new_address())
    }

    pub fn new_address(&self) -> SegmentAddress {
        SegmentAddress {
            path: PathBuf::from(&self.path),
            name: self::random_name(),
        }
    }

    pub fn schema_template(&self) -> &SegmentSchema {
        &self.schema_template
    }

    pub fn list_segments(&self) -> Vec<SegmentAddress> {
        let walker = WalkDir::new(&self.path)
            .min_depth(1)
            .max_depth(1)
            .into_iter();
        let entries = walker.filter_entry(|e| {
            e.file_type().is_dir()
                || e.file_name()
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
                    path: PathBuf::from(&self.path),
                    name: name,
                }
            })
            .collect::<Vec<SegmentAddress>>()
    }

    pub fn open_reader(&self) -> IndexReader {
        let walker = WalkDir::new(&self.path)
            .min_depth(1)
            .max_depth(1)
            .into_iter();
        let entries = walker.filter_entry(|e| {
            e.file_type().is_dir()
                || e.file_name()
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
                    path: PathBuf::from(&self.path),
                    name: name,
                };
                SegmentReader::new(self.schema_template.clone(), address)
            })
            .collect::<Vec<SegmentReader>>();
        IndexReader {
            segment_readers: segments,
        }
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
            path: PathBuf::from(&self.path),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldValue {
    String(String),
}

pub type Doc = HashMap<String, FieldValue>;

pub struct SegmentBuilder {
    schema: SegmentSchema,
    address: SegmentAddress,
    docs: Vec<Doc>,
}

impl SegmentBuilder {
    pub fn new(schema: SegmentSchema, address: SegmentAddress) -> SegmentBuilder {
        SegmentBuilder {
            address,
            schema,
            docs: Vec::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.address.name
    }

    pub fn add_doc(&mut self, doc: Doc) {
        self.docs.push(doc)
    }

    pub fn commit(&self) -> Result<(), Error> {
        write_seg(&self.schema, &self.address, &self.docs)
    }
}

pub fn write_seg(
    schema: &SegmentSchema,
    address: &SegmentAddress,
    docs: &[Doc],
) -> Result<(), Error> {
    if docs.is_empty() {
        return Ok(());
    }
    for feature in &schema.features {
        feature.write_segment(address, docs)?;
    }
    let mut file = address.create_file("seg")?;
    write_vint(&mut file, docs.len() as u64)?;
    Ok(())
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

    pub fn full_doc(&self) -> Option<&FullDocReader> {
        for reader in self.readers.iter() {
            match reader.as_any().downcast_ref::<FullDocReader>() {
                Some(reader) => {
                    return Some(reader);
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
    fn docs_to_term_map<'a>(&self, docs: &'a [Doc]) -> Vec<(Cow<'a, str>, u64)> {
        let mut terms: Vec<(Cow<'a, str>, u64)> = Vec::new();
        {
            for (doc_id, doc) in docs.iter().enumerate() {
                for (_name, val) in doc.iter().filter(|e| e.0 == &self.field_name) {
                    match *val {
                        FieldValue::String(ref value) => for token in self.analyzer.analyze(&value)
                        {
                            terms.push((token, doc_id as u64))
                        },
                    };
                }
            }
        }
        afsort::sort_unstable_by(&mut terms, |t| t.0.as_bytes());
        terms
    }

    fn write_term_map(
        &self,
        address: &SegmentAddress,
        terms: Vec<(Cow<str>, u64)>,
    ) -> Result<(), Error> {
        if terms.is_empty() {
            return Ok(());
        }

        let mut offset: u64 = 0;
        let tid = address.create_file(&format!("{}.{}", &self.field_name, TERM_ID_LISTING))?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();
        let mut iddoc = BufWriter::new(address.create_file(&format!("{}.{}", self.field_name, ID_DOC_LISTING))?);
        let mut ids = Vec::new();
        let mut last_term = &terms[0].0;
        for &(ref term, id) in terms.iter() {
            if term != last_term {
                tid_builder.insert(&last_term.as_bytes(), offset).unwrap();
                offset += write_vint(&mut iddoc, ids.len() as u64)? as u64;
                for id in ids.iter() {
                    offset += write_vint(&mut iddoc, *id)? as u64;
                }
                ids.clear();
            }
            ids.push(id);
            last_term = term;
        }
        tid_builder.insert(&last_term.as_bytes(), offset).unwrap();
        offset += write_vint(&mut iddoc, ids.len() as u64)? as u64;
        for id in ids.iter() {
            offset += write_vint(&mut iddoc, *id)? as u64;
        }
        tid_builder.finish().unwrap();
        iddoc.flush().unwrap();
        Ok(())
    }
}

impl<'a> Feature for StringIndex {
    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error> {
        let term_map = self.docs_to_term_map(docs);
        self.write_term_map(address, term_map)
    }

    fn reader(&self, address: SegmentAddress) -> Box<FeatureReader> {
        let path = address.path.join(format!(
            "{}.{}.{}",
            address.name, self.field_name, TERM_ID_LISTING
        ));
        if path.exists() {
            Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address,
                    map: Some(unsafe { Map::from_path(path).unwrap() }),
                }
            })
        } else {
            Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address,
                    map: None,
                }
            })
        }
    }

    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error> {
        let mut maps = Vec::with_capacity(old_segments.len());
        let segments_to_merge = old_segments
            .iter()
            .filter(|s| {
                let path = s.address.path.join(format!(
                    "{}.{}.{}",
                    s.address.name, self.field_name, TERM_ID_LISTING
                ));
                path.exists()
            })
            .collect::<Vec<&SegmentInfo>>();
        {
            for segment in segments_to_merge.iter() {
                let path = segment.address.path.join(format!(
                    "{}.{}.{}",
                    segment.address.name, self.field_name, TERM_ID_LISTING
                ));
                maps.push(unsafe { Map::from_path(path).unwrap() });
            }

            let mut op_builder = OpBuilder::new();
            for map in maps.iter() {
                op_builder.push(map.stream());
            }

            let mut source_id_doc_files = Vec::with_capacity(old_segments.len());
            let ending = format!("{}.{}", self.field_name, ID_DOC_LISTING);

            for old_segment in &segments_to_merge {
                source_id_doc_files.push(BufReader::new(old_segment.address.open_file(&ending)?));
            }

            let tid =
                new_segment.create_file(&format!("{}.{}", &self.field_name, TERM_ID_LISTING))?;
            //TODO: Not unwrap
            let mut tid_builder = MapBuilder::new(BufWriter::new(tid)).unwrap();
            let fp = new_segment.create_file(&format!("{}.{}", self.field_name, ID_DOC_LISTING))?;
            let mut iddoc = BufWriter::new(fp);

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
                    source_id_doc_file.seek(SeekFrom::Start(term_offset.value as u64))?;
                    term_doc_counts[term_offset.index] = read_vint(source_id_doc_file)? as u64;
                }
                let term_doc_count: u64 = term_doc_counts.iter().sum();
                offset += write_vint(&mut iddoc, term_doc_count)? as u64;

                for term_offset in term_offsets.iter() {
                    let source_id_doc_file = &mut source_id_doc_files[term_offset.index];
                    for _ in 0..term_doc_counts[term_offset.index] {
                        let doc_id = read_vint(source_id_doc_file)?;
                        offset +=
                            write_vint(&mut iddoc, new_offsets[term_offset.index] + doc_id)? as u64;
                    }
                }
            }
            tid_builder.finish().unwrap();
        }
        for &s in &segments_to_merge {
            s.address
                .remove_file(&format!("{}.{}", &self.field_name, TERM_ID_LISTING))?;
            s.address
                .remove_file(&format!("{}.{}", &self.field_name, ID_DOC_LISTING))?;
        }
        Ok(())
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
            None => Ok(None),
            Some(offset) => {
                let mut iddoc = BufReader::new(self.address
                    .open_file(&format!("{}.{}", field, ID_DOC_LISTING))?);
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
            None => None,
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
        StringValues {
            field_name: field_name.into(),
        }
    }
}

impl Feature for StringValues {
    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error> {
        let mut offset: u64 = 0;
        let mut di = address.create_file(&format!("{}.{}", self.field_name, "di"))?;
        let mut dv = address.create_file(&format!("{}.{}", self.field_name, "dv"))?;
        for doc in docs {
            for (name, val) in doc.iter() {
                if name == &self.field_name {
                    di.write_u64::<BigEndian>(offset)?;
                    match *val {
                        FieldValue::String(ref value) => {
                            offset += write_vint(&mut dv, value.len() as u64)? as u64;
                            dv.write((value).as_bytes())?;
                            offset += value.len() as u64;
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

    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error> {
        let mut target_val_offset_file =
            new_segment.create_file(&format!("{}.{}", &self.field_name, "di"))?;
        let mut target_val_file =
            new_segment.create_file(&format!("{}.{}", &self.field_name, "dv"))?;
        let mut offset = 0u64;
        for segment in old_segments.iter() {
            let mut source_val_file = segment
                .address
                .open_file(&format!("{}.{}", &self.field_name, "dv"))?;
            let mut source_val_offset_file = segment
                .address
                .open_file(&format!("{}.{}", &self.field_name, "di"))?;
            loop {
                match source_val_offset_file.read_u64::<BigEndian>() {
                    Ok(source_offset) => {
                        target_val_offset_file.write_u64::<BigEndian>(offset)?;
                        source_val_file.seek(SeekFrom::Start(source_offset))?;
                        let val_len = read_vint(&mut source_val_file)?;
                        offset += write_vint(&mut target_val_file, val_len)? as u64;
                        for _ in 0..val_len {
                            let mut buf = [0];
                            source_val_file.read_exact(&mut buf)?;
                            target_val_file.write(&buf)?;
                            offset += 1;
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
    pub fn read_value(&self, docid: u64) -> Result<Vec<u8>, Error> {
        let mut di = self.address
            .open_file(&format!("{}.{}", self.feature.field_name, "di"))?;
        di.seek(SeekFrom::Start(docid * 8))?;
        let offset = di.read_u64::<BigEndian>()?;

        let mut dv = self.address
            .open_file(&format!("{}.{}", self.feature.field_name, "dv"))?;
        dv.seek(SeekFrom::Start(offset))?;

        let val_length = read_vint(&mut dv)?;
        let mut value = Vec::with_capacity(val_length as usize);
        for _ in 0..val_length {
            let mut buf = [0];
            dv.read_exact(&mut buf)?;
            value.push(buf[0])
        }
        Ok(value)
    }
}

#[derive(Clone)]
pub struct FullDoc {}

impl FullDoc {
    pub fn new() -> FullDoc {
        FullDoc {}
    }
}

impl Feature for FullDoc {
    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error> {
        let mut offset: u64;
        let mut doc_offsets = address.create_file("fdo")?;
        let mut docs_packed = address.create_file("fdv")?;
        for doc in docs {
            offset = docs_packed.seek(SeekFrom::Current(0))?;
            doc_offsets.write_u64::<BigEndian>(offset)?;
            doc.serialize(&mut Serializer::new(&docs_packed)).unwrap();
        }
        doc_offsets.sync_all()?;
        docs_packed.sync_all()?;
        Ok(())
    }

    fn reader<'b>(&self, address: SegmentAddress) -> Box<FeatureReader> {
        Box::new({ FullDocReader { address } })
    }

    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error> {
        let mut target_val_offset_file = new_segment.create_file("fdo")?;
        let mut target_val_file = new_segment.create_file("fdv")?;
        let mut base_offset = 0u64;
        for segment in old_segments.iter() {
            let mut source_val_offset_file = segment.address.open_file("fdo")?;
            let mut source_val_file = segment.address.open_file("fdv")?;
            loop {
                match source_val_offset_file.read_u64::<BigEndian>() {
                    Ok(source_offset) => {
                        target_val_offset_file.write_u64::<BigEndian>(base_offset + source_offset)?;
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            return Err(error);
                        }
                        break;
                    }
                }
            }
            io::copy(&mut source_val_file, &mut target_val_file)?;
            base_offset = target_val_file.seek(SeekFrom::Current(0))?;
        }
        target_val_file.sync_all()?;
        target_val_offset_file.sync_all()?;
        Ok(())
    }
}

pub struct FullDocReader {
    address: SegmentAddress,
}

impl FeatureReader for FullDocReader {
    fn as_any(&self) -> &Any {
        self
    }
}

impl FullDocReader {
    pub fn read_doc(&self, docid: u64) -> Result<Doc, Error> {
        let mut offsets_file = self.address.open_file("fdo")?;
        let mut values_file = self.address.open_file("fdv")?;
        offsets_file.seek(SeekFrom::Start(docid * 8))?;
        let offset = offsets_file.read_u64::<BigEndian>()?;
        values_file.seek(SeekFrom::Start(offset))?;
        Ok(Deserialize::deserialize(&mut Deserializer::new(values_file)).unwrap())
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
    return Result::Ok(count);
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

impl<'a> serde::Serialize for FieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            FieldValue::String(ref value) => serializer.serialize_str(&value),
        }
    }
}

impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D>(deserializer: D) -> Result<FieldValue, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(FieldValueVisitor)
    }
}

use serde::de::{self, Visitor};
use std::fmt;

struct FieldValueVisitor;

impl<'de> Visitor<'de> for FieldValueVisitor {
    type Value = FieldValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("A string value")
    }

    fn visit_str<E>(self, value: &str) -> Result<FieldValue, E>
    where
        E: de::Error,
    {
        Ok(FieldValue::String(String::from(value)))
    }
}

#[cfg(test)]
mod tests {

    use super::read_vint;
    use super::write_vint;
    use super::Doc;
    use super::FieldValue;
    use proptest::collection::hash_map;
    use proptest::prelude::*;
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};
    use std::io::Cursor;

    fn arb_fieldvalue() -> BoxedStrategy<FieldValue> {
        prop_oneof![".*".prop_map(FieldValue::String),].boxed()
    }

    fn arb_fieldname() -> BoxedStrategy<String> {
        "[a-z]+".prop_map(|s| s).boxed()
    }

    fn arb_doc() -> BoxedStrategy<Doc> {
        hash_map(arb_fieldname(), arb_fieldvalue(), 0..100).boxed()
    }

    proptest!{
        #[test]
        fn read_write_correct(num in any::<u64>()) {
            let mut write = Cursor::new(vec![0 as u8; 100]);
            write_vint(&mut write, num).unwrap();
            write.set_position(0);
            assert!(num == read_vint(&mut write).unwrap())
        }

        #[test]
        fn serializes_doc_correct(ref doc in arb_doc()) {
            let mut buf = Vec::new();
            doc.serialize(&mut Serializer::new(&mut buf)).unwrap();
            let mut de = Deserializer::new(&buf[..]);
            assert!(doc == &Deserialize::deserialize(&mut de).unwrap());
        }
    }
}