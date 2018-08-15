use super::Error;
use afsort;
use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use fst::map::OpBuilder;
use fst::{Map, MapBuilder, Streamer};
use rand::{self, Rng};
use rmps;
use serde::{self, Deserialize, Serialize};
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use walkdir::WalkDir;

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum FeatureConfig {
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Map(HashMap<String, FeatureConfig>),
}

impl FeatureConfig {
    fn str_at(&self, path: &str) -> Option<&str> {
        if let FeatureConfig::Map(map) = self {
            if let Some(field) = map.get(path) {
                if let FeatureConfig::String(value) = field {
                    return Some(&value);
                }
            }
        }
        return None;
    }
}

pub trait Feature: FeatureClone + Sync + Send {
    fn feature_type(&self) -> &'static str;
    //TODO add error for faulty configs
    fn from_config(FeatureConfig) -> Self
    where
        Self: Sized;
    fn to_config(&self) -> FeatureConfig;
    fn as_any(&self) -> &Any;
    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error>;
    fn reader<'a>(&self, address: SegmentAddress) -> Box<FeatureReader>;
    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error>;
}

pub trait FeatureReader {
    fn as_any(&self) -> &Any;
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

#[derive(Serialize, Deserialize, Debug)]
pub struct FeatureMeta {
    feature_type: String,
    feature_config: FeatureConfig,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SegmentMeta {
    feature_metas: Vec<FeatureMeta>,
    doc_count: u64,
}

#[derive(Clone)]
pub struct SegmentSchema {
    pub features: Vec<Box<Feature>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SegmentAddress {
    path: PathBuf,
    name: String,
}

#[derive(Clone)]
pub struct SegmentInfo {
    pub address: SegmentAddress,
    pub schema: SegmentSchema,
    pub doc_count: u64,
}

#[derive(Clone)]
pub struct Index {
    pub schema_template: SegmentSchema,
    path: PathBuf,
}

impl SegmentAddress {
    pub fn read_info(&self) -> Result<SegmentInfo, Error> {
        let seg_file = self.open_file("seg")?;
        let segment_meta: SegmentMeta = rmps::from_read(seg_file)?;

        let mut features = Vec::new();
        for feature_meta in segment_meta.feature_metas {
            let feature: Box<Feature> = match feature_meta.feature_type.as_ref() {
                "full_doc" => Box::new(FullDoc::from_config(feature_meta.feature_config)),
                "string_index" => Box::new(StringIndex::from_config(feature_meta.feature_config)),
                //TODO error handling
                _ => panic!("No such feature"),
            };
            features.push(feature);
        }

        return Ok(SegmentInfo {
            address: self.clone(),
            schema: SegmentSchema { features },
            doc_count: segment_meta.doc_count,
        });
    }

    pub fn remove_files(&self) -> Result<(), io::Error> {
        let dir = fs::read_dir(&self.path)?;
        for path_res in dir {
            let entry = path_res?;
            if entry.file_type()?.is_file()
                && entry.file_name().to_string_lossy().starts_with(&self.name)
            {
                fs::remove_file(&entry.path())?
            }
        }
        Ok(())
    }

    fn create_file(&self, ending: &str) -> Result<File, io::Error> {
        if !self.path.exists() {
            fs::create_dir_all(&self.path).unwrap();
        }
        let name = format!("{}.{}", self.name, ending);
        File::create(self.path.join(name))
    }

    fn open_file(&self, ending: &str) -> Result<File, io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        File::open(file)
    }

    fn remove_file(&self, ending: &str) -> Result<(), io::Error> {
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
            e.file_type().is_dir() || e
                .file_name()
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
                    name,
                }
            }).collect::<Vec<SegmentAddress>>()
    }

    pub fn merge(&self, addresses: &[&SegmentAddress]) -> Result<SegmentAddress, Error> {
        let mut infos: Vec<SegmentInfo> = Vec::with_capacity(addresses.len());
        for address in addresses.into_iter() {
            let mut seg_file = address.open_file("seg")?;
            let segment_meta: SegmentMeta = rmps::from_read(seg_file)?;

            let mut features = Vec::new();
            for feature_meta in segment_meta.feature_metas {
                let feature: Box<Feature> = match feature_meta.feature_type.as_ref() {
                    "full_doc" => Box::new(FullDoc::from_config(feature_meta.feature_config)),
                    "string_index" => {
                        Box::new(StringIndex::from_config(feature_meta.feature_config))
                    }
                    //TODO error handling
                    _ => panic!("No such feature"),
                };
                features.push(feature);
            }

            infos.push(SegmentInfo {
                address: (*address).clone(),
                schema: SegmentSchema { features },
                doc_count: segment_meta.doc_count,
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
        let mut feature_metas = Vec::new();
        for feature in &self.schema_template.features {
            feature_metas.push(FeatureMeta {
                feature_type: feature.feature_type().to_string(),
                feature_config: feature.to_config(),
            });
        }
        let segment_meta = SegmentMeta {
            feature_metas,
            doc_count,
        };
        let mut file = new_segment_address.create_file("seg")?;
        rmps::encode::write(&mut file, &segment_meta).unwrap();
        for address in addresses {
            address.remove_file("seg")?;
        }
        Ok(new_segment_address)
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
    let mut feature_metas = Vec::new();
    for feature in &schema.features {
        feature_metas.push(FeatureMeta {
            feature_type: feature.feature_type().to_string(),
            feature_config: feature.to_config(),
        });
    }
    let segment_meta = SegmentMeta {
        feature_metas,
        doc_count: docs.len() as u64,
    };
    let mut file = address.create_file("seg")?;
    rmps::encode::write(&mut file, &segment_meta).unwrap();
    Ok(())
}

pub struct SegmentReader {
    //address: SegmentAddress,
    readers: Vec<Box<FeatureReader>>,
}

impl SegmentReader {
    pub fn new(info: SegmentInfo) -> SegmentReader {
        SegmentReader {
            readers: info
                .schema
                .features
                .iter()
                .map(|feature| feature.reader(info.address.clone()))
                .collect(),
        }
    }

    pub fn string_index(
        &self,
        field_name: &str,
        analyzer: &Analyzer,
    ) -> Option<&StringIndexReader> {
        for reader in self.readers.iter() {
            match reader.as_any().downcast_ref::<StringIndexReader>() {
                Some(reader) => {
                    if reader.feature.field_name == field_name
                        && analyzer.analyzer_type() == reader.feature.analyzer.analyzer_type()
                    {
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
        afsort::sort_unstable_by(&mut terms, |t| &t.0);
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
        let mut iddoc = BufWriter::new(
            address.create_file(&format!("{}.{}", self.field_name, ID_DOC_LISTING))?,
        );
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

impl Feature for StringIndex {
    fn as_any(&self) -> &Any {
        self
    }

    fn feature_type(&self) -> &'static str {
        "string_index"
    }

    fn from_config(config: FeatureConfig) -> Self {
        let field_name = config.str_at("name").unwrap().to_string();
        let analyzer_name = config.str_at("analyzer").unwrap();
        let analyzer: Box<Analyzer> = match analyzer_name {
            "uax29" => Box::new(UAX29Analyzer),
            "whitespace" => Box::new(WhiteSpaceAnalyzer),
            "noop" => Box::new(NoopAnalyzer),
            _ => panic!("No such analyzer"),
        };
        StringIndex {
            field_name,
            analyzer,
        }
    }

    fn to_config(&self) -> FeatureConfig {
        let mut map = HashMap::new();
        map.insert(
            "name".to_string(),
            FeatureConfig::String(self.field_name.to_string()),
        );
        map.insert(
            "analyzer".to_string(),
            FeatureConfig::String(self.analyzer.analyzer_type().to_string()),
        );
        FeatureConfig::Map(map)
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
            }).collect::<Vec<&SegmentInfo>>();
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
            iddoc.flush()?;
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
                let mut iddoc = BufReader::new(
                    self.address
                        .open_file(&format!("{}.{}", field, ID_DOC_LISTING))?,
                );
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

    fn feature_type(&self) -> &'static str {
        "full_doc"
    }

    fn from_config(_config: FeatureConfig) -> Self {
        FullDoc {}
    }

    fn to_config(&self) -> FeatureConfig {
        FeatureConfig::Map(HashMap::new())
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error> {
        let mut offset: u64;
        let mut doc_offsets = BufWriter::new(address.create_file("fdo")?);
        let mut docs_packed = address.create_file("fdv")?;
        for doc in docs {
            offset = docs_packed.seek(SeekFrom::Current(0))?;
            doc_offsets.write_u64::<BigEndian>(offset)?;
            doc.serialize(&mut rmps::Serializer::new(&docs_packed))
                .unwrap();
        }
        doc_offsets.flush()?;
        docs_packed.flush()?;
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
        let mut target_val_offset_file = BufWriter::new(new_segment.create_file("fdo")?);
        let mut target_val_file = new_segment.create_file("fdv")?;
        let mut base_offset = 0u64;
        for segment in old_segments.iter() {
            let mut source_val_offset_file = BufReader::new(segment.address.open_file("fdo")?);
            loop {
                match source_val_offset_file.read_u64::<BigEndian>() {
                    Ok(source_offset) => {
                        target_val_offset_file
                            .write_u64::<BigEndian>(base_offset + source_offset)?;
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            return Err(Error::IOError);
                        }
                        break;
                    }
                }
            }
            let mut source_val_file = segment.address.open_file("fdv")?;
            io::copy(&mut source_val_file, &mut target_val_file)?;
            base_offset = target_val_file.seek(SeekFrom::Current(0))?;
        }
        target_val_file.flush()?;
        target_val_offset_file.flush()?;
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
        Ok(rmps::from_read(values_file).unwrap())
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
