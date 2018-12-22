use std::any::Any;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Read;
use std::path::PathBuf;

use bit_vec::BitVec;
use rayon::prelude::*;
use rmps;

use analyzis::Analyzer;
use doc::Doc;
use error::Error;
use full_doc::FullDoc;
use full_doc::FullDocReader;
use string_index::StringIndex;
use string_index::StringIndexReader;
use string_pos_index::StringPosIndex;
use string_pos_index::StringPosIndexReader;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum FeatureConfig {
    None,
    Bool(bool),
    Int(i64),
    String(String),
    Map(HashMap<String, FeatureConfig>),
}

impl FeatureConfig {
    pub fn str_at(&self, path: &str) -> Option<&str> {
        if let FeatureConfig::Map(map) = self {
            if let Some(field) = map.get(path) {
                if let FeatureConfig::String(value) = field {
                    return Some(&value);
                }
            }
        }
        None
    }

    pub fn int_at(&self, path: &str) -> Option<i64> {
        if let FeatureConfig::Map(map) = self {
            if let Some(field) = map.get(path) {
                if let FeatureConfig::Int(value) = field {
                    return Some(*value);
                }
            }
        }
        None
    }

    fn is_none(&self) -> bool {
        match &self {
            FeatureConfig::None => true,
            _ => false,
        }
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
    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error>;
    fn reader(&self, address: &FeatureAddress) -> Result<Box<FeatureReader>, Error>;
    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo, BitVec)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error>;
}

pub trait FeatureReader: Sync + Send {
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
    #[serde(rename = "type")]
    ftype: String,
    #[serde(default = "no_config", skip_serializing_if = "FeatureConfig::is_none")]
    config: FeatureConfig,
}

fn no_config() -> FeatureConfig {
    FeatureConfig::None
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SegmentMeta {
    feature_metas: HashMap<String, FeatureMeta>,
    doc_count: u64,
}

#[derive(Clone)]
pub struct SegmentSchema {
    pub features: HashMap<String, Box<Feature>>,
}

#[derive(Default)]
pub struct SegmentSchemaBuilder {
    features: HashMap<String, Box<Feature>>,
}

impl SegmentSchemaBuilder {
    pub fn new() -> SegmentSchemaBuilder {
        SegmentSchemaBuilder {
            features: HashMap::new(),
        }
    }

    pub fn add_feature<N: Into<String>>(mut self, name: N, feature: Box<Feature>) -> Self {
        self.features.insert(name.into(), feature);
        self
    }

    pub fn add_string_index<N, F>(mut self, name: N, field: F, analyzer: Box<Analyzer>) -> Self
    where
        N: Into<String>,
        F: Into<String>,
    {
        self.features.insert(
            name.into(),
            Box::new(StringIndex::new(field.into(), analyzer)),
        );
        self
    }

    pub fn add_string_pos_index<N, F>(mut self, name: N, field: F, analyzer: Box<Analyzer>) -> Self
    where
        N: Into<String>,
        F: Into<String>,
    {
        self.features.insert(
            name.into(),
            Box::new(StringPosIndex::new(field.into(), analyzer)),
        );
        self
    }

    pub fn add_full_doc<N>(mut self, name: N) -> Self
    where
        N: Into<String>,
    {
        self.features.insert(name.into(), Box::new(FullDoc::new()));
        self
    }

    pub fn add_full_doc_with_compression<N>(mut self, name: N, compression_level: u32) -> Self
    where
        N: Into<String>,
    {
        self.features.insert(
            name.into(),
            Box::new(FullDoc::with_compression_level(compression_level)),
        );
        self
    }

    pub fn build(self) -> SegmentSchema {
        SegmentSchema {
            features: self.features,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SegmentAddress {
    pub path: PathBuf,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FeatureAddress {
    pub segment: SegmentAddress,
    pub name: String,
}

impl FeatureAddress {
    pub fn with_ending(&self, ending: &str) -> PathBuf {
        let file_name = format!("{}.{}.{}", self.segment.name, self.name, ending);
        self.segment.path.join(file_name).clone()
    }
}

#[derive(Clone)]
pub struct SegmentInfo {
    pub address: SegmentAddress,
    pub schema: SegmentSchema,
    pub doc_count: u64,
}

impl SegmentInfo {
    pub fn count_deleted(&self) -> Result<u64, Error> {
        Ok(self
            .address
            .read_deleted(self.doc_count as usize)?
            .iter()
            .filter(|b| *b)
            .count() as u64)
    }
}

pub fn schema_from_metas(feature_metas: HashMap<String, FeatureMeta>) -> SegmentSchema {
    let mut features = HashMap::new();
    for (name, feature_meta) in feature_metas {
        let feature: Box<Feature> = match feature_meta.ftype.as_ref() {
            "full_doc" => Box::new(FullDoc::from_config(feature_meta.config)),
            "string_index" => Box::new(StringIndex::from_config(feature_meta.config)),
            "string_pos_index" => Box::new(StringPosIndex::from_config(feature_meta.config)),
            //TODO error handling
            _ => panic!("No such feature"),
        };
        features.insert(name, feature);
    }
    SegmentSchema { features }
}

pub fn schema_to_feature_metas(schema: &SegmentSchema) -> HashMap<String, FeatureMeta> {
    let mut feature_metas = HashMap::new();
    for (name, feature) in &schema.features {
        feature_metas.insert(
            name.clone(),
            FeatureMeta {
                ftype: feature.feature_type().to_string(),
                config: feature.to_config(),
            },
        );
    }
    feature_metas
}

impl SegmentAddress {
    pub fn read_info(&self) -> Result<SegmentInfo, Error> {
        let seg_file = self.open_file("seg")?;
        let segment_meta: SegmentMeta = rmps::from_read(seg_file)?;

        let feature_metas = segment_meta.feature_metas;
        let schema = schema_from_metas(feature_metas);

        Ok(SegmentInfo {
            address: self.clone(),
            schema,
            doc_count: segment_meta.doc_count,
        })
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

    pub fn create_file(&self, ending: &str) -> Result<File, io::Error> {
        if !self.path.exists() {
            fs::create_dir_all(&self.path)?;
        }
        let name = format!("{}.{}", self.name, ending);
        File::create(self.path.join(name))
    }

    pub fn open_file(&self, ending: &str) -> Result<File, io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        File::open(file)
    }

    pub fn open_file_with_options(
        &self,
        ending: &str,
        options: OpenOptions,
    ) -> Result<File, io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        options.open(file)
    }

    pub fn open_file_if_exists(&self, ending: &str) -> Result<Option<File>, io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        if file.exists() {
            Ok(Some(File::open(file)?))
        } else {
            Ok(None)
        }
    }

    pub fn remove_file(&self, ending: &str) -> Result<(), io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        fs::remove_file(file)
    }

    pub fn read_deleted(&self, doc_count: usize) -> Result<BitVec<u32>, Error> {
        let deleted_docs = match self.open_file_if_exists(".del")? {
            Some(mut file) => {
                let mut buffer = Vec::with_capacity((doc_count / 8) as usize);
                file.read_to_end(&mut buffer)?;
                BitVec::from_bytes(&buffer)
            }
            None => BitVec::from_elem(doc_count, false),
        };
        Ok(deleted_docs)
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
    schema.features.par_iter().try_for_each(|(name, feature)| {
        feature.write_segment(
            &FeatureAddress {
                segment: address.clone(),
                name: name.clone(),
            },
            docs,
        )
    })?;
    let feature_metas = schema_to_feature_metas(&schema);
    let segment_meta = SegmentMeta {
        feature_metas,
        doc_count: docs.len() as u64,
    };
    let mut file = address.create_file("seg")?;
    rmps::encode::write(&mut file, &segment_meta).unwrap();
    Ok(())
}

pub fn merge(
    schema: &SegmentSchema,
    new_address: &SegmentAddress,
    addresses: &[SegmentAddress],
) -> Result<(), Error> {
    let mut infos: Vec<SegmentInfo> = Vec::with_capacity(addresses.len());
    for address in addresses {
        let mut seg_file = address.open_file("seg")?;
        let segment_meta: SegmentMeta = rmps::from_read(seg_file)?;

        let mut features = HashMap::new();
        for (name, feature_meta) in segment_meta.feature_metas {
            let feature: Box<Feature> = match feature_meta.ftype.as_ref() {
                "full_doc" => Box::new(FullDoc::from_config(feature_meta.config)),
                "string_index" => Box::new(StringIndex::from_config(feature_meta.config)),
                "string_pos_index" => Box::new(StringPosIndex::from_config(feature_meta.config)),
                //TODO error handling
                _ => panic!("No such feature"),
            };
            features.insert(name, feature);
        }

        infos.push(SegmentInfo {
            address: (*address).clone(),
            schema: SegmentSchema { features },
            doc_count: segment_meta.doc_count,
        });
    }
    schema
        .features
        .par_iter()
        .try_for_each(|(name, feature)| -> Result<(), Error> {
            let mut old_addressses: Vec<(FeatureAddress, SegmentInfo, BitVec)> = Vec::new();
            for info in &infos {
                let deleted_docs = info.address.read_deleted(info.doc_count as usize)?;
                old_addressses.push((
                    FeatureAddress {
                        segment: info.address.clone(),
                        name: name.clone(),
                    },
                    info.clone(),
                    deleted_docs,
                ))
            }
            feature.merge_segments(
                &old_addressses,
                &FeatureAddress {
                    segment: new_address.clone(),
                    name: name.clone(),
                },
            )?;
            Ok(())
        })?;
    let mut feature_metas = HashMap::new();
    for (name, feature) in &schema.features {
        feature_metas.insert(
            name.clone(),
            FeatureMeta {
                ftype: feature.feature_type().to_string(),
                config: feature.to_config(),
            },
        );
    }
    let mut num_deleted = 0u64;
    for info in &infos {
        num_deleted += info.count_deleted()?;
    }
    let doc_count: u64 = infos.iter().map(|i| i.doc_count).sum::<u64>() - num_deleted;
    let segment_meta = SegmentMeta {
        feature_metas,
        doc_count,
    };
    let mut file = new_address.create_file("seg")?;
    rmps::encode::write(&mut file, &segment_meta)?;
    Ok(())
}

pub struct SegmentReader {
    //address: SegmentAddress,
    info: SegmentInfo,
    deleted_docs: BitVec,
    readers: HashMap<String, Box<FeatureReader>>,
}

impl SegmentReader {
    pub fn open(info: SegmentInfo) -> Result<SegmentReader, Error> {
        let mut feature_readers = HashMap::new();
        for (name, feature) in info.schema.features.iter() {
            let address = &FeatureAddress {
                segment: info.address.clone(),
                name: name.clone(),
            };
            feature_readers.insert(name.clone(), feature.reader(address)?);
        }
        let deleted_docs = info.address.read_deleted(info.doc_count as usize)?;
        Ok(SegmentReader {
            info,
            deleted_docs,
            readers: feature_readers,
        })
    }

    pub fn info(&self) -> &SegmentInfo {
        &self.info
    }

    pub fn deleted_docs(&self) -> &BitVec {
        &self.deleted_docs
    }

    pub fn string_index(
        &self,
        field_name: &str,
        analyzer: &Analyzer,
    ) -> Option<&StringIndexReader> {
        for reader in self.readers.values() {
            if let Some(reader) = reader.as_any().downcast_ref::<StringIndexReader>() {
                if reader.feature.field_name == field_name
                    && analyzer.analyzer_type() == reader.feature.analyzer.analyzer_type()
                {
                    return Some(reader);
                }
            }
        }
        None
    }

    pub fn string_pos_index(
        &self,
        field_name: &str,
        analyzer: &Analyzer,
    ) -> Option<&StringPosIndexReader> {
        for reader in self.readers.values() {
            if let Some(reader) = reader.as_any().downcast_ref::<StringPosIndexReader>() {
                if reader.feature().field_name == field_name
                    && analyzer.analyzer_type() == reader.feature().analyzer.analyzer_type()
                {
                    return Some(reader);
                }
            }
        }
        None
    }

    pub fn full_doc(&self) -> Option<&FullDocReader> {
        for reader in self.readers.values() {
            if let Some(reader) = reader.as_any().downcast_ref::<FullDocReader>() {
                return Some(reader);
            }
        }
        None
    }
}
