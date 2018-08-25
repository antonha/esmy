use analyzis::Analyzer;
use doc::Doc;
use error::Error;
use full_doc::FullDoc;
use full_doc::FullDocReader;
use rmps;
use std::any::Any;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::PathBuf;
use string_index::StringIndex;
use string_index::StringIndexReader;
use rayon::prelude::*;

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum FeatureConfig {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
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
        return None;
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
    fn reader<'a>(&self, address: &FeatureAddress) -> Box<FeatureReader>;
    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo)],
        new_segment: &FeatureAddress,
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
    #[serde(rename = "type")]
    ftype: String,
    #[serde(
        default = "no_config",
        skip_serializing_if = "FeatureConfig::is_none"
    )]
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

pub fn schema_from_metas(feature_metas: HashMap<String, FeatureMeta>) -> SegmentSchema {
    let mut features = HashMap::new();
    for (name, feature_meta) in feature_metas {
        let feature: Box<Feature> = match feature_meta.ftype.as_ref() {
            "full_doc" => Box::new(FullDoc::from_config(feature_meta.config)),
            "string_index" => Box::new(StringIndex::from_config(feature_meta.config)),
            //TODO error handling
            _ => panic!("No such feature"),
        };
        features.insert(name, feature);
    }
    let schema = SegmentSchema { features };
    schema
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
    return feature_metas;
}

impl SegmentAddress {
    pub fn read_info(&self) -> Result<SegmentInfo, Error> {
        let seg_file = self.open_file("seg")?;
        let segment_meta: SegmentMeta = rmps::from_read(seg_file)?;

        let feature_metas = segment_meta.feature_metas;
        let schema = schema_from_metas(feature_metas);

        return Ok(SegmentInfo {
            address: self.clone(),
            schema: schema,
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

    pub fn create_file(&self, ending: &str) -> Result<File, io::Error> {
        if !self.path.exists() {
            fs::create_dir_all(&self.path).unwrap();
        }
        let name = format!("{}.{}", self.name, ending);
        File::create(self.path.join(name))
    }

    pub fn open_file(&self, ending: &str) -> Result<File, io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        File::open(file)
    }

    pub fn remove_file(&self, ending: &str) -> Result<(), io::Error> {
        let name = format!("{}.{}", self.name, ending);
        let file = self.path.join(name);
        fs::remove_file(file)
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
    for (name, feature) in &schema.features {
        feature.write_segment(
            &FeatureAddress {
                segment: address.clone(),
                name: name.clone(),
            },
            docs,
        )?;
    }
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
    addresses: &[&SegmentAddress],
) -> Result<(), Error> {
    let mut infos: Vec<SegmentInfo> = Vec::with_capacity(addresses.len());
    for address in addresses.into_iter() {
        let mut seg_file = address.open_file("seg")?;
        let segment_meta: SegmentMeta = rmps::from_read(seg_file)?;

        let mut features = HashMap::new();
        for (name, feature_meta) in segment_meta.feature_metas {
            let feature: Box<Feature> = match feature_meta.ftype.as_ref() {
                "full_doc" => Box::new(FullDoc::from_config(feature_meta.config)),
                "string_index" => Box::new(StringIndex::from_config(feature_meta.config)),
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
    schema.features.par_iter().try_for_each( | (name, feature) |  -> Result<(), Error> {
        let old_addressses: Vec<(FeatureAddress, SegmentInfo)> = infos
            .iter()
            .map(|i| {
                (
                    FeatureAddress {
                        segment: i.address.clone(),
                        name: name.clone(),
                    },
                    i.clone(),
                )
            }).collect();
        feature.merge_segments(
            &old_addressses,
            &FeatureAddress {
                segment: new_address.clone(),
                name: name.clone(),
            },
        )
    })?;
    let doc_count: u64 = infos.iter().map(|i| i.doc_count).sum();
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
    readers: HashMap<String, Box<FeatureReader>>,
}

impl SegmentReader {
    pub fn new(info: SegmentInfo) -> SegmentReader {
        SegmentReader {
            readers: info
                .schema
                .features
                .iter()
                .map(|(name, feature)| {
                    (
                        name.clone(),
                        feature.reader(&FeatureAddress {
                            segment: info.address.clone(),
                            name: name.clone(),
                        }),
                    )
                }).collect(),
        }
    }

    pub fn string_index(
        &self,
        field_name: &str,
        analyzer: &Analyzer,
    ) -> Option<&StringIndexReader> {
        for reader in self.readers.values() {
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
        for reader in self.readers.values() {
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
