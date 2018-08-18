use analyzis::Analyzer;
use doc::Doc;
use error::Error;
use full_doc::FullDoc;
use full_doc::FullDocReader;
use rand::{self, Rng};
use rmps;
use std::any::Any;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::path::PathBuf;
use string_index::StringIndex;
use string_index::StringIndexReader;
use walkdir::WalkDir;

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
    feature_type: String,
    feature_config: FeatureConfig,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SegmentMeta {
    feature_metas: HashMap<String, FeatureMeta>,
    doc_count: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMeta {
    pub feature_template_metas: HashMap<String, FeatureMeta>,
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

#[derive(Clone)]
pub struct Index {
    pub schema_template: SegmentSchema,
    path: PathBuf,
}

pub fn schema_from_metas(feature_metas: HashMap<String, FeatureMeta>) -> SegmentSchema {
    let mut features = HashMap::new();
    for (name, feature_meta) in feature_metas {
        let feature: Box<Feature> = match feature_meta.feature_type.as_ref() {
            "full_doc" => Box::new(FullDoc::from_config(feature_meta.feature_config)),
            "string_index" => Box::new(StringIndex::from_config(feature_meta.feature_config)),
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
                feature_type: feature.feature_type().to_string(),
                feature_config: feature.to_config(),
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

pub fn read_index_meta(path: &Path) -> Result<IndexMeta, Error> {
    let file = File::open(path.join("index_meta"))?;
    Ok(rmps::from_read(file)?)
}

pub fn write_index_meta(path: &Path, meta: &IndexMeta) -> Result<(), Error> {
    let mut file = File::create(path.join("index_meta"))?;
    Ok(rmps::encode::write(&mut file, meta)?)
}

impl Index {
    pub fn new(schema_template: SegmentSchema, path: PathBuf) -> Index {
        Index {
            schema_template,
            path,
        }
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

            let mut features = HashMap::new();
            for (name, feature_meta) in segment_meta.feature_metas {
                let feature: Box<Feature> = match feature_meta.feature_type.as_ref() {
                    "full_doc" => Box::new(FullDoc::from_config(feature_meta.feature_config)),
                    "string_index" => {
                        Box::new(StringIndex::from_config(feature_meta.feature_config))
                    }
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
        let new_segment_address = SegmentAddress {
            path: PathBuf::from(&self.path),
            name: random_name(),
        };
        for (name, feature) in self.schema_template.features.iter() {
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
                    segment: new_segment_address.clone(),
                    name: name.clone(),
                },
            )?;
        }
        let doc_count: u64 = infos.iter().map(|i| i.doc_count).sum();
        let mut feature_metas = HashMap::new();
        for (name, feature) in &self.schema_template.features {
            feature_metas.insert(
                name.clone(),
                FeatureMeta {
                    feature_type: feature.feature_type().to_string(),
                    feature_config: feature.to_config(),
                },
            );
        }
        let segment_meta = SegmentMeta {
            feature_metas,
            doc_count,
        };
        let mut file = new_segment_address.create_file("seg")?;
        rmps::encode::write(&mut file, &segment_meta)?;
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
