use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use fst::map::OpBuilder;
use fst::{self, Map, MapBuilder, Streamer};
use error::Error;
use std::borrow::Cow;
use seg::FeatureAddress;
use std::io::BufWriter;
use seg::Feature;
use seg::FeatureConfig;
use std::collections::HashMap;
use seg::FeatureReader;
use seg::SegmentInfo;
use std::io::BufReader;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Write;
use std::any::Any;
use std::fs::File;
use util::read_vint;
use util::write_vint;
use doc::Doc;
use doc::FieldValue;
use afsort;

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";


#[derive(Clone)]
pub struct StringIndex {
    pub field_name: String,
    pub analyzer: Box<Analyzer>,
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
        address: &FeatureAddress,
        terms: Vec<(Cow<str>, u64)>,
    ) -> Result<(), Error> {
        if terms.is_empty() {
            return Ok(());
        }

        let mut offset: u64 = 0;
        let tid = File::create(address.with_ending(TERM_ID_LISTING))?;
        //TODO: Not unwrap
        let mut tid_builder = MapBuilder::new(BufWriter::new(tid))?;
        let mut iddoc = BufWriter::new(File::create(address.with_ending(ID_DOC_LISTING))?);
        let mut ids = Vec::new();
        let mut last_term = &terms[0].0;
        for &(ref term, id) in terms.iter() {
            if term != last_term {
                tid_builder.insert(&last_term.as_bytes(), offset)?;
                offset += write_vint(&mut iddoc, ids.len() as u64)? as u64;
                for id in ids.iter() {
                    offset += write_vint(&mut iddoc, *id)? as u64;
                }
                ids.clear();
            }
            ids.push(id);
            last_term = term;
        }
        tid_builder.insert(&last_term.as_bytes(), offset)?;
        offset += write_vint(&mut iddoc, ids.len() as u64)? as u64;
        for id in ids.iter() {
            offset += write_vint(&mut iddoc, *id)? as u64;
        }
        tid_builder.finish()?;
        iddoc.flush()?;
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
        let field_name = config.str_at("field").unwrap().to_string();
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
            "field".to_string(),
            FeatureConfig::String(self.field_name.to_string()),
        );
        map.insert(
            "analyzer".to_string(),
            FeatureConfig::String(self.analyzer.analyzer_type().to_string()),
        );
        FeatureConfig::Map(map)
    }

    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error> {
        let term_map = self.docs_to_term_map(docs);
        self.write_term_map(address, term_map)
    }

    fn reader(&self, address: &FeatureAddress) -> Box<FeatureReader> {
        let path = address.with_ending(TERM_ID_LISTING);
        if path.exists() {
            Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: Some(unsafe { Map::from_path(path).unwrap() }),
                }
            })
        } else {
            Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: None,
                }
            })
        }
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let mut maps = Vec::with_capacity(old_segments.len());
        let mut source_id_doc_files = Vec::with_capacity(old_segments.len());
        {
            for (old_address, old_info) in old_segments.iter() {
                let path = old_address.with_ending(TERM_ID_LISTING);
                maps.push(unsafe { Map::from_path(path).unwrap() });
                source_id_doc_files.push(BufReader::new(File::open(old_address.with_ending(&TERM_ID_LISTING))?));
            }

            let mut op_builder = OpBuilder::new();
            for map in maps.iter() {
                op_builder.push(map.stream());
            }

            let tid = File::create(new_segment.with_ending(TERM_ID_LISTING))?;
            let mut tid_builder = MapBuilder::new(BufWriter::new(tid))?;
            let fp = File::create(new_segment.with_ending(ID_DOC_LISTING))?;
            let mut iddoc = BufWriter::new(fp);
            let mut new_offsets = Vec::with_capacity(old_segments.len());
            {
                let mut new_offset = 0;
                for (_old_address, old_info) in old_segments.iter() {
                    new_offsets.push(new_offset);
                    new_offset += old_info.doc_count;
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
            tid_builder.finish()?;
            iddoc.flush()?;
        }
        Ok(())
    }
}

pub struct StringIndexReader {
    pub feature: StringIndex,
    pub address: FeatureAddress,
    pub map: Option<Map>,
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
                    File::open(self.address.with_ending(ID_DOC_LISTING))?
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
            Some(match read_vint(& mut self.file) {
                Ok(doc) => Ok(doc),
                Err(e) => Err(Error::from(e))
            })
        } else {
            None
        }
    }
}

impl From<fst::Error> for Error {
    fn from(_e: fst::Error) -> Self {
        return Error::IOError;
    }
}
