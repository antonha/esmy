use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use bit_vec::BitVec;
use fasthash::RandomState;
use fasthash::sea::Hash64;
use fst::{self, Map, MapBuilder, Streamer};
use fst::map::OpBuilder;
use indexmap::IndexMap;
use indexmap::map;
use memmap::Mmap;

use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use Doc;
use doc::FieldValue;
use doc_iter::DocIter;
use DocId;
use error::Error;
use seg::Feature;
use seg::FeatureAddress;
use seg::FeatureConfig;
use seg::FeatureReader;
use seg::SegmentInfo;
use util::read_vint;
use util::write_vint;

const TERM_ID_LISTING: &str = "tid";
const ID_DOC_LISTING: &str = "iddoc";

#[derive(Clone)]
pub struct StringIndex {
    pub field_name: String,
    pub analyzer: Box<dyn Analyzer>,
}

impl StringIndex {
    pub fn new(field_name: String, analyzer: Box<dyn Analyzer>) -> StringIndex {
        StringIndex {
            field_name,
            analyzer,
        }
    }
}

impl StringIndex {
    fn write_docs<'a>(&self, address: &FeatureAddress, docs: &'a [Doc]) -> Result<(), Error> {
        let analyzer = &self.analyzer;
        let field_name = &self.field_name;
        let s = RandomState::<Hash64>::new();
        let mut map = IndexMap::with_hasher(s);

        for (doc_id, doc) in docs.iter().enumerate() {
            for (_name, val) in doc.iter().filter(|e| e.0 == field_name) {
                match *val {
                    FieldValue::String(ref value) => {
                        for token in analyzer.analyze(value) {
                            match map.entry(token) {
                                map::Entry::Vacant(vacant) => {
                                    vacant.insert(vec![doc_id as u64]);
                                }
                                map::Entry::Occupied(mut occupied) => {
                                    let term_docs = occupied.get_mut();
                                    if *term_docs.last().unwrap() != doc_id as u64 {
                                        term_docs.push(doc_id as u64)
                                    }
                                }
                            }
                        }
                    }
                };
            }
        }
        if map.is_empty() {
            return Ok(());
        }
        map.sort_keys();
        let fst_writer = BufWriter::new(File::create(address.with_ending(TERM_ID_LISTING))?);
        let mut target_terms = MapBuilder::new(fst_writer)?;
        let mut target_postings =
            BufWriter::new(File::create(address.with_ending(ID_DOC_LISTING))?);
        let mut offset = 0u64;
        for (term, doc_ids) in map.iter() {
            target_terms.insert(term.as_bytes(), offset)?;
            offset += u64::from(write_vint(&mut target_postings, doc_ids.len() as u64)?);
            let mut prev = 0u64;
            for doc_id in doc_ids {
                offset += u64::from(write_vint(&mut target_postings, (*doc_id - prev) as u64)?);
                prev = *doc_id;
            }
        }
        target_postings.flush()?;
        target_terms.finish()?;
        Ok(())
    }
}

impl Feature for StringIndex {
    fn feature_type(&self) -> &'static str {
        "string_index"
    }

    fn from_config(config: FeatureConfig) -> Self {
        let field_name = config.str_at("field").unwrap().to_string();
        let analyzer_name = config.str_at("analyzer").unwrap();
        let analyzer: Box<dyn Analyzer> = match analyzer_name {
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

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error> {
        self.write_docs(address, docs)
    }

    fn reader(&self, address: &FeatureAddress) -> Result<Box<dyn FeatureReader>, Error> {
        let path = address.with_ending(TERM_ID_LISTING);
        if path.exists() {
            let mmap = unsafe { Mmap::map(&File::open(path)?)? };
            Ok(Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: Some( Map::new(mmap)? ),
                }
            }))
        } else {
            Ok(Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: None,
                }
            }))
        }
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo, BitVec)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let target_terms_path = new_segment.with_ending(&TERM_ID_LISTING);
        let target_terms_file = File::create(&target_terms_path)?;

        let mut target_term_map = MapBuilder::new(BufWriter::new(target_terms_file))?;
        let target_postings_path = new_segment.with_ending(&ID_DOC_LISTING);
        let target_postings_file = File::create(&target_postings_path)?;
        let mut target_postings = BufWriter::new(target_postings_file);

        let (
            ref mut source_terms,
            ref mut source_postings,
            ref source_doc_offsets,
            ref mut deletions,
            ref mut deleted_remap,
        ) = {
            let mut source_terms = Vec::new();
            let mut source_postings = Vec::new();
            let mut source_doc_offsets = Vec::new();
            let mut source_offset = 0u64;
            let mut deletions = Vec::new();
            let mut deleted_remap = Vec::new();
            for (old_address, old_info, deleted_docs) in old_segments {
                let old_terms_path = old_address.with_ending(&TERM_ID_LISTING);
                if old_terms_path.exists() {
                    let mmap = unsafe { Mmap::map(&File::open(old_terms_path)?)? };
                    source_terms.push(Map::new(mmap)?);
                    source_postings.push(BufReader::new(File::open(
                        old_address.with_ending(ID_DOC_LISTING),
                    )?));
                    source_doc_offsets.push(source_offset);
                    source_offset +=
                        old_info.doc_count - deleted_docs.iter().filter(|b| *b).count() as u64;
                    deleted_remap.push(remap_deleted(&deleted_docs));
                    deletions.push(deleted_docs);
                }
            }
            (
                source_terms,
                source_postings,
                source_doc_offsets,
                deletions,
                deleted_remap,
            )
        };

        let mut op_builder = OpBuilder::new();
        for map in source_terms {
            op_builder.push(map.stream());
        }
        let mut union = op_builder.union();
        let mut postings_offset = 0u64;
        let mut has_written = false;
        while let Some((term, term_offsets)) = union.next() {
            let mut sorted_offsets = term_offsets.to_vec();
            sorted_offsets.sort_by_key(|o| o.index);

            let mut docs_to_write = Vec::new();
            for term_offset in &sorted_offsets {
                let mut source_posting = &mut source_postings[term_offset.index];
                source_posting.seek(SeekFrom::Start(term_offset.value as u64))?;
                let term_doc_count: u64 = read_vint(source_posting)?;
                let mut last_read_doc_id = 0;
                for _i in 0..term_doc_count {
                    let diff = read_vint(&mut source_posting)?;
                    let read_doc_id = last_read_doc_id + diff;
                    last_read_doc_id = read_doc_id;
                    if !deletions[term_offset.index]
                        .get(read_doc_id as usize)
                        .unwrap_or(false)
                        {
                            docs_to_write.push(
                                source_doc_offsets[term_offset.index]
                                    + deleted_remap[term_offset.index][read_doc_id as usize],
                            );
                        }
                }
            }

            if !docs_to_write.is_empty() {
                has_written = true;
                target_term_map.insert(term, postings_offset)?;
                let mut last_written_doc_id = 0u64;
                postings_offset +=
                    write_vint(&mut target_postings, docs_to_write.len() as u64)? as u64;
                for doc in docs_to_write {
                    postings_offset +=
                        write_vint(&mut target_postings, doc - last_written_doc_id)? as u64;
                    last_written_doc_id = doc;
                }
            }
        }
        target_term_map.finish()?;
        target_postings.flush()?;
        if !has_written {
            ::std::fs::remove_file(&target_postings_path)?;
            ::std::fs::remove_file(&target_terms_path)?;
        }
        Ok(())
    }
}

pub struct StringIndexReader {
    pub feature: StringIndex,
    pub address: FeatureAddress,
    pub map: Option<Map<Mmap>>,
}

impl FeatureReader for StringIndexReader {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl StringIndexReader {
    pub fn doc_iter(&self, term: &str) -> Result<Option<TermDocIter>, Error> {
        let maybe_offset = self.term_offset(term)?;
        match maybe_offset {
            None => Ok(None),
            Some(offset) => {
                let mut iddoc =
                    BufReader::new(File::open(self.address.with_ending(ID_DOC_LISTING))?);
                iddoc.seek(SeekFrom::Start(offset as u64))?;
                let num = read_vint(&mut iddoc)?;
                Ok(Some(TermDocIter {
                    file: iddoc,
                    current_doc_id: 0,
                    finished: false,
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

pub struct TermDocIter {
    file: BufReader<File>,
    current_doc_id: DocId,
    finished: bool,
    left: u64,
}

impl DocIter for TermDocIter {
    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        if self.left != 0 {
            self.left -= 1;
            match read_vint(&mut self.file) {
                Ok(diff) => {
                    self.current_doc_id += diff;
                    Ok(Some(self.current_doc_id))
                }
                Err(e) => Err(Error::from(e)),
            }
        } else {
            self.finished = true;
            Ok(None)
        }
    }

    fn current_doc(&self) -> Option<DocId> {
        if self.finished {
            None
        } else {
            Some(self.current_doc_id)
        }
    }
}

impl From<fst::Error> for Error {
    fn from(e: fst::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

fn remap_deleted(deleted_docs: &BitVec) -> Vec<u64> {
    let mut new_doc = 0u64;
    let mut ids = Vec::with_capacity(deleted_docs.len());
    for (_doc, deleted) in deleted_docs.iter().enumerate() {
        ids.push(new_doc);
        if !deleted {
            new_doc += 1;
        }
    }
    ids
}
