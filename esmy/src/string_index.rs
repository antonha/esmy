use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use doc::FieldValue;
use doc_iter::DocIter;
use error::Error;
use fst::map::OpBuilder;
use fst::{self, Map, MapBuilder, Streamer};
use seg::Feature;
use seg::FeatureAddress;
use seg::FeatureConfig;
use seg::FeatureReader;
use seg::SegmentInfo;
use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use util::read_vint;
use util::write_vint;
use Doc;
use DocId;

const TERM_ID_LISTING: &str = "tid";
const ID_DOC_LISTING: &str = "iddoc";

#[derive(Clone)]
pub struct StringIndex {
    pub field_name: String,
    pub analyzer: Box<Analyzer>,
}

impl StringIndex {
    pub fn new(field_name: String, analyzer: Box<Analyzer>) -> StringIndex {
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
        let mut map: ::std::collections::BTreeMap<Cow<'a, str>, Vec<u64>> =
            ::std::collections::BTreeMap::new();
        for (doc_id, doc) in docs.iter().enumerate() {
            for (_name, val) in doc.iter().filter(|e| e.0 == field_name) {
                match *val {
                    FieldValue::String(ref value) => {
                        for token in analyzer.analyze(value) {
                            match map.entry(token) {
                                ::std::collections::btree_map::Entry::Vacant(vacant) => {
                                    vacant.insert(vec![doc_id as u64]);
                                }
                                ::std::collections::btree_map::Entry::Occupied(mut occupied) => {
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
        let x = 3;
        if x == 4 {
            eprint!("foo");
        }
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
        self.write_docs(address, docs)
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let mut sources: Vec<(u64, Map, BufReader<File>)> = Vec::with_capacity(old_segments.len());
        for (old_address, old_info) in old_segments {
            sources.push((
                old_info.doc_count,
                unsafe { Map::from_path(old_address.with_ending(&TERM_ID_LISTING))? },
                BufReader::new(File::open(old_address.with_ending(ID_DOC_LISTING))?),
            ))
        }
        let target = (
            MapBuilder::new(BufWriter::new(File::create(
                new_segment.with_ending(&TERM_ID_LISTING),
            )?))?,
            BufWriter::new(File::create(new_segment.with_ending(&ID_DOC_LISTING))?),
        );
        do_merge(&mut sources, target)
    }

    fn reader(&self, address: &FeatureAddress) -> Result<Box<FeatureReader>, Error> {
        let path = address.with_ending(TERM_ID_LISTING);
        if path.exists() {
            Ok(Box::new({
                StringIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: Some(unsafe { Map::from_path(path)? }),
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
    fn current_doc(&self) -> Option<DocId> {
        if self.finished {
            None
        } else {
            Some(self.current_doc_id)
        }
    }

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
}

impl From<fst::Error> for Error {
    fn from(e: fst::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

fn do_merge<R, W>(sources: &mut [(u64, Map, R)], target: (MapBuilder<W>, W)) -> Result<(), Error>
where
    W: Write + Sized,
    R: Read + Seek + Sized,
{
    let (mut term_builder, mut target_postings) = target;
    let (ref mut new_offsets, ref mut union, ref mut source_postings) = {
        let mut new_offset = 0u64;
        let mut new_offsets = Vec::with_capacity(sources.len());

        let mut op_builder = OpBuilder::new();
        let mut source_postings = Vec::new();
        for (doc_count, source_terms, source_posting) in sources.into_iter() {
            op_builder.push(source_terms.stream());
            new_offsets.push(new_offset);
            new_offset += *doc_count;
            source_postings.push(source_posting);
        }
        (new_offsets, op_builder.union(), source_postings)
    };

    let mut offset = 0u64;
    while let Some((term, term_offsets)) = union.next() {
        let mut sorted_offsets = term_offsets.to_vec();
        sorted_offsets.sort_by_key(|o| o.index);
        term_builder.insert(term, offset)?;

        let mut term_doc_counts: Vec<u64> = vec![0; source_postings.len()];
        for term_offset in &sorted_offsets {
            let mut source_posting = &mut source_postings[term_offset.index];
            source_posting.seek(SeekFrom::Start(term_offset.value as u64))?;
            term_doc_counts[term_offset.index] = read_vint(&mut source_posting)?;
        }

        let term_doc_count: u64 = term_doc_counts.iter().sum();
        offset += u64::from(write_vint(&mut target_postings, term_doc_count)?);
        let mut last_written_doc_id = 0u64;
        for term_offset in &sorted_offsets {
            let mut source_posting = &mut source_postings[term_offset.index];
            let mut last_read_doc_id = 0u64;
            for _i in 0..term_doc_counts[term_offset.index] {
                let diff = read_vint(&mut source_posting)?;
                let read_doc_id = last_read_doc_id + diff;
                let doc_id_to_write = new_offsets[term_offset.index] + read_doc_id;
                let diff_to_write = doc_id_to_write - last_written_doc_id;
                offset += u64::from(write_vint(&mut target_postings, diff_to_write)?);
                last_read_doc_id = read_doc_id;
                last_written_doc_id = doc_id_to_write;
            }
        }
    }
    term_builder.finish()?;
    target_postings.flush()?;
    Ok(())
}
