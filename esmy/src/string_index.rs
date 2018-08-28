use afsort;
use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use doc::Doc;
use doc::FieldValue;
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

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

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
    fn docs_to_term_map<'a>(&self, docs: &'a [Doc]) -> Vec<(Cow<'a, str>, u64)> {
        let analyzer = &self.analyzer;
        let field_name = &self.field_name;
        let mut terms: Vec<(Cow<'a, str>, u64)> = docs
            .iter()
            .enumerate()
            .flat_map(|(doc_id, doc)| {
                let mut doc_terms = Vec::new();
                for (_name, val) in doc.iter().filter(|e| e.0 == field_name) {
                    match *val {
                        FieldValue::String(ref value) => {
                            doc_terms.extend(
                                analyzer.analyze(value).map(|token| (token, doc_id as u64)),
                            );
                        }
                    };
                }
                doc_terms
            }).collect();
        afsort::sort_unstable_by(&mut terms, |t| &t.0);
        terms
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
        let terms = self.docs_to_term_map(docs);
        if terms.len() > 0 {
            let fst_writer = BufWriter::new(File::create(address.with_ending(TERM_ID_LISTING))?);
            let target_postings =
                BufWriter::new(File::create(address.with_ending(ID_DOC_LISTING))?);
            let target_terms = MapBuilder::new(fst_writer)?;
            write_term_map(terms, target_terms, target_postings)?;
        }
        Ok(())
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
        do_merge(&mut sources, target)?;
        Ok(())
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
    pub fn doc_iter(&self, term: &str) -> Result<Option<DocIter>, Error> {
        let maybe_offset = self.term_offset(term)?;
        match maybe_offset {
            None => Ok(None),
            Some(offset) => {
                let mut iddoc =
                    BufReader::new(File::open(self.address.with_ending(ID_DOC_LISTING))?);
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
            Some(match read_vint(&mut self.file) {
                Ok(doc) => Ok(doc),
                Err(e) => Err(Error::from(e)),
            })
        } else {
            None
        }
    }
}

impl From<fst::Error> for Error {
    fn from(e: fst::Error) -> Self {
        return Error::Other(Box::new(e));
    }
}

fn write_term_map<W>(
    terms: Vec<(Cow<str>, u64)>,
    mut target_terms: MapBuilder<W>,
    mut target_postings: W,
) -> Result<(), Error>
where
    W: Write + Sized,
{
    if terms.is_empty() {
        return Ok(());
    }

    let mut offset: u64 = 0;

    let mut ids = Vec::new();
    let mut last_term = &terms[0].0;
    for &(ref term, id) in terms.iter() {
        if term != last_term {
            target_terms.insert(&last_term.as_bytes(), offset)?;
            offset += write_vint(&mut target_postings, ids.len() as u64)? as u64;
            for id in ids.iter() {
                offset += write_vint(&mut target_postings, *id)? as u64;
            }
            ids.clear();
        }
        ids.push(id);
        last_term = term;
    }
    target_terms.insert(&last_term.as_bytes(), offset)?;
    offset += write_vint(&mut target_postings, ids.len() as u64)? as u64;
    for id in ids.iter() {
        offset += write_vint(&mut target_postings, *id)? as u64;
    }
    target_terms.finish()?;
    target_postings.flush()?;
    Ok(())
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
        term_builder.insert(term, offset)?;

        let mut term_doc_counts: Vec<u64> = vec![0; source_postings.len()];
        for term_offset in term_offsets {
            let mut source_posting = source_postings.get_mut(term_offset.index).unwrap();
            source_posting.seek(SeekFrom::Start(term_offset.value as u64))?;
            term_doc_counts[term_offset.index] = read_vint(&mut source_posting)?;
        }

        let term_doc_count: u64 = term_doc_counts.iter().sum();
        offset += write_vint(&mut target_postings, term_doc_count)? as u64;
        for term_offset in term_offsets {
            let mut source_posting = source_postings.get_mut(term_offset.index).unwrap();
            for _i in 0..term_doc_counts[term_offset.index] {
                let doc_id = read_vint(&mut source_posting)?;
                offset += write_vint(
                    &mut target_postings,
                    new_offsets[term_offset.index] + doc_id,
                )? as u64;
            }
        }
    }
    term_builder.finish()?;
    target_postings.flush()?;
    Ok(())
}
