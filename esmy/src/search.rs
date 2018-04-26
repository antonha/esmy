use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use seg::{Doc, FieldValue, IndexReader, SegmentReader};
use std::borrow::Cow;
use std::io::Error;

pub fn search(
    index_reader: &IndexReader,
    query: &SegmentQuery,
    collector: &mut Collector,
) -> Result<(), Error> {
    for segment_reader in index_reader.segment_readers() {
        match query.segment_matches(&segment_reader)? {
            Some(disi) => for doc in disi {
                collector.collect(segment_reader, doc?);
            },
            None => (),
        };
    }
    Ok(())
}

pub trait SegmentQuery {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error>;
}

pub trait FullDocQuery {
    fn matches(&self, doc: &Doc) -> bool;
}

#[derive(Debug, Clone)]
pub struct ValueQuery {
    field: String,
    value: String,
}

impl<'a> ValueQuery {
    pub fn new(field: String, value: String) -> ValueQuery {
        ValueQuery { field, value }
    }
}

impl<'a> SegmentQuery for ValueQuery {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
        match reader.string_index(&self.field, &NoopAnalyzer) {
            Some(index) => match index.doc_iter(&self.field, &self.value)? {
                Some(iter) => Ok(Some(Box::from(iter))),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }
}

impl FullDocQuery for ValueQuery {
    fn matches(&self, doc: &Doc) -> bool {
        match doc.get(&self.field) {
            Some(&FieldValue::String(ref val)) => &self.value == val,
            None => false,
        }
    }
}

pub struct TextQuery<'a> {
    field: &'a str,
    values: Vec<Cow<'a, str>>,
    analyzer: &'a Analyzer
}

impl<'a> TextQuery<'a> {
    pub fn new<'n>(field: &'n str, value: &'n str, analyzer: &'n Analyzer) -> TextQuery<'n> {
        TextQuery {
            field: field,
            values: analyzer.analyze(value).collect::<Vec<Cow<str>>>(),
            analyzer
        }
    }
}

impl<'a> SegmentQuery for TextQuery<'a> {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
        let index = reader.string_index(self.field, self.analyzer).unwrap();
        match index.doc_iter(self.field, &self.values[0])? {
            Some(iter) => Ok(Some(Box::from(iter))),
            None => Ok(None),
        }
    }
}

pub trait Collector {
    fn collect(&mut self, reader: &SegmentReader, doc_id: u64);
}

pub struct CountCollector {
    count: u64,
}

impl CountCollector {
    pub fn new() -> CountCollector {
        CountCollector { count: 0 }
    }

    pub fn total_count(&self) -> u64 {
        self.count
    }
}

impl Collector for CountCollector {
    fn collect(&mut self, _reader: &SegmentReader, _doc_id: u64) {
        self.count += 1;
    }
}

pub struct AllDocsCollector {
    docs: Vec<Doc>,
}

impl AllDocsCollector {
    pub fn new() -> AllDocsCollector {
        AllDocsCollector { docs: Vec::new() }
    }

    pub fn docs(&self) -> &[Doc] {
        &self.docs
    }
}

impl Collector for AllDocsCollector {
    fn collect(&mut self, reader: &SegmentReader, doc_id: u64) {
        let doc = reader.full_doc().unwrap().read_doc(doc_id).unwrap();
        self.docs.push(doc);
    }
}
