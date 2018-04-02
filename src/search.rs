use analyzis::Analyzer;
use seg::{Doc, IndexReader, SegmentReader, FieldValue};
use std::borrow::Cow;
use std::io::Error;

pub fn search(
    index_reader: &IndexReader,
    query: &SegmentQuery,
    collector: &mut Collector,
) -> Result<(), Error> {
    for segment_reader in index_reader.segment_readers() {
        match query.segment_matches(&segment_reader) {
            Some(disi) => for doc in disi {
                collector.collect(segment_reader, doc.unwrap());
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
    ) -> Option<Box<Iterator<Item = Result<u64, Error>>>>;
}

pub trait FullDocQuery {
    fn matches(&self, doc: &Doc) -> bool;
}

pub struct ValueQuery<'a> {
    field: &'a str,
    value: &'a str,
}

impl<'a> SegmentQuery for ValueQuery<'a> {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Option<Box<Iterator<Item = Result<u64, Error>>>> {
        let index = reader.string_index(self.field).unwrap();
        match index.doc_iter(self.field, &self.value).unwrap() {
            Some(iter) => Some(Box::from(iter)),
            None => None,
        }
    }
}

impl<'a> FullDocQuery for ValueQuery<'a> {
    fn matches(&self, doc: &Doc) -> bool{
        match doc.get(self.field){
            Some(&FieldValue::String(ref val)) => &val == &self.value,
            None => false
        }
    }
}

pub struct TextQuery<'a> {
    field: &'a str,
    values: Vec<Cow<'a, str>>,
}

impl<'a> TextQuery<'a> {
    pub fn new<'n>(field: &'n str, value: &'n str, analyzer: &Analyzer) -> TextQuery<'n> {
        TextQuery {
            field: field,
            values: analyzer.analyze(value).collect::<Vec<Cow<str>>>(),
        }
    }
}

impl<'a> SegmentQuery for TextQuery<'a> {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Option<Box<Iterator<Item = Result<u64, Error>>>> {
        let index = reader.string_index(self.field).unwrap();
        match index.doc_iter(self.field, &self.values[0]).unwrap() {
            Some(iter) => Some(Box::from(iter)),
            None => None,
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
