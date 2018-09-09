use super::Error;
use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use doc::Doc;
use doc::FieldValue;
use full_doc::FullDocCursor;
use index::ManagedIndexReader;
use seg::SegmentReader;
use std::borrow::Cow;

pub fn search(
    index_reader: &ManagedIndexReader,
    query: &SegmentQuery,
    collector: &mut Collector,
) -> Result<(), Error> {
    for segment_reader in index_reader.segment_readers() {
        collector.set_reader(segment_reader)?;
        match query.segment_matches(&segment_reader)? {
            Some(disi) => for doc in disi {
                collector.collect(doc?)?;
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
            Some(index) => match index.doc_iter(&self.value)? {
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
    analyzer: &'a Analyzer,
}

impl<'a> TextQuery<'a> {
    pub fn new<'n>(field: &'n str, value: &'n str, analyzer: &'n Analyzer) -> TextQuery<'n> {
        TextQuery {
            field: field,
            values: analyzer.analyze(value).collect::<Vec<Cow<str>>>(),
            analyzer,
        }
    }
}

impl<'a> SegmentQuery for TextQuery<'a> {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
        let index = reader.string_index(self.field, self.analyzer).unwrap();
        match index.doc_iter(&self.values[0])? {
            Some(iter) => Ok(Some(Box::from(iter))),
            None => Ok(None),
        }
    }
}

pub trait Collector {
    fn set_reader(&mut self, reader: &SegmentReader) -> Result<(), Error>;
    fn collect(&mut self, doc_id: u64) -> Result<(), Error>;
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
    fn set_reader(&mut self, _reader: &SegmentReader) -> Result<(), Error> {
        Ok(())
    }

    fn collect(&mut self, _doc_id: u64) -> Result<(), Error> {
        self.count += 1;
        Ok(())
    }
}

pub struct AllDocsCollector {
    docs: Vec<Doc>,
    doc_cursor: Option<FullDocCursor>,
}

impl AllDocsCollector {
    pub fn new() -> AllDocsCollector {
        AllDocsCollector {
            docs: Vec::new(),
            doc_cursor: None,
        }
    }

    pub fn docs(&self) -> &[Doc] {
        &self.docs
    }
}

impl Collector for AllDocsCollector {
    fn set_reader(&mut self, reader: &SegmentReader) -> Result<(), Error> {
        self.doc_cursor = Some(reader.full_doc().unwrap().cursor()?);
        Ok(())
    }

    fn collect(&mut self, doc_id: u64) -> Result<(), Error> {
        match &mut self.doc_cursor {
            Some(curs) => {
                let doc = curs.read_doc(doc_id)?;
                self.docs.push(doc);
            }
            None => {}
        }
        Ok(())
    }
}
