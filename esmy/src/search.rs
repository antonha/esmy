use super::Error;
use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use doc::Doc;
use doc::DocId;
use doc::FieldValue;
use doc_iter::AllDocIter;
use doc_iter::AllDocsDocIter;
use doc_iter::DocIter;
use doc_iter::VecDocIter;
use full_doc::FullDocCursor;
use index::ManagedIndexReader;
use seg::SegmentReader;
use std::fmt::Debug;

pub fn search(
    index_reader: &ManagedIndexReader,
    query: &impl Query,
    collector: &mut Collector,
) -> Result<(), Error> {
    for segment_reader in index_reader.segment_readers() {
        collector.set_reader(segment_reader)?;
        match query.segment_matches(&segment_reader)? {
            Some(mut disi) => {
                while let Some(doc_id) = disi.next_doc()? {
                    collector.collect(doc_id)?;
                }
            }
            None => (),
        };
    }
    Ok(())
}

pub trait Query: QueryClone + Debug {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error>;
    fn matches(&self, doc: &Doc) -> bool;
}

impl Query for Box<Query> {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        self.as_ref().segment_matches(reader)
    }

    fn matches(&self, doc: &Doc) -> bool {
        self.as_ref().matches(doc)
    }
}

pub trait QueryClone {
    fn clone_box(&self) -> Box<Query>;
}

impl<T> QueryClone for T
where
    T: 'static + Query + Clone,
{
    fn clone_box(&self) -> Box<Query> {
        Box::new(self.clone())
    }
}

impl Clone for Box<Query> {
    fn clone(&self) -> Box<Query> {
        self.clone_box()
    }
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

impl<'a> Query for ValueQuery {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        match reader.string_index(&self.field, &NoopAnalyzer) {
            Some(index) => match index.doc_iter(&self.value)? {
                Some(iter) => Ok(Some(Box::from(iter))),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn matches(&self, doc: &Doc) -> bool {
        match doc.get(&self.field) {
            Some(&FieldValue::String(ref val)) => &self.value == val,
            None => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct TermQuery {
    field: String,
    value: String,
    analyzer: Box<dyn Analyzer + 'static>,
}

impl TermQuery {
    pub fn new(field: String, value: String, analyzer: Box<dyn Analyzer + 'static>) -> TermQuery {
        TermQuery {
            field,
            value,
            analyzer,
        }
    }
}

impl Query for TermQuery {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        match reader.string_index(&self.field, &*self.analyzer) {
            Some(index) => match index.doc_iter(&self.value)? {
                Some(iter) => Ok(Some(Box::from(iter))),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    fn matches(&self, doc: &Doc) -> bool {
        match doc.get(&self.field) {
            Some(&FieldValue::String(ref val)) => self
                .analyzer
                .analyze(val)
                .find(|t| t == &self.value)
                .is_some(),
            None => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TextQuery {
    field: String,
    values: Vec<String>,
    //TODO: Cow<str> instead?
    analyzer: Box<Analyzer>,
}

impl TextQuery {
    pub fn new(field: String, value: String, analyzer: Box<Analyzer>) -> TextQuery {
        TextQuery {
            field: field,
            values: analyzer
                .analyze(&value)
                .map(|c| c.to_string())
                .collect::<Vec<String>>(),
            analyzer,
        }
    }
}

impl Query for TextQuery {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        if self.values.len() == 1 {
            let index = reader.string_index(&self.field, &*self.analyzer).unwrap();
            match index.doc_iter(&self.values[0])? {
                Some(iter) => Ok(Some(Box::from(iter))),
                None => Ok(None),
            }
        } else {
            let mut sub: Vec<Box<DocIter>> = Vec::with_capacity(self.values.len());
            let index = reader.string_index(&self.field, &*self.analyzer).unwrap();
            for q in &self.values {
                match index.doc_iter(&q)? {
                    Some(iter) => sub.push(Box::new(iter)),
                    None => return Ok(None),
                };
            }

            let mut full_doc = reader.full_doc().unwrap().cursor()?;
            let mut ids: Vec<DocId> = Vec::new();
            let mut all_iter = AllDocIter::new(sub);
            while let Some(doc_id) = all_iter.next_doc()? {
                if self.matches(&full_doc.read_doc(doc_id)?) {
                    ids.push(doc_id);
                }
            }
            return Ok(Some(Box::new(VecDocIter::new(ids))));
        }
    }

    fn matches(&self, doc: &Doc) -> bool {
        match doc.get(&self.field) {
            Some(&FieldValue::String(ref val)) => {
                let doc_vals = self
                    .analyzer
                    .analyze(val)
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>();
                doc_vals
                    .windows(self.values.len())
                    .find(|t| t == &self.values.as_slice())
                    .is_some()
            }
            None => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MatchAllDocsQuery;

impl MatchAllDocsQuery {
    pub fn new() -> MatchAllDocsQuery {
        MatchAllDocsQuery {}
    }
}

impl Query for MatchAllDocsQuery {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<DocIter>>, Error> {
        Ok(Some(Box::new(AllDocsDocIter::new(reader.info().doc_count))))
    }

    fn matches(&self, _doc: &Doc) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct AllQuery {
    queries: Vec<Box<Query>>,
}

impl AllQuery {
    pub fn new(queries: Vec<Box<Query>>) -> AllQuery {
        AllQuery { queries }
    }
}

impl Query for AllQuery {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<DocIter>>, Error> {
        let mut sub: Vec<Box<DocIter>> =
            Vec::with_capacity(self.queries.len());
        for q in &self.queries {
            match q.segment_matches(reader)? {
                Some(sub_iter) => sub.push(sub_iter),
                None => return Ok(None),
            }
        }
        return Ok(Some(Box::new(AllDocIter::new(sub))));
    }

    fn matches(&self, doc: &Doc) -> bool {
        for q in &self.queries {
            if !q.matches(doc) {
                return false;
            }
        }
        return true;
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

    fn collect(&mut self, _doc_id: DocId) -> Result<(), Error> {
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

    fn collect(&mut self, doc_id: DocId) -> Result<(), Error> {
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
