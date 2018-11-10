use std::any::Any;
use std::fmt::Debug;

use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use doc::FieldValue;
use doc_iter::AllDocIter;
use doc_iter::AllDocsDocIter;
use doc_iter::DocIter;
use doc_iter::DocSpansIter;
use doc_iter::OrderedNearDocSpansIter;
use doc_iter::VecDocIter;
use index::ManagedIndexReader;
use seg::SegmentReader;
use Doc;
use DocId;

use super::Error;

pub fn search(
    index_reader: &ManagedIndexReader,
    query: &impl Query,
    collector: &mut Collector,
) -> Result<(), Error> {
    for segment_reader in index_reader.segment_readers() {
        if let Some(mut disi) = query.segment_matches(&segment_reader)? {
            collector.collect_for(segment_reader, &mut *disi)?;
        };
    }
    Ok(())
}

pub trait Query: QueryClone + Debug + Sync {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error>;
    fn matches(&self, doc: &Doc) -> bool;
    fn as_any(&self) -> &Any;
}

impl Query for Box<Query> {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        self.as_ref().segment_matches(reader)
    }

    fn matches(&self, doc: &Doc) -> bool {
        self.as_ref().matches(doc)
    }

    fn as_any(&self) -> &Any {
        &*self
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
        (**self).clone_box()
    }
}

#[derive(Debug, Clone)]
pub struct ValueQuery {
    field: String,
    value: String,
}

impl<'a> ValueQuery {
    pub fn new<F, V>(field: F, value: V) -> ValueQuery
    where
        F: Into<String>,
        V: Into<String>,
    {
        ValueQuery {
            field: field.into(),
            value: value.into(),
        }
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn value(&self) -> &str {
        &self.value
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

    fn as_any(&self) -> &Any {
        self
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
            Some(&FieldValue::String(ref val)) => {
                self.analyzer.analyze(val).any(|t| t == self.value)
            }
            None => false,
        }
    }

    fn as_any(&self) -> &Any {
        self
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
    pub fn new<N, V>(field: N, value: V, analyzer: Box<Analyzer>) -> TextQuery
    where
        N: Into<String>,
        V: Into<String>,
    {
        let v = value.into();
        let values = analyzer
            .analyze(&v)
            .map(|c| c.to_string())
            .collect::<Vec<String>>();
        TextQuery {
            field: field.into(),
            values,
            analyzer,
        }
    }
}

impl Query for TextQuery {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        if self.values.len() == 1 {
            if let Some(string_index_reader) = reader.string_index(&self.field, &*self.analyzer) {
                match string_index_reader.doc_iter(&self.values[0])? {
                    Some(iter) => Ok(Some(Box::from(iter))),
                    None => Ok(None),
                }
            } else if let Some(string_pos_index_reader) =
                reader.string_pos_index(&self.field, &*self.analyzer)
            {
                return match string_pos_index_reader.doc_spans_iter(&self.values[0])? {
                    Some(iter) => Ok(Some(Box::from(iter))),
                    None => Ok(None),
                };
            } else if let Some(full_doc_reader) = reader.full_doc() {
                let mut doc_ids = Vec::new();
                if let Some(mut cursor) = full_doc_reader.cursor()? {
                    for doc_id in 0..reader.info().doc_count {
                        let doc = cursor.read_doc(doc_id)?;
                        if self.matches(&doc) {
                            doc_ids.push(doc_id);
                        }
                    }
                }
                Ok(Some(Box::new(VecDocIter::new(doc_ids))))
            } else {
                panic!()
            }
        } else if let Some(string_pos_reader) =
            reader.string_pos_index(&self.field, &*self.analyzer)
        {
            let mut sub_spans = Vec::new();
            for v in &self.values {
                if let Some(sub_span) = string_pos_reader.doc_spans_iter(&v)? {
                    sub_spans.push(Box::new(sub_span) as Box<DocSpansIter>);
                } else {
                    return Ok(None);
                }
            }
            return Ok(Some(
                Box::new(OrderedNearDocSpansIter::new(sub_spans)) as Box<DocIter>
            ));
        } else if let Some(string_reader) = reader.string_index(&self.field, &*self.analyzer) {
            let mut sub: Vec<Box<DocIter>> = Vec::with_capacity(self.values.len());
            for v in &self.values {
                match string_reader.doc_iter(&v)? {
                    Some(iter) => sub.push(Box::new(iter)),
                    None => return Ok(None),
                };
            }

            let mut ids: Vec<DocId> = Vec::new();
            if let Some(mut full_doc) = reader.full_doc().unwrap().cursor()? {
                let mut all_iter = AllDocIter::new(sub);
                while let Some(doc_id) = all_iter.next_doc()? {
                    if self.matches(&full_doc.read_doc(doc_id)?) {
                        ids.push(doc_id);
                    }
                }
            }
            return Ok(Some(Box::new(VecDocIter::new(ids))));
        } else {
            //TODO fix
            panic!();
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
                    .any(|t| t == self.values.as_slice())
            }
            None => false,
        }
    }

    fn as_any(&self) -> &Any {
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct MatchAllDocsQuery;

impl MatchAllDocsQuery {
    pub fn new() -> MatchAllDocsQuery {
        MatchAllDocsQuery {}
    }
}

impl Query for MatchAllDocsQuery {
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        Ok(Some(Box::new(AllDocsDocIter::new(reader.info().doc_count))))
    }

    fn matches(&self, _doc: &Doc) -> bool {
        true
    }

    fn as_any(&self) -> &Any {
        self
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
    fn segment_matches(&self, reader: &SegmentReader) -> Result<Option<Box<DocIter>>, Error> {
        let mut sub: Vec<Box<DocIter>> = Vec::with_capacity(self.queries.len());
        for q in &self.queries {
            match q.segment_matches(reader)? {
                Some(sub_iter) => sub.push(sub_iter),
                None => return Ok(None),
            }
        }
        Ok(Some(Box::new(AllDocIter::new(sub))))
    }

    fn matches(&self, doc: &Doc) -> bool {
        for q in &self.queries {
            if !q.matches(doc) {
                return false;
            }
        }
        true
    }

    fn as_any(&self) -> &Any {
        self
    }
}

pub trait Collector: Sync {
    fn collect_for(&mut self, reader: &SegmentReader, docs: &mut DocIter) -> Result<(), Error>;
}

#[derive(Default)]
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
    fn collect_for(&mut self, _reader: &SegmentReader, docs: &mut DocIter) -> Result<(), Error> {
        let mut i = 0u64;
        while let Some(_doc_id) = docs.next_doc()? {
            i += 1;
        }
        self.count += i;
        Ok(())
    }
}

#[derive(Default)]
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
    fn collect_for(&mut self, reader: &SegmentReader, docs: &mut DocIter) -> Result<(), Error> {
        if let Some(mut doc_cursor) = reader.full_doc().unwrap().cursor()? {
            while let Some(doc_id) = docs.next_doc().unwrap() {
                if !reader.deleted_docs().get(doc_id as usize).unwrap_or(false) {
                    let doc = doc_cursor.read_doc(doc_id).unwrap();
                    self.docs.push(doc);
                }
            }
        }
        Ok(())
    }
}
