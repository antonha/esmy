use super::Error;
use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use doc::Doc;
use doc::FieldValue;
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
            Some(disi) => for doc in disi {
                collector.collect(doc?)?;
            },
            None => (),
        };
    }
    Ok(())
}

pub trait Query: QueryClone + Debug {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error>;
    fn matches(&self, doc: &Doc) -> bool;
}

impl Query for Box<Query> {
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
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
    fn segment_matches(
        &self,
        reader: &SegmentReader,
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
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

/*
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

impl<'a> Query for TextQuery<'a> {
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
}*/

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
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
        Ok(Some(Box::new((0..reader.info().doc_count).map(|i| Ok(i)))))
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
    ) -> Result<Option<Box<Iterator<Item = Result<u64, Error>>>>, Error> {
        let mut sub: Vec<Box<Iterator<Item = Result<u64, Error>>>> =
            Vec::with_capacity(self.queries.len());
        for q in &self.queries {
            match q.segment_matches(reader)? {
                Some(sub_iter) => sub.push(sub_iter),
                None => return Ok(None),
            }
        }
        return Ok(Some(Box::new(AllIterator { sub })));
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

struct AllIterator {
    sub: Vec<Box<Iterator<Item = Result<u64, Error>>>>,
}

impl Iterator for AllIterator {
    type Item = Result<u64, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let size = self.sub.len();

        let mut target = {
            let s = &mut self.sub[0];
            match s.next() {
                Some(res) => match res {
                    Ok(t) => t,
                    Err(e) => return Some(Err(e)),
                },
                None => return None,
            }
        };

        let mut i = 0usize;
        let mut skip = 0usize;
        while i < size {
            if i != skip {
                let s = &mut self.sub[i];
                match advance(s, target) {
                    Some(r) => {
                        if let Err(e) = r {
                            return Some(Err(e));
                        }
                        let sub_doc_id = r.unwrap();
                        if sub_doc_id > target {
                            target = sub_doc_id;
                            if i != 0 {
                                skip = i;
                                i = 0;
                                continue;
                            } else {
                                skip = 0;
                            }
                        }
                    }
                    None => return None,
                }
            }
            i += 1;
        }
        return Some(Ok(target));
    }
}

fn advance(
    iter: &mut Iterator<Item = Result<u64, Error>>,
    target: u64,
) -> Option<Result<u64, Error>> {
    loop {
        let next = iter.next();
        match next {
            Some(res) => {
                if let Ok(n) = res {
                    if n < target {
                        continue;
                    } else {
                        return Some(Ok(n));
                    }
                } else {
                    return Some(res);
                }
            }
            None => {
                return None;
            }
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
