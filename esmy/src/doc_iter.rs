use doc::DocId;
use Error;

pub trait DocIter {
    fn current_doc(&self) -> Option<DocId>;
    fn next_doc(&mut self) -> Result<Option<DocId>, Error>;
    fn advance(&mut self, target: DocId) -> Result<Option<DocId>, Error> {
        loop {
            let next = self.next_doc()?;
            match next {
                Some(doc_id) => {
                    if doc_id < target {
                        continue;
                    } else {
                        return Ok(Some(doc_id));
                    }
                }
                None => {
                    return Ok(None);
                }
            }
        }
    }
}

pub struct AllDocIter {
    sub: Vec<Box<DocIter>>,
    current_doc: Option<DocId>,
}

impl AllDocIter {
    pub fn new(sub: Vec<Box<DocIter>>) -> AllDocIter {
        AllDocIter {
            sub,
            current_doc: None,
        }
    }
}

impl DocIter for AllDocIter {
    fn current_doc(&self) -> Option<DocId> {
        self.current_doc
    }

    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        let size = self.sub.len();
        let mut target = {
            let s = &mut self.sub[0];
            match s.next_doc()? {
                Some(target) => target,
                None => {
                    self.current_doc = None;
                    return Ok(None);
                }
            }
        };

        let mut i = 0usize;
        let mut skip = 0usize;
        while i < size {
            if i != skip {
                let s = &mut self.sub[i];
                match s.advance(target)? {
                    Some(sub_doc_id) => {
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
                    None => {
                        self.current_doc = None;
                        return Ok(None);
                    }
                }
            }
            i += 1;
        }
        self.current_doc = Some(target);
        return Ok(Some(target));
    }
}

pub struct VecDocIter {
    doc_ids: Vec<DocId>,
    pos: usize,
}

impl VecDocIter {
    pub fn new(doc_ids: Vec<DocId>) -> VecDocIter {
        VecDocIter { doc_ids, pos: 0 }
    }
}

impl DocIter for VecDocIter {

    fn current_doc(&self) -> Option<DocId> {
        if self.pos >= self.doc_ids.len() {
            None
        } else {
            Some(self.doc_ids[self.pos])
        }
    }

    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        if self.pos >= self.doc_ids.len() {
            Ok(None)
        } else {
            let res = Some(self.doc_ids[self.pos]);
            self.pos += 1;
            Ok(res)
        }
    }

}

pub struct AllDocsDocIter {
    num_docs: DocId,
    current: DocId,
}

impl AllDocsDocIter {
    pub fn new(num_docs: DocId) -> AllDocsDocIter {
        AllDocsDocIter { num_docs, current: 0 }
    }
}

impl DocIter for AllDocsDocIter {

    fn current_doc(&self) -> Option<DocId> {
        if self.current >= self.num_docs {
            None
        } else {
            Some(self.current)
        }
    }

    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        if self.current >= self.num_docs {
            Ok(None)
        } else {
            let res = Some(self.current);
            self.current += 1;
            Ok(res)
        }
    }

}
