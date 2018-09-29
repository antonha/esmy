use std::collections::BTreeSet;
use std::collections::VecDeque;
use DocId;
use Error;

pub trait DocIter {
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
    fn current_doc(&self) -> Option<DocId>;
}

pub type Position = u64;
pub trait DocSpansIter: DocIter {
    fn next_start_pos(&mut self) -> Result<Option<Position>, Error>;
    fn start_pos(&self) -> Option<Position>;
    fn end_pos(&self) -> Option<Position>;
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
        self.current_doc = conjunction_advance(&mut self.sub)?;
        Ok(self.current_doc)
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
        AllDocsDocIter {
            num_docs,
            current: 0,
        }
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

pub struct OrderedNearDocSpansIter {
    sub_spans: Vec<Box<DocSpansIter>>,
    current_doc: Option<DocId>,
    position_queue: VecDeque<Position>,
    current_position: Option<Position>,
}

impl OrderedNearDocSpansIter {
    pub fn new(sub_spans: Vec<Box<DocSpansIter>>) -> OrderedNearDocSpansIter {
        OrderedNearDocSpansIter {
            sub_spans,
            current_doc: None,
            position_queue: VecDeque::new(),
            current_position: None,
        }
    }
}

impl DocIter for OrderedNearDocSpansIter {
    fn current_doc(&self) -> Option<DocId> {
        self.current_doc
    }

    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        loop {
            match conjunction_advance_span(&mut self.sub_spans)? {
                Some(doc) => {
                    //TODO this can be done so much better
                    let mut sub_pos = Vec::new();
                    for sub in self.sub_spans.iter_mut() {
                        let mut set = BTreeSet::new();
                        while let Some(pos) = sub.next_start_pos()? {
                            set.insert(pos);
                        }
                        sub_pos.push(set);
                    }
                    let mut valid_first_pos = VecDeque::new();
                    'outer: for first_pos in sub_pos.first().unwrap().iter() {
                        for (off, other_positions) in sub_pos.iter().enumerate().skip(1) {
                            if !other_positions.contains(&(*first_pos + off as u64)) {
                                continue 'outer;
                            }
                        }
                        valid_first_pos.push_back(*first_pos);
                    }
                    if !valid_first_pos.is_empty() {
                        self.position_queue = valid_first_pos;
                        self.current_doc = Some(doc);
                        break;
                    }
                }
                None => {
                    self.current_doc = None;
                    break;
                }
            }
        }
        Ok(self.current_doc)
    }
}

impl DocSpansIter for OrderedNearDocSpansIter {
    fn next_start_pos(&mut self) -> Result<Option<Position>, Error> {
        self.current_position = self.position_queue.pop_front();
        Ok(self.current_position)
    }

    fn start_pos(&self) -> Option<Position> {
        self.current_position
    }

    fn end_pos(&self) -> Option<Position> {
        self.current_position
            .map(|p| p + self.sub_spans.len() as u64)
    }
}

fn conjunction_advance(iters: &mut [Box<DocIter>]) -> Result<Option<DocId>, Error> {
    let size = iters.len();
    let mut target = {
        let s = &mut iters[0];
        match s.next_doc()? {
            Some(target) => target,
            None => return Ok(None),
        }
    };

    let mut i = 0usize;
    let mut skip = 0usize;
    while i < size {
        if i != skip {
            let s = &mut iters[i];
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
                    return Ok(None);
                }
            }
        }
        i += 1;
    }
    return Ok(Some(target));
}

//Same as above, can't figure out how to cast..
fn conjunction_advance_span(iters: &mut [Box<DocSpansIter>]) -> Result<Option<DocId>, Error> {
    let size = iters.len();
    let mut target = {
        let s = &mut iters[0];
        match s.next_doc()? {
            Some(target) => target,
            None => return Ok(None),
        }
    };

    let mut i = 0usize;
    let mut skip = 0usize;
    while i < size {
        if i != skip {
            let s = &mut iters[i];
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
                    return Ok(None);
                }
            }
        }
        i += 1;
    }
    return Ok(Some(target));
}
