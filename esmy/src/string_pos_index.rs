use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use bit_vec::BitVec;
use fasthash::RandomState;
use fasthash::sea::SeaHash;
use fst::{Map, MapBuilder, Streamer};
use fst::map::OpBuilder;
use indexmap::IndexMap;
use indexmap::map;
use smallvec::SmallVec;

use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use Doc;
use doc::FieldValue;
use doc_iter::DocIter;
use doc_iter::DocSpansIter;
use doc_iter::Position;
use DocId;
use error::Error;
use seg::Feature;
use seg::FeatureAddress;
use seg::FeatureConfig;
use seg::FeatureReader;
use seg::SegmentInfo;
use util::read_vint;
use util::write_vint;

const TERM_ID_LISTING: &str = "tid";
const ID_DOC_LISTING: &str = "iddoc";

#[derive(Clone)]
pub struct StringPosIndex {
    pub field_name: String,
    pub analyzer: Box<Analyzer>,
}

impl StringPosIndex {
    pub fn new(field_name: String, analyzer: Box<Analyzer>) -> StringPosIndex {
        StringPosIndex {
            field_name,
            analyzer,
        }
    }
}

impl StringPosIndex {
    fn write_docs<'a>(&self, address: &FeatureAddress, docs: &'a [Doc]) -> Result<(), Error> {
        let analyzer = &self.analyzer;
        let field_name = &self.field_name;
        let s = RandomState::<SeaHash>::new();
        let mut map = IndexMap::with_hasher(s);
        for (doc_id, doc) in docs.iter().enumerate() {
            for (_name, val) in doc.iter().filter(|e| e.0 == field_name) {
                match *val {
                    FieldValue::String(ref value) => {
                        for (pos, token) in analyzer.analyze(value).enumerate() {
                            match map.entry(token) {
                                map::Entry::Vacant(vacant) => {
                                    let mut pos_vec = SmallVec::<[u64; 1]>::new();
                                    pos_vec.push(pos as u64);
                                    let mut doc_pos_vec =
                                        SmallVec::<[(u64, SmallVec<[u64; 1]>); 1]>::new();
                                    doc_pos_vec.push((doc_id as u64, pos_vec));
                                    vacant.insert(doc_pos_vec);
                                }
                                map::Entry::Occupied(mut occupied) => {
                                    let mut term_docs = occupied.get_mut();
                                    if term_docs.last().unwrap().0 == doc_id as u64 {
                                        term_docs.last_mut().unwrap().1.push(pos as u64);
                                    } else {
                                        let mut pos_vec = SmallVec::<[u64; 1]>::new();
                                        pos_vec.push(pos as u64);
                                        term_docs.push((doc_id as u64, pos_vec))
                                    }
                                }
                            }
                        }
                    }
                };
            }
        }
        map.sort_keys();
        if map.is_empty() {
            return Ok(());
        }

        let fst_writer = BufWriter::new(File::create(address.with_ending(TERM_ID_LISTING))?);
        let mut target_terms = MapBuilder::new(fst_writer)?;
        let mut target_postings =
            BufWriter::new(File::create(address.with_ending(ID_DOC_LISTING))?);
        let mut target_positions = BufWriter::new(File::create(address.with_ending("pos"))?);
        let mut id_offset = 0u64;
        let mut pos_offset = 0u64;

        for (term, doc_ids_and_pos) in map {
            target_terms.insert(term.as_bytes(), id_offset)?;
            id_offset += u64::from(write_vint(
                &mut target_postings,
                doc_ids_and_pos.len() as u64,
            )?);
            let mut prev_doc_id = 0u64;
            let mut prev_pos_offset = 0u64;
            for (doc_id, positions) in doc_ids_and_pos {
                id_offset += u64::from(write_vint(
                    &mut target_postings,
                    (doc_id - prev_doc_id) as u64,
                )?);
                id_offset += u64::from(write_vint(
                    &mut target_postings,
                    (pos_offset - prev_pos_offset) as u64,
                )?);
                prev_pos_offset = pos_offset;
                pos_offset += u64::from(write_vint(&mut target_positions, positions.len() as u64)?);
                let mut last_pos = 0u64;
                for pos in positions {
                    pos_offset +=
                        u64::from(write_vint(&mut target_positions, (pos - last_pos) as u64)?);
                    last_pos = pos;
                }
                prev_doc_id = doc_id;
            }
        }
        target_positions.flush()?;
        target_postings.flush()?;
        target_terms.finish()?;
        Ok(())
    }
}

impl Feature for StringPosIndex {
    fn feature_type(&self) -> &'static str {
        "string_pos_index"
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
        StringPosIndex {
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

    fn as_any(&self) -> &Any {
        self
    }

    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error> {
        self.write_docs(address, docs)
    }

    fn reader(&self, address: &FeatureAddress) -> Result<Box<FeatureReader>, Error> {
        let path = address.with_ending(TERM_ID_LISTING);
        if path.exists() {
            Ok(Box::new({
                StringPosIndexReader {
                    total_doc_count: address.segment.read_info()?.doc_count,
                    feature: self.clone(),
                    address: address.clone(),
                    map: Some(unsafe { Map::from_path(path)? }),
                }
            }))
        } else {
            Ok(Box::new({
                StringPosIndexReader {
                    total_doc_count: address.segment.read_info()?.doc_count,
                    feature: self.clone(),
                    address: address.clone(),
                    map: None,
                }
            }))
        }
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo, BitVec)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let target_map_path = new_segment.with_ending(&TERM_ID_LISTING);
        let mut term_builder = MapBuilder::new(BufWriter::new(File::create(&target_map_path)?))?;
        let target_postings_path = new_segment.with_ending(&ID_DOC_LISTING);
        let mut target_postings = BufWriter::new(File::create(&target_postings_path)?);
        let target_positions_path = new_segment.with_ending("pos");
        let mut target_positions = BufWriter::new(File::create(&target_positions_path)?);

        let (
            ref mut source_maps,
            ref mut source_postings,
            ref mut source_positions,
            ref mut source_doc_offsets,
            ref deletions,
            ref deleted_remap,
        ) = {
            let mut source_maps = Vec::new();
            let mut source_postings = Vec::new();
            let mut source_positions = Vec::new();
            let mut source_offset = 0u64;
            let mut source_doc_offsets = Vec::new();
            let mut deletions = Vec::new();
            let mut deleted_remap = Vec::new();
            for (old_address, old_info, deleted_docs) in old_segments {
                let source_terms_path = old_address.with_ending(&TERM_ID_LISTING);
                if source_terms_path.exists() {
                    source_maps.push(unsafe { Map::from_path(source_terms_path)? });
                    source_postings.push(BufReader::new(File::open(
                        old_address.with_ending(ID_DOC_LISTING),
                    )?));
                    source_positions
                        .push(BufReader::new(File::open(old_address.with_ending("pos"))?));
                    source_doc_offsets.push(source_offset);
                    source_offset +=
                        old_info.doc_count - deleted_docs.iter().filter(|b| *b).count() as u64;
                    deleted_remap.push(remap_deleted(&deleted_docs));
                    deletions.push(deleted_docs);
                }
            }
            (
                source_maps,
                source_postings,
                source_positions,
                source_doc_offsets,
                deletions,
                deleted_remap,
            )
        };

        let mut postings_offset = 0u64;
        let mut positions_offset = 0u64;
        let mut op_builder = OpBuilder::new();
        for map in source_maps {
            op_builder.push(map.stream());
        }
        let mut union = op_builder.union();

        let mut has_written = false;
        while let Some((term, term_offsets)) = union.next() {
            let mut sorted_offsets = term_offsets.to_vec();
            sorted_offsets.sort_by_key(|o| o.index);

            let mut docs_to_write = Vec::new();

            for term_offset in sorted_offsets {
                let mut source_posting = &mut source_postings[term_offset.index];
                source_posting.seek(SeekFrom::Start(term_offset.value as u64))?;
                let mut source_position = &mut source_positions[term_offset.index];

                let term_doc_count = read_vint(source_posting)?;

                let mut last_read_doc_id = 0u64;
                let mut last_read_pos_offset = 0u64;
                for _i in 0..term_doc_count {
                    let diff = read_vint(&mut source_posting)?;
                    let read_doc_id = last_read_doc_id + diff;
                    last_read_doc_id = read_doc_id;

                    let positions_offset_diff = read_vint(&mut source_posting)?;
                    let read_position_offset = positions_offset_diff + last_read_pos_offset;
                    last_read_pos_offset = read_position_offset;
                    source_position.seek(SeekFrom::Start(read_position_offset))?;

                    if !deletions[term_offset.index]
                        .get(read_doc_id as usize)
                        .unwrap_or(false)
                        {
                            let mut positions = Vec::new();
                            let num_positions = read_vint(source_position)?;
                            let mut last_read_position = 0u64;
                            for _j in 0..num_positions {
                                let pos_diff = read_vint(source_position)?;
                                let read_position = last_read_position + pos_diff;
                                positions.push(read_position);
                                last_read_position = read_position;
                            }
                            let doc_id_to_write = source_doc_offsets[term_offset.index]
                                + deleted_remap[term_offset.index][read_doc_id as usize];
                            docs_to_write.push((doc_id_to_write, positions));
                        }
                }
            }

            if !docs_to_write.is_empty() {
                let mut last_written_doc_id = 0u64;
                let mut last_written_pos_offset = 0u64;
                term_builder.insert(term, postings_offset)?;
                postings_offset +=
                    write_vint(&mut target_postings, docs_to_write.len() as u64)? as u64;
                for (doc, positions) in docs_to_write {
                    postings_offset +=
                        write_vint(&mut target_postings, doc - last_written_doc_id)? as u64;
                    last_written_doc_id = doc;

                    postings_offset += write_vint(
                        &mut target_postings,
                        positions_offset - last_written_pos_offset,
                    )? as u64;
                    last_written_pos_offset = positions_offset;

                    positions_offset +=
                        write_vint(&mut target_positions, positions.len() as u64)? as u64;
                    let mut last_written_position = 0u64;
                    for pos in positions {
                        positions_offset +=
                            write_vint(&mut target_positions, pos - last_written_position)? as u64;
                        last_written_position = pos;
                    }
                }
                has_written = true;
            }
        }
        term_builder.finish()?;
        target_postings.flush()?;
        target_positions.flush()?;
        if !has_written {
            ::std::fs::remove_file(target_map_path)?;
            ::std::fs::remove_file(target_postings_path)?;
            ::std::fs::remove_file(target_positions_path)?;
        }
        Ok(())
    }
}

pub struct StringPosIndexReader {
    feature: StringPosIndex,
    address: FeatureAddress,
    map: Option<Map>,
    total_doc_count: u64,
}

impl StringPosIndexReader {
    pub fn feature(&self) -> &StringPosIndex {
        &self.feature
    }
}

impl FeatureReader for StringPosIndexReader {
    fn as_any(&self) -> &Any {
        self
    }
}

impl StringPosIndexReader {
    pub fn doc_spans_iter(&self, term: &str) -> Result<Option<TermDocSpansIter>, Error> {
        let maybe_offset = self.term_offset(term)?;
        match maybe_offset {
            None => Ok(None),
            Some(offset) => {
                let mut iddoc =
                    BufReader::new(File::open(self.address.with_ending(ID_DOC_LISTING))?);
                iddoc.seek(SeekFrom::Start(offset as u64))?;
                let num = read_vint(&mut iddoc)?;
                let mut pos = BufReader::new(File::open(self.address.with_ending("pos"))?);
                Ok(Some(TermDocSpansIter {
                    total_num_docs: self.total_doc_count,
                    count: num,
                    doc_file: iddoc,
                    pos_file: pos,
                    current_doc_id: 0,
                    current_pos_offset: 0,
                    current_pos: 0,
                    pos_left: 0,
                    finished: false,
                    pos_count: 0,
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

pub struct TermDocSpansIter {
    total_num_docs: u64,
    count: u64,
    doc_file: BufReader<File>,
    pos_file: BufReader<File>,
    current_doc_id: DocId,
    current_pos_offset: u64,
    current_pos: u64,
    pos_left: u64,
    pos_count: u64,
    finished: bool,
    left: u64,
}

impl TermDocSpansIter {
    pub fn doc_count(&self) -> u64 {
        self.count
    }

    pub fn pos_count(&self) -> u64 {
        self.pos_count
    }
}

impl DocIter for TermDocSpansIter {
    fn score(&self) -> Option<f32> {
        if self.finished {
            None
        } else {
            //Basic tf-idf, bm25 coming up
            let tf_idf = (self.pos_count as f32) / (self.total_num_docs as f32 / self.count as f32).log(2f32);
            eprintln!("TF-IDF: {}", tf_idf);
            Some(tf_idf)
        }
    }

    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        if self.left != 0 {
            self.left -= 1;
            let doc_diff = read_vint(&mut self.doc_file)?;
            self.current_doc_id += doc_diff;
            let pos_offset_diff = read_vint(&mut self.doc_file)?;
            self.current_pos_offset += pos_offset_diff;
            self.pos_file
                .seek(SeekFrom::Start(self.current_pos_offset))?;
            self.pos_left = read_vint(&mut self.pos_file)?;
            self.pos_count = self.pos_left;
            self.current_pos = 0;
            Ok(Some(self.current_doc_id))
        } else {
            self.finished = true;
            Ok(None)
        }
    }

    fn current_doc(&self) -> Option<DocId> {
        if self.finished {
            None
        } else {
            Some(self.current_doc_id)
        }
    }
}

impl DocSpansIter for TermDocSpansIter {
    fn next_start_pos(&mut self) -> Result<Option<Position>, Error> {
        if self.pos_left == 0 {
            return Ok(None);
        }
        let pos_diff = read_vint(&mut self.pos_file)?;
        self.current_pos += pos_diff;
        self.pos_left -= 1;
        Ok(Some(self.current_pos))
    }

    fn start_pos(&self) -> Option<Position> {
        if self.pos_left == 0 {
            Some(self.current_pos)
        } else {
            None
        }
    }

    fn end_pos(&self) -> Option<Position> {
        self.start_pos().map(|start| start + 1)
    }
}

fn remap_deleted(deleted_docs: &BitVec) -> Vec<u64> {
    let mut new_doc = 0u64;
    let mut ids = Vec::with_capacity(deleted_docs.len());
    for (_doc, deleted) in deleted_docs.iter().enumerate() {
        ids.push(new_doc);
        if !deleted {
            new_doc += 1;
        }
    }
    ids
}
