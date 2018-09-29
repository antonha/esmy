use analyzis::Analyzer;
use analyzis::NoopAnalyzer;
use analyzis::UAX29Analyzer;
use analyzis::WhiteSpaceAnalyzer;
use doc::FieldValue;
use doc_iter::DocIter;
use doc_iter::DocSpansIter;
use doc_iter::Position;
use error::Error;
use fst::map::OpBuilder;
use fst::{Map, MapBuilder, Streamer};
use seg::Feature;
use seg::FeatureAddress;
use seg::FeatureConfig;
use seg::FeatureReader;
use seg::SegmentInfo;
use std::any::Any;
use std::borrow::Cow;
use std::collections::btree_map;
use std::collections::BTreeMap;
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
use Doc;
use DocId;

const TERM_ID_LISTING: &'static str = "tid";
const ID_DOC_LISTING: &'static str = "iddoc";

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
        let mut map: BTreeMap<Cow<'a, str>, Vec<(u64, Vec<u64>)>> = BTreeMap::new();
        for (doc_id, doc) in docs.iter().enumerate() {
            for (_name, val) in doc.iter().filter(|e| e.0 == field_name) {
                match *val {
                    FieldValue::String(ref value) => {
                        for (pos, token) in analyzer.analyze(value).enumerate() {
                            match map.entry(token) {
                                btree_map::Entry::Vacant(vacant) => {
                                    vacant.insert(vec![(doc_id as u64, vec![pos as u64])]);
                                }
                                btree_map::Entry::Occupied(mut occupied) => {
                                    let mut term_docs = occupied.get_mut();
                                    if term_docs.last().unwrap().0 == doc_id as u64 {
                                        term_docs.last_mut().unwrap().1.push(pos as u64);
                                    } else {
                                        term_docs.push((doc_id as u64, vec![pos as u64]))
                                    }
                                }
                            }
                        }
                    }
                };
            }
        }
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
        for (term, doc_ids_and_pos) in map.iter() {
            target_terms.insert(term.as_bytes(), id_offset)?;
            id_offset += write_vint(&mut target_postings, doc_ids_and_pos.len() as u64)? as u64;
            let mut prev_doc_id = 0u64;
            let mut prev_pos_offset = 0u64;
            for (doc_id, positions) in doc_ids_and_pos {
                id_offset +=
                    write_vint(&mut target_postings, (*doc_id - prev_doc_id) as u64)? as u64;
                id_offset +=
                    write_vint(&mut target_postings, (pos_offset - prev_pos_offset) as u64)? as u64;
                prev_pos_offset = pos_offset;
                pos_offset += write_vint(&mut target_positions, positions.len() as u64)? as u64;
                let mut last_pos = 0u64;
                for pos in positions {
                    pos_offset +=
                        write_vint(&mut target_positions, (*pos - last_pos) as u64)? as u64;
                    last_pos = *pos;
                }
                prev_doc_id = *doc_id;
            }
        }
        target_positions.flush()?;
        target_postings.flush()?;
        target_terms.finish()?;
        Ok(())
    }
}

impl Feature for StringPosIndex {
    fn as_any(&self) -> &Any {
        self
    }

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

    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error> {
        self.write_docs(address, docs)
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let mut sources: Vec<(u64, Map, BufReader<File>, BufReader<File>)> =
            Vec::with_capacity(old_segments.len());
        for (old_address, old_info) in old_segments {
            sources.push((
                old_info.doc_count,
                unsafe { Map::from_path(old_address.with_ending(&TERM_ID_LISTING))? },
                BufReader::new(File::open(old_address.with_ending(ID_DOC_LISTING))?),
                BufReader::new(File::open(old_address.with_ending("pos"))?),
            ))
        }
        let target = (
            MapBuilder::new(BufWriter::new(File::create(
                new_segment.with_ending(&TERM_ID_LISTING),
            )?))?,
            BufWriter::new(File::create(new_segment.with_ending(&ID_DOC_LISTING))?),
            BufWriter::new(File::create(new_segment.with_ending("pos"))?),
        );
        do_merge(&mut sources, target)?;
        Ok(())
    }

    fn reader(&self, address: &FeatureAddress) -> Result<Box<FeatureReader>, Error> {
        let path = address.with_ending(TERM_ID_LISTING);
        if path.exists() {
            Ok(Box::new({
                StringPosIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: Some(unsafe { Map::from_path(path)? }),
                }
            }))
        } else {
            Ok(Box::new({
                StringPosIndexReader {
                    feature: self.clone(),
                    address: address.clone(),
                    map: None,
                }
            }))
        }
    }
}

pub struct StringPosIndexReader {
    pub feature: StringPosIndex,
    pub address: FeatureAddress,
    pub map: Option<Map>,
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
                    doc_file: iddoc,
                    pos_file: pos,
                    current_doc_id: 0,
                    current_pos_offset: 0,
                    current_pos: 0,
                    pos_left: 0,
                    finished: false,
                    new_pos_offset: false,
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
    doc_file: BufReader<File>,
    pos_file: BufReader<File>,
    current_doc_id: DocId,
    current_pos_offset: u64,
    current_pos: u64,
    pos_left: u64,
    finished: bool,
    new_pos_offset: bool,
    left: u64,
}

impl DocIter for TermDocSpansIter {
    fn current_doc(&self) -> Option<DocId> {
        if self.finished {
            None
        } else {
            Some(self.current_doc_id)
        }
    }

    fn next_doc(&mut self) -> Result<Option<DocId>, Error> {
        if self.left != 0 {
            self.left -= 1;
            let doc_diff = read_vint(&mut self.doc_file)?;
            self.current_doc_id += doc_diff;
            let pos_offset_diff = read_vint(&mut self.doc_file)?;
            self.current_pos_offset += pos_offset_diff;
            self.new_pos_offset = true;
            Ok(Some(self.current_doc_id))
        } else {
            self.finished = true;
            Ok(None)
        }
    }
}

impl DocSpansIter for TermDocSpansIter {
    fn next_start_pos(&mut self) -> Result<Option<Position>, Error> {
        if self.new_pos_offset {
            self.new_pos_offset = false;
            self.pos_file
                .seek(SeekFrom::Start(self.current_pos_offset))?;
            self.pos_left = read_vint(&mut self.pos_file)?;
            self.current_pos = 0;
        }
        if self.pos_left == 0 {
            return Ok(None);
        }
        let pos_diff = read_vint(&mut self.pos_file)?;
        self.current_pos += pos_diff;
        self.pos_left -= 1;
        return Ok(Some(self.current_pos));
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

fn do_merge<R, W>(
    sources: &mut [(u64, Map, R, R)],
    target: (MapBuilder<W>, W, W),
) -> Result<(), Error>
where
    W: Write + Seek + Sized,
    R: Read + Seek + Sized,
{
    let (mut term_builder, mut target_postings, mut target_positions) = target;
    let (ref mut new_offsets, ref mut union, ref mut source_postings, ref mut source_positions) = {
        let mut new_offset = 0u64;
        let mut new_offsets = Vec::with_capacity(sources.len());

        let mut op_builder = OpBuilder::new();
        let mut source_postings = Vec::new();
        let mut source_positions = Vec::new();
        for (doc_count, source_terms, source_posting, source_position) in sources.into_iter() {
            op_builder.push(source_terms.stream());
            new_offsets.push(new_offset);
            new_offset += *doc_count;
            source_postings.push(source_posting);
            source_positions.push(source_position);
        }
        (
            new_offsets,
            op_builder.union(),
            source_postings,
            source_positions,
        )
    };

    let mut postings_offset = 0u64;
    let mut positions_offset = 0u64;
    while let Some((term, term_offsets)) = union.next() {
        let mut sorted_offsets = term_offsets.to_vec();
        sorted_offsets.sort_by_key(|o| o.index);
        term_builder.insert(term, postings_offset)?;

        let mut term_doc_counts: Vec<u64> = vec![0; source_postings.len()];
        for term_offset in &sorted_offsets {
            let mut source_posting = source_postings.get_mut(term_offset.index).unwrap();
            source_posting.seek(SeekFrom::Start(term_offset.value as u64))?;
            term_doc_counts[term_offset.index] = read_vint(&mut source_posting)?;
        }

        let term_doc_count: u64 = term_doc_counts.iter().sum();
        postings_offset += write_vint(&mut target_postings, term_doc_count)? as u64;
        let mut last_written_doc_id = 0u64;
        let mut last_written_pos_offset = 0u64;
        for term_offset in &sorted_offsets {
            let mut source_posting = source_postings.get_mut(term_offset.index).unwrap();
            let mut source_position = source_positions.get_mut(term_offset.index).unwrap();
            let mut last_read_doc_id = 0u64;
            let mut last_read_pos_offset = 0u64;
            for _i in 0..term_doc_counts[term_offset.index] {
                let doc_diff = read_vint(&mut source_posting)?;
                let read_doc_id = last_read_doc_id + doc_diff;
                let doc_id_to_write = new_offsets[term_offset.index] + read_doc_id;
                let diff_to_write = doc_id_to_write - last_written_doc_id;
                postings_offset += write_vint(&mut target_postings, diff_to_write)? as u64;
                last_read_doc_id = read_doc_id;
                last_written_doc_id = doc_id_to_write;

                let positions_offset_diff = read_vint(&mut source_posting)?;
                let read_position_offset = positions_offset_diff + last_read_pos_offset;

                source_position.seek(SeekFrom::Start(read_position_offset))?;
                let pos_offset_to_write = positions_offset - last_written_pos_offset;
                postings_offset += write_vint(&mut target_postings, pos_offset_to_write)? as u64;
                last_read_pos_offset = read_position_offset;
                last_written_pos_offset = positions_offset;


                let num_positions = read_vint(source_position)?;
                positions_offset += write_vint(&mut target_positions, num_positions)? as u64;
                for _j in 0..num_positions {
                    let pos_diff = read_vint(source_position)?;
                    positions_offset += write_vint(&mut target_positions, pos_diff)? as u64;
                }
            }
        }
    }
    term_builder.finish()?;
    target_postings.flush()?;
    target_positions.flush()?;
    Ok(())
}
