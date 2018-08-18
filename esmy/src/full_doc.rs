use seg::Feature;
use std::any::Any;
use seg::FeatureConfig;
use std::collections::HashMap;
use seg::SegmentAddress;
use std::io::BufWriter;
use std::io::SeekFrom;
use std::io::Seek;
use std::io::Write;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use doc::Doc;
use seg::FeatureReader;
use seg::SegmentInfo;
use serde::Serialize;
use error::Error;
use std::io::BufReader;
use std::io;
use rmps;

#[derive(Clone)]
pub struct FullDoc {}

impl FullDoc {
    pub fn new() -> FullDoc {
        FullDoc {}
    }
}

impl Feature for FullDoc {
    fn as_any(&self) -> &Any {
        self
    }

    fn feature_type(&self) -> &'static str {
        "full_doc"
    }

    fn from_config(_config: FeatureConfig) -> Self {
        FullDoc {}
    }

    fn to_config(&self) -> FeatureConfig {
        FeatureConfig::Map(HashMap::new())
    }

    fn write_segment(&self, address: &SegmentAddress, docs: &[Doc]) -> Result<(), Error> {
        let mut offset: u64;
        let mut doc_offsets = BufWriter::new(address.create_file("fdo")?);
        let mut docs_packed = address.create_file("fdv")?;
        for doc in docs {
            offset = docs_packed.seek(SeekFrom::Current(0))?;
            doc_offsets.write_u64::<BigEndian>(offset)?;
            doc.serialize(&mut rmps::Serializer::new(&docs_packed))
                .unwrap();
        }
        doc_offsets.flush()?;
        docs_packed.flush()?;
        Ok(())
    }

    fn reader<'b>(&self, address: SegmentAddress) -> Box<FeatureReader> {
        Box::new({ FullDocReader { address } })
    }

    fn merge_segments(
        &self,
        old_segments: &[SegmentInfo],
        new_segment: &SegmentAddress,
    ) -> Result<(), Error> {
        let mut target_val_offset_file = BufWriter::new(new_segment.create_file("fdo")?);
        let mut target_val_file = new_segment.create_file("fdv")?;
        let mut base_offset = 0u64;
        for segment in old_segments.iter() {
            let mut source_val_offset_file = BufReader::new(segment.address.open_file("fdo")?);
            loop {
                match source_val_offset_file.read_u64::<BigEndian>() {
                    Ok(source_offset) => {
                        target_val_offset_file
                            .write_u64::<BigEndian>(base_offset + source_offset)?;
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            return Err(Error::IOError);
                        }
                        break;
                    }
                }
            }
            let mut source_val_file = segment.address.open_file("fdv")?;
            io::copy(&mut source_val_file, &mut target_val_file)?;
            base_offset = target_val_file.seek(SeekFrom::Current(0))?;
        }
        target_val_file.flush()?;
        target_val_offset_file.flush()?;
        Ok(())
    }
}

pub struct FullDocReader {
    address: SegmentAddress,
}

impl FeatureReader for FullDocReader {
    fn as_any(&self) -> &Any {
        self
    }
}

impl FullDocReader {
    pub fn read_doc(&self, docid: u64) -> Result<Doc, Error> {
        let mut offsets_file = self.address.open_file("fdo")?;
        let mut values_file = self.address.open_file("fdv")?;
        offsets_file.seek(SeekFrom::Start(docid * 8))?;
        let offset = offsets_file.read_u64::<BigEndian>()?;
        values_file.seek(SeekFrom::Start(offset))?;
        Ok(rmps::from_read(values_file).unwrap())
    }
}


