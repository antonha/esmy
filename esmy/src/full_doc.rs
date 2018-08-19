use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use doc::Doc;
use error::Error;
use rmps;
use seg::Feature;
use seg::FeatureAddress;
use seg::FeatureConfig;
use seg::FeatureReader;
use seg::SegmentInfo;
use serde::Serialize;
use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

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

    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error> {
        let mut offset: u64;
        let mut doc_offsets = BufWriter::new(File::create(address.with_ending("fdo"))?);
        let mut docs_packed = File::create(address.with_ending("fdv"))?;
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

    fn reader<'b>(&self, address: &FeatureAddress) -> Box<FeatureReader> {
        Box::new(FullDocReader {
            address: address.clone(),
        })
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let mut target_val_offset_file =
            BufWriter::new(File::create(new_segment.with_ending("fdo"))?);
        let mut target_val_file = File::create(new_segment.with_ending("fdv"))?;
        let mut base_offset = 0u64;
        for (feature_address, _old_info) in old_segments.iter() {
            let mut source_val_offset_file =
                BufReader::new(File::open(feature_address.with_ending("fdo"))?);
            loop {
                match source_val_offset_file.read_u64::<BigEndian>() {
                    Ok(source_offset) => {
                        target_val_offset_file
                            .write_u64::<BigEndian>(base_offset + source_offset)?;
                    }
                    Err(error) => {
                        if error.kind() != io::ErrorKind::UnexpectedEof {
                            return Err(Error::IOError(error));
                        }
                        break;
                    }
                }
            }
            let mut source_val_file = File::open(feature_address.with_ending(&"fdv"))?;
            io::copy(&mut source_val_file, &mut target_val_file)?;
            base_offset = target_val_file.seek(SeekFrom::Current(0))?;
        }
        target_val_file.flush()?;
        target_val_offset_file.flush()?;
        Ok(())
    }
}

pub struct FullDocReader {
    address: FeatureAddress,
}

impl FeatureReader for FullDocReader {
    fn as_any(&self) -> &Any {
        self
    }
}

impl FullDocReader {
    pub fn read_doc(&self, docid: u64) -> Result<Doc, Error> {
        let mut offsets_file = File::open(self.address.with_ending("fdo"))?;
        let mut values_file = File::open(self.address.with_ending("fdv"))?;
        offsets_file.seek(SeekFrom::Start(docid * 8))?;
        let offset = offsets_file.read_u64::<BigEndian>()?;
        values_file.seek(SeekFrom::Start(offset))?;
        Ok(rmps::from_read(values_file).unwrap())
    }
}
