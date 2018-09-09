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

trait Offsets {
    fn new(file_offset: u64, block_offset: u64) -> Self;
    fn file_offset(&self) -> u64;
    fn block_offset(&self) -> u64;
}

impl Offsets for u64 {
    fn new(file_offset: u64, block_offset: u64) -> Self {
        (file_offset << 12) + block_offset
    }

    fn file_offset(&self) -> u64 {
        self >> 12
    }
    fn block_offset(&self) -> u64 {
        self & 0xFFF
    }
}

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
        let mut file_offset = 0u64;
        let mut doc_offsets = BufWriter::new(File::create(address.with_ending("fdo"))?);
        let mut doc_buf_writer = File::create(address.with_ending("fdv"))?;
        let mut writer =
            ::flate2::write::GzEncoder::new(doc_buf_writer, ::flate2::Compression::default());
        let mut block_offset = 0;
        for doc in docs {
            doc_offsets.write_u64::<BigEndian>(Offsets::new(file_offset, block_offset))?;
            doc.serialize(&mut rmps::Serializer::new(&mut writer))
                .unwrap();
            block_offset += 1;
            if block_offset % 4096 == 0 {
                doc_buf_writer = writer.finish()?;
                doc_buf_writer.flush()?;
                file_offset = doc_buf_writer.seek(SeekFrom::Current(0))?;
                writer = ::flate2::write::GzEncoder::new(
                    doc_buf_writer,
                    ::flate2::Compression::default(),
                );
                block_offset = 0;
            }
        }
        doc_buf_writer = writer.finish()?;
        doc_buf_writer.flush()?;
        doc_offsets.flush()?;
        Ok(())
    }

    fn reader<'b>(&self, address: &FeatureAddress) -> Result<Box<FeatureReader>, Error> {
        Ok(Box::new(FullDocReader {
            address: address.clone(),
        }))
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
                        target_val_offset_file.write_u64::<BigEndian>(Offsets::new(
                            base_offset + source_offset.file_offset(),
                            source_offset.block_offset(),
                        ))?;
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
    pub fn cursor(&self) -> Result<FullDocCursor, Error> {
        FullDocCursor::open(&self.address)
    }
}

pub struct FullDocCursor {
    curr_block: u64,
    next_doc: u64,
    offsets_file: File,
    deserializer: Option<
        ::rmps::Deserializer<
            ::rmps::decode::ReadReader<::flate2::bufread::GzDecoder<BufReader<File>>>,
        >,
    >,
}

impl FullDocCursor {
    pub fn open(address: &FeatureAddress) -> Result<FullDocCursor, Error> {
        Ok(FullDocCursor {
            curr_block: 0,
            next_doc: 0,
            offsets_file: File::open(address.with_ending("fdo"))?,
            deserializer: Some(::rmps::Deserializer::new(
                ::flate2::bufread::GzDecoder::new(BufReader::new(File::open(
                    address.with_ending("fdv"),
                )?)),
            )),
        })
    }

    pub fn read_doc(&mut self, docid: u64) -> Result<Doc, Error> {
        self.offsets_file.seek(SeekFrom::Start(docid * 8))?;
        let offsets = self.offsets_file.read_u64::<BigEndian>()?;
        if offsets.file_offset() != self.curr_block {
            let old_deser = ::std::mem::replace(&mut self.deserializer, None);
            let mut file = old_deser.unwrap().into_inner().into_inner().into_inner();
            file.seek(SeekFrom::Start(offsets.file_offset()))?;
            self.deserializer = Some(::rmps::Deserializer::new(
                ::flate2::bufread::GzDecoder::new(BufReader::new(file)),
            ));
            self.next_doc = 0;
            self.curr_block = offsets.file_offset();
        }
        if self.next_doc > offsets.block_offset() {
            //TODO panic is kind of weird
            panic!("NOT VALID")
        }
        match self.deserializer {
            Some(ref mut deser) => {
                while self.next_doc < offsets.block_offset() {
                    let _: Doc = ::serde::Deserialize::deserialize(&mut *deser).unwrap();
                    self.next_doc += 1;
                }
                let ret: Doc = ::serde::Deserialize::deserialize(&mut *deser).unwrap();
                self.next_doc += 1;
                Ok(ret)
            }
            None => panic!(),
        }
    }
}
