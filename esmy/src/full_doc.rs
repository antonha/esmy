use std::any::Any;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use bit_vec::BitVec;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use lz4::Decoder;
use lz4::EncoderBuilder;
use rmps;
use serde::Serialize;

use doc::Doc;
use error::Error;
use seg::Feature;
use seg::FeatureAddress;
use seg::FeatureConfig;
use seg::FeatureReader;
use seg::SegmentInfo;
use lz4::Encoder;

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

#[derive(Clone, Default)]
pub struct FullDoc {
    level: Option<u32>,
}

impl FullDoc {
    pub fn new() -> FullDoc {
        FullDoc {
            level: None
        }
    }

    pub fn with_compression_level(compression_level: u32) -> FullDoc {
        FullDoc {
            level: Some(compression_level)
        }
    }

    fn create_encoder<W: Write>(&self, writer: W) -> Result<Encoder<W>, Error> {
        match self.level {
            Some(level) => Ok(EncoderBuilder::new().level(level).build(writer)?),
            None => Ok(EncoderBuilder::new().build(writer)?)
        }
    }
}

impl Feature for FullDoc {
    fn feature_type(&self) -> &'static str {
        "full_doc"
    }

    fn from_config(config: FeatureConfig) -> Self {
        let level = config
            .int_at("compression_level")
            .map(|n| n as u32);
        FullDoc { level }
    }

    fn to_config(&self) -> FeatureConfig {
        let mut map = HashMap::new();
        if let Some(level) = self.level {
            map.insert(
                "compression_level".to_string(),
                FeatureConfig::Int(level as i64),
            );
        }
        FeatureConfig::Map(map)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn write_segment(&self, address: &FeatureAddress, docs: &[Doc]) -> Result<(), Error> {
        let mut block_file_offset = 0u64;
        let mut doc_offsets = BufWriter::new(File::create(address.with_ending("fdo"))?);
        let doc_buf_writer = BufWriter::new(File::create(address.with_ending("fdv"))?);
        let mut encoder = self.create_encoder(doc_buf_writer)?;
        let mut block_offset = 0;
        for doc in docs {
            doc_offsets.write_u64::<BigEndian>(Offsets::new(block_file_offset, block_offset))?;
            doc.serialize(&mut rmps::Serializer::new(&mut encoder))
                .unwrap();
            block_offset += 1;
            //TODO don't write larger
            if block_offset % 4096 == 0 {
                let (mut doc_buf_writer, res) = encoder.finish();
                //Force get result of writing
                res?;
                doc_buf_writer.flush()?;
                block_file_offset = doc_buf_writer.seek(SeekFrom::Current(0))?;
                encoder = EncoderBuilder::new().build(doc_buf_writer)?;
                block_offset = 0;
            }
        }
        let (mut doc_buf_writer, res) = encoder.finish();
        res?;
        doc_buf_writer.flush()?;
        doc_offsets.flush()?;
        Ok(())
    }

    fn reader(&self, address: &FeatureAddress) -> Result<Box<dyn FeatureReader>, Error> {
        Ok(Box::new(FullDocReader {
            address: address.clone(),
        }))
    }

    fn merge_segments(
        &self,
        old_segments: &[(FeatureAddress, SegmentInfo, BitVec)],
        new_segment: &FeatureAddress,
    ) -> Result<(), Error> {
        let target_offset_path = new_segment.with_ending("fdo");
        let mut target_val_offset_file = BufWriter::new(File::create(&target_offset_path)?);
        let target_value_path = new_segment.with_ending("fdv");
        let mut target_val_file = BufWriter::new(File::create(&target_value_path)?);
        let mut base_offset = 0u64;
        let mut has_written = false;
        for (feature_address, info, deleted_docs) in old_segments.iter() {
            let source_offset_path = feature_address.with_ending("fdo");
            if source_offset_path.exists() {
                if !deleted_docs.iter().find(|b| *b).is_some() {
                    has_written = true;
                    let mut source_val_offset_file =
                        BufReader::new(File::open(source_offset_path)?);
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
                } else {
                    let reader = FullDocReader {
                        address: feature_address.clone(),
                    };
                    //Know that we can unwrap since offsets file exists
                    let mut cursor = reader.cursor()?.unwrap();
                    let mut file_offset = target_val_file.seek(SeekFrom::Current(0))?;
                    let mut encoder = self.create_encoder(target_val_file)?;
                    let mut block_offset = 0;
                    for doc_id in 0..info.doc_count {
                        if !deleted_docs.get(doc_id as usize).unwrap() {
                            has_written = true;
                            let doc = cursor.read_doc(doc_id)?;
                            target_val_offset_file
                                .write_u64::<BigEndian>(Offsets::new(file_offset, block_offset))?;
                            doc.serialize(&mut rmps::Serializer::new(&mut encoder))
                                .unwrap();
                            block_offset += 1;
                            //TODO max size
                            if false || block_offset % 4096 == 0 {
                                let (tf, res) = encoder.finish();
                                res?;
                                target_val_file = tf;
                                target_val_file.flush()?;
                                file_offset = target_val_file.seek(SeekFrom::Current(0))?;
                                encoder = self.create_encoder(target_val_file)?;
                                block_offset = 0;
                            }
                        }
                    }
                    let (tf, res) = encoder.finish();
                    res?;
                    target_val_file = tf;
                    target_val_file.flush()?;
                    base_offset = target_val_file.seek(SeekFrom::Current(0))?;
                }
            }
        }
        target_val_file.flush()?;
        target_val_offset_file.flush()?;
        if !has_written {
            ::std::fs::remove_file(target_offset_path)?;
            ::std::fs::remove_file(target_value_path)?;
        }
        Ok(())
    }
}

pub struct FullDocReader {
    address: FeatureAddress,
}

impl FeatureReader for FullDocReader {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FullDocReader {
    pub fn cursor(&self) -> Result<Option<FullDocCursor>, Error> {
        FullDocCursor::open(&self.address)
    }
}

pub struct FullDocCursor {
    curr_block: u64,
    next_doc: u64,
    offsets_file: File,
    deserializer: Option<
        ::rmps::Deserializer<
            ::rmps::decode::ReadReader<Decoder<BufReader<File>>>,
        >,
    >,
}

impl FullDocCursor {
    pub fn open(address: &FeatureAddress) -> Result<Option<FullDocCursor>, Error> {
        let source_offset_path = address.with_ending("fdo");
        if source_offset_path.exists() {
            Ok(Some(FullDocCursor {
                curr_block: 0,
                next_doc: 0,
                offsets_file: File::open(source_offset_path)?,
                deserializer: Some(::rmps::Deserializer::new(
                    Decoder::new(BufReader::new(File::open(
                        address.with_ending("fdv"),
                    )?))?,
                )),
            }))
        } else {
            Ok(None)
        }
    }

    pub fn read_doc(&mut self, docid: u64) -> Result<Doc, Error> {
        self.offsets_file.seek(SeekFrom::Start(docid * 8))?;
        let offsets = self.offsets_file.read_u64::<BigEndian>()?;
        if offsets.file_offset() != self.curr_block {
            let old_deser = ::std::mem::replace(&mut self.deserializer, None);
            //Ignore error on purpose, only errors if we have not read all things in stream
            let (buf_reader, _res) = old_deser.unwrap().into_inner().finish();

            let mut file = buf_reader.into_inner();
            file.seek(SeekFrom::Start(offsets.file_offset()))?;
            self.deserializer = Some(::rmps::Deserializer::new(
                Decoder::new(BufReader::new(file))?,
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
