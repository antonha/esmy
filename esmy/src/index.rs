use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::io::prelude::*;
use std::mem;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::RwLock;

use bit_vec::BitVec;
use num_cpus;
use rand;
use rand::distributions::{Alphanumeric};
use rand::Rng;
use rayon::prelude::*;
use rmps;
use walkdir::WalkDir;

use doc::Doc;
use doc_iter::DocIter;
use search;
use search::Collector;
use search::Query;
use seg;
use seg::write_seg;
use seg::FeatureMeta;
use seg::SegmentSchema;
use seg::{SegmentAddress, SegmentInfo, SegmentReader};

use super::Error;

pub struct SegRef {
    info: SegmentInfo,
    delete_on_drop: AtomicBool,
}

impl SegRef {
    fn new(info: SegmentInfo) -> SegRef {
        SegRef {
            info,
            delete_on_drop: AtomicBool::new(false),
        }
    }
}

impl AsRef<SegmentInfo> for SegRef {
    fn as_ref(&self) -> &SegmentInfo {
        &self.info
    }
}

impl Hash for SegRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.info.address.hash(state);
    }
}

impl PartialEq for SegRef {
    fn eq(&self, other: &SegRef) -> bool {
        self.info.address.eq(&other.info.address)
    }
}

impl Eq for SegRef {}

impl Drop for SegRef {
    fn drop(&mut self) {
        if self.delete_on_drop.load(atomic::Ordering::SeqCst) {
            //TODO: How to handle failure to remove files?
            self.info.address.remove_files().unwrap();
        }
    }
}

#[derive(Clone)]
struct IndexOptions {
    auto_commit: bool,
    auto_merge: bool,
}

pub struct IndexBuilder {
    options: IndexOptions,
}

impl Default for IndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexBuilder {
    pub fn new() -> IndexBuilder {
        IndexBuilder {
            options: IndexOptions {
                auto_commit: true,
                auto_merge: true,
            },
        }
    }

    pub fn auto_commit(mut self, val: bool) -> IndexBuilder {
        self.options.auto_commit = val;
        self
    }

    pub fn auto_merge(mut self, val: bool) -> IndexBuilder {
        self.options.auto_merge = val;
        self
    }

    pub fn open<P>(self, path: P) -> Result<Index, Error>
    where
        P: Into<PathBuf>,
    {
        Index::open_with_options(path, self.options)
    }

    pub fn create<P>(self, path: P, schema_template: SegmentSchema) -> Result<Index, Error>
    where
        P: Into<PathBuf>,
    {
        Index::create_with_options(path, schema_template, self.options)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexMeta {
    pub feature_template_metas: HashMap<String, FeatureMeta>,
}

pub fn read_index_meta(path: &Path) -> Result<IndexMeta, Error> {
    let file = File::open(&path.join("index_meta"))?;
    Ok(rmps::from_read(file)?)
}

pub fn write_index_meta(path: &Path, meta: &IndexMeta) -> Result<(), Error> {
    let mut file = File::create(path.join("index_meta"))?;
    rmps::encode::write(&mut file, meta)?;
    Ok(())
}

pub struct Index {
    indexer: Arc<Indexer>,
}

impl Index {
    pub fn open(path: PathBuf) -> Result<Index, Error> {
        IndexBuilder::new().open(path)
    }

    fn open_with_options<P>(path: P, options: IndexOptions) -> Result<Index, Error>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        let schema = seg::schema_from_metas(read_index_meta(&path).unwrap().feature_template_metas);
        Ok(Index {
            indexer: Indexer::start(path, schema, options)?,
        })
    }

    fn create_with_options<P>(
        path: P,
        schema: SegmentSchema,
        options: IndexOptions,
    ) -> Result<Index, Error>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();
        fs::create_dir_all(&path)?;
        write_index_meta(
            &path,
            &IndexMeta {
                feature_template_metas: seg::schema_to_feature_metas(&schema),
            },
        )?;
        Ok(Index {
            indexer: Indexer::start(path, schema, options)?,
        })
    }

    pub fn add_doc(&self, doc: Doc) -> Result<(), Error> {
        self.indexer.add_doc(doc)
    }

    pub fn commit(&self) -> Result<(), Error> {
        self.indexer.force_commit()
    }

    pub fn merge(&self) -> Result<(), Error> {
        self.indexer.merge()
    }

    pub fn force_merge(&self) -> Result<(), Error> {
        self.indexer.force_merge()
    }

    pub fn delete(&self, query: &impl Query) -> Result<(), Error> {
        self.indexer.delete(query)
    }

    pub fn open_reader(&self) -> Result<ManagedIndexReader, Error> {
        self.indexer.open_reader()
    }
}

struct Indexer {
    path: PathBuf,
    options: IndexOptions,
    schema_template: SegmentSchema,
    state: Arc<RwLock<IndexState>>,
}

lazy_static! {
    static ref NUM_CPUS: usize = num_cpus::get();
}

impl Indexer {
    fn start(
        path: PathBuf,
        schema_template: SegmentSchema,
        options: IndexOptions,
    ) -> Result<Arc<Self>, Error> {
        let state = Indexer::init_state(&path)?;
        let indexer = Arc::new(Indexer {
            path,
            options,
            schema_template,
            state: state.clone(),
        });
        Ok(indexer.clone())
    }

    fn init_state(path: &Path) -> Result<Arc<RwLock<IndexState>>, Error> {
        let mut segments = HashMap::new();
        for segment_address in Self::segments_on_disk(&path)? {
            segments.insert(
                segment_address.clone(),
                Arc::from(SegRef::new(segment_address.read_info()?)),
            );
        }
        Ok(Arc::new(RwLock::new(IndexState {
            docs_to_index: Vec::new(),
            active_segments: segments,
            waiting_merge: HashSet::new(),
        })))
    }

    fn segments_on_disk(path: &Path) -> Result<Vec<SegmentAddress>, Error> {
        let walker = WalkDir::new(path).min_depth(1).max_depth(1).into_iter();
        let entries = walker.filter_entry(|e| {
            e.file_type().is_dir()
                || e.file_name()
                    .to_str()
                    .map(|s| s.ends_with(".seg"))
                    .unwrap_or(false)
        });
        let mut addresses = Vec::new();
        for entry_res in entries {
            //TODO error handling
            let entry = entry_res.unwrap();
            let file_name = entry.file_name().to_str().unwrap();
            let segment_name = file_name.split('.').next().unwrap();
            addresses.push(SegmentAddress {
                path: PathBuf::from(path),
                name: segment_name.to_string(),
            })
        }
        Ok(addresses)
    }

    pub fn add_doc(&self, doc: Doc) -> Result<(), Error> {
        //TODO long-term goal here is to add to some transaction log instead of just adding to in-memory
        let mut local_state = self.state.write().unwrap();
        local_state.docs_to_index.push(doc);
        let should_commit = self.options.auto_commit && local_state.docs_to_index.len() >= 10_000;
        if should_commit {
            let docs = mem::replace(&mut local_state.docs_to_index, Vec::new());
            drop(local_state);
            self.do_commit(&docs)?;
        }
        Ok(())
    }

    pub fn force_commit(&self) -> Result<(), Error> {
        let docs = mem::replace(&mut self.state.write().unwrap().docs_to_index, Vec::new());
        self.do_commit(&docs)
    }

    pub fn merge(&self) -> Result<(), Error> {
        self.find_merges_and_merge(false)
    }

    pub fn force_merge(&self) -> Result<(), Error> {
        self.find_merges_and_merge(true)
    }

    pub fn delete(&self, query: &impl Query) -> Result<(), Error> {
        let reader = self.open_reader()?;
        let mut collector = DeletingCollector::new();
        reader.search(query, &mut collector)?;
        Ok(())
    }

    fn do_commit(&self, docs: &[Doc]) -> Result<(), Error> {
        if docs.is_empty() {
            return Ok(());
        }
        if docs.len() <= 1000 {
            self.try_commit(&docs)?;
        } else {
            docs.par_chunks(docs.len() / *NUM_CPUS)
                .try_for_each(|chunk| self.try_commit(&chunk))?;
        };
        if self.options.auto_merge {
            self.find_merges_and_merge(false)?;
        }
        Ok(())
    }

    fn try_commit(&self, chunk: &[Doc]) -> Result<(), Error> {
        let address = new_segment_address(&self.path);
        write_seg(&self.schema_template, &address, &chunk)?;
        self.state.write().unwrap().active_segments.insert(
            address.clone(),
            Arc::new(SegRef::new(address.read_info().unwrap())),
        );
        Ok(())
    }

    fn find_merges_and_merge(&self, force: bool) -> Result<(), Error> {
        self.items_to_merge(force)
            .par_iter()
            .try_for_each(move |segments| self.try_merge(&segments))?;
        Ok(())
    }

    fn items_to_merge(&self, force: bool) -> Vec<Vec<SegmentInfo>> {
        let mut local_state = self.state.write().unwrap();
        let infos: Vec<SegmentInfo> = local_state
            .active_segments
            .values()
            .filter(|info| !local_state.waiting_merge.contains(&info.info.address))
            .map(|item| item.info.clone())
            .collect();
        let to_merge = if force {
            vec![infos]
        } else {
            find_merges(infos).to_merge
        };
        for stage in &to_merge {
            for seg in stage {
                local_state.waiting_merge.insert(seg.address.clone());
            }
        }
        to_merge
    }

    fn try_merge(&self, segments: &[SegmentInfo]) -> Result<(), Error> {
        let new_address = new_segment_address(&self.path);
        let seg_cloned: Vec<SegmentAddress> =
            segments.iter().map(|info| info.address.clone()).collect();
        seg::merge(&self.schema_template, &new_address, &seg_cloned)?;
        let mut local_state = self.state.write().unwrap();
        for old_segment in seg_cloned.iter() {
            //TODO inefficient iteration
            if let Some(old_ref) = local_state.active_segments.remove(old_segment) {
                old_ref.delete_on_drop.store(true, atomic::Ordering::SeqCst)
            }
            local_state.waiting_merge.remove(old_segment);
        }
        let new_info = Arc::new(SegRef::new(new_address.read_info().unwrap()));
        local_state.active_segments.insert(new_address, new_info);
        Ok(())
    }

    pub fn open_reader(&self) -> Result<ManagedIndexReader, Error> {
        let guard = self.state.read().unwrap();
        let mut readers = Vec::new();
        for seg_ref in guard.active_segments.values() {
            readers.push(SegmentReader::open(seg_ref.info.clone())?);
        }
        Ok(ManagedIndexReader {
            _segment_refs: guard.active_segments.values().cloned().collect(),
            readers,
        })
    }
}

struct DeletingCollector {}

impl DeletingCollector {
    pub fn new() -> DeletingCollector {
        DeletingCollector {}
    }
}

impl Collector for DeletingCollector {
    fn collect_for(&mut self, reader: &SegmentReader, docs: &mut dyn DocIter) -> Result<(), Error> {
        let doc_count = reader.info().doc_count;
        let mut to_delete = BitVec::from_elem(doc_count as usize, false);
        while let Some(doc_id) = docs.next_doc()? {
            to_delete.set(doc_id as usize, true);
        }

        let to_write = match reader.info().address.open_file_if_exists(".del")? {
            Some(mut file) => {
                let mut buffer = Vec::with_capacity((doc_count / 8) as usize);
                file.read_to_end(&mut buffer)?;
                let mut existing = BitVec::from_bytes(&buffer);
                existing.truncate(doc_count as usize);
                existing.or(&to_delete);
                existing
            }
            None => to_delete,
        };
        let mut options = OpenOptions::new();
        options.write(true);
        options.create(true);
        let mut file = reader
            .info()
            .address
            .open_file_with_options(".del", options)?;
        file.write_all(&to_write.to_bytes())?;
        Ok(())
    }
}

impl Drop for Indexer {
    fn drop(&mut self) {
        self.force_commit().unwrap();
    }
}

struct IndexState {
    docs_to_index: Vec<Doc>,
    active_segments: HashMap<SegmentAddress, Arc<SegRef>>,
    waiting_merge: HashSet<SegmentAddress>,
}

fn new_segment_address(path: &Path) -> SegmentAddress {

    let name: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    SegmentAddress {
        path: PathBuf::from(path),
        name,
    }
}

pub struct ManagedIndexReader {
    _segment_refs: Vec<Arc<SegRef>>,
    readers: Vec<SegmentReader>,
}

impl ManagedIndexReader {
    pub fn segment_readers(&self) -> &[SegmentReader] {
        &self.readers
    }

    pub fn search(&self, query: &impl Query, collector: &mut dyn Collector) -> Result<(), Error> {
        search::search(self, query, collector)
    }
}

struct MergeSpec {
    to_merge: Vec<Vec<SegmentInfo>>,
}

fn find_merges(mut segments_in: Vec<SegmentInfo>) -> MergeSpec {
    segments_in.sort_by(|a, b| a.doc_count.cmp(&b.doc_count).reverse());
    let mut queue = ::std::collections::VecDeque::from(segments_in);
    let mut to_merge: Vec<Vec<SegmentInfo>> = Vec::new();
    while let Some(first) = queue.pop_front() {
        let mut stage = Vec::new();
        loop {
            if !queue.is_empty() {
                let is_in_stage = {
                    let info = queue.front().unwrap();
                    info.doc_count as f64 > first.doc_count as f64 * 0.6
                };
                if is_in_stage {
                    stage.push(queue.pop_front().unwrap());
                    if stage.len() > 20 {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if stage.len() >= 10 {
            to_merge.push(stage);
        }
    }
    MergeSpec { to_merge }
}
