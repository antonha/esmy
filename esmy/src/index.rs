use super::Error;
use doc::Doc;
use rand;
use rand::Rng;
use rmps;
use seg;
use seg::write_seg;
use seg::FeatureMeta;
use seg::SegmentSchema;
use seg::{SegmentAddress, SegmentInfo, SegmentReader};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::mem;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread::sleep;
use std::time::Duration;
use threadpool::ThreadPool;
use walkdir::WalkDir;

pub struct Index {
    options: IndexOptions,
    schema_template: SegmentSchema,
    path: PathBuf,
    state: Arc<RwLock<IndexState>>,
    pool: ThreadPool,
}

struct IndexState {
    pub docs_to_index: Vec<Doc>,
    pub active_segments: HashMap<SegmentAddress, Arc<SegRef>>,
    pub waiting_merge: HashSet<SegmentAddress>,
}

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

struct IndexOptions {
    auto_commit: bool,
    auto_merge: bool,
}

pub struct IndexBuilder {
    options: IndexOptions,
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

    pub fn open(self, path: PathBuf) -> Result<Index, Error> {
        Index::open_with_options(path, self.options)
    }

    pub fn create(self, path: PathBuf, schema_template: SegmentSchema) -> Result<Index, Error> {
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
    Ok(rmps::encode::write(&mut file, meta)?)
}

impl Index {
    pub fn open(path: PathBuf) -> Result<Index, Error> {
        IndexBuilder::new().open(path)
    }

    fn open_with_options(path: PathBuf, options: IndexOptions) -> Result<Index, Error> {
        let mut segments = HashMap::new();
        for segment_address in Index::list_segments(&path) {
            segments.insert(
                segment_address.clone(),
                Arc::from(SegRef::new(segment_address.read_info()?)),
            );
        }
        let schema = seg::schema_from_metas(read_index_meta(&path).unwrap().feature_template_metas);
        Ok(Index {
            options,
            schema_template: schema,
            path,
            state: Arc::new(RwLock::new(IndexState {
                docs_to_index: Vec::new(),
                active_segments: segments,
                waiting_merge: HashSet::new(),
            })),
            pool: ThreadPool::new(4),
        })
    }

    fn create_with_options(
        path: PathBuf,
        schema_template: SegmentSchema,
        options: IndexOptions,
    ) -> Result<Index, Error> {
        write_index_meta(
            &path,
            &IndexMeta {
                feature_template_metas: seg::schema_to_feature_metas(&schema_template),
            },
        )?;
        Ok(Index {
            options,
            schema_template,
            path,
            state: Arc::new(RwLock::new(IndexState {
                docs_to_index: Vec::new(),
                active_segments: HashMap::new(),
                waiting_merge: HashSet::new(),
            })),
            pool: ThreadPool::new(4),
        })
    }

    pub fn add_doc(&self, doc: Doc) {
        self.wait_jobs(50);
        //TODO long-term goal here is to add to some transaction log instead of just adding to in-memory
        {
            let mut local_state = self.state.write().unwrap();
            local_state.docs_to_index.push(doc);
            if self.options.auto_commit && local_state.docs_to_index.len() > 10000 {
                let to_commit = mem::replace(&mut local_state.docs_to_index, Vec::new());
                let schema = self.schema_template.clone();
                let address = self.new_segment_address();
                let state = self.state.clone();
                self.pool.execute(move || {
                    write_seg(&schema, &address, &to_commit).unwrap();
                    let mut local_state = state.write().unwrap();
                    local_state.active_segments.insert(
                        address.clone(),
                        Arc::new(SegRef::new(address.read_info().unwrap())),
                    );
                });
                if self.options.auto_merge {
                    self.submit_merges(&mut local_state);
                }
            }
        }
    }

    pub fn commit(&self) -> Result<(), Error> {
        {
            self.wait_jobs(0);
            let mut local_state = self.state.write().unwrap();
            if !local_state.docs_to_index.is_empty() {
                let address = self.new_segment_address();
                write_seg(&self.schema_template, &address, &local_state.docs_to_index)?;
                local_state.active_segments.insert(
                    address.clone(),
                    Arc::new(SegRef::new(address.read_info().unwrap())),
                );
                local_state.docs_to_index = Vec::new();
                if self.options.auto_merge {
                    self.submit_merges(&mut local_state);
                }
            }
        }
        self.wait_jobs(0);
        Ok(())
    }

    fn new_segment_address(&self) -> SegmentAddress {
        let name: String = rand::thread_rng().gen_ascii_chars().take(10).collect();
        SegmentAddress {
            path: PathBuf::from(&self.path),
            name,
        }
    }

    fn list_segments(path: &PathBuf) -> Vec<SegmentAddress> {
        let walker = WalkDir::new(path).min_depth(1).max_depth(1).into_iter();
        let entries = walker.filter_entry(|e| {
            e.file_type().is_dir() || e
                .file_name()
                .to_str()
                .map(|s| s.ends_with(".seg"))
                .unwrap_or(false)
        });
        entries
            .map(|e| {
                let name = String::from(
                    e.unwrap()
                        .file_name()
                        .to_str()
                        .unwrap()
                        .split(".")
                        .next()
                        .unwrap(),
                );
                SegmentAddress {
                    path: PathBuf::from(path),
                    name,
                }
            }).collect::<Vec<SegmentAddress>>()
    }

    pub fn merge(&self) {
        self.wait_jobs(0);
        let mut local_state = self.state.write().unwrap();
        self.submit_merges(&mut local_state);
        self.wait_jobs(0)
    }

    fn submit_merges(&self, state: &mut IndexState) {
        let infos: Vec<&SegmentInfo> = state
            .active_segments
            .values()
            .filter(|info| !state.waiting_merge.contains(&info.info.address))
            .map(|item| &item.info)
            .collect();
        for segments in find_merges(infos).to_merge {
            for seg in segments.iter() {
                state.waiting_merge.insert(seg.address.clone());
            }
            println!(
                "Merging {} segments with {} docs.",
                segments.len(),
                segments.iter().map(|i| i.doc_count).sum::<u64>()
            );
            let seg_cloned: Vec<SegmentAddress> =
                segments.iter().map(|info| info.address.clone()).collect();
            let state = self.state.clone();
            let schema = self.schema_template.clone();
            let new_address = self.new_segment_address();
            self.pool.execute(move || {
                let addresses_to_merge: Vec<&SegmentAddress> =
                    seg_cloned.iter().map(|address| address).collect();
                seg::merge(&schema, &new_address, &addresses_to_merge).unwrap();
                let mut local_state = state.write().unwrap();
                for old_segment in seg_cloned.iter() {
                    //TODO inefficient iteration
                    if let Some(old_ref) = local_state.active_segments.remove(old_segment) {
                        old_ref.delete_on_drop.store(true, atomic::Ordering::SeqCst)
                    }
                    local_state.waiting_merge.remove(old_segment);
                }
                let new_info = Arc::new(SegRef::new(new_address.read_info().unwrap()));
                local_state.active_segments.insert(new_address, new_info);
            });
        }
    }

    pub fn wait_jobs(&self, num: usize) {
        while self.pool.active_count() + self.pool.queued_count() > num {
            sleep(Duration::from_millis(100));
        }
    }

    pub fn open_reader(&self) -> ManagedIndexReader {
        let guard = self.state.read().unwrap();
        let mut readers = Vec::new();
        for seg_ref in guard.active_segments.values() {
            readers.push(SegmentReader::new(seg_ref.info.clone()));
        }
        ManagedIndexReader {
            _segment_refs: guard.active_segments.values().cloned().collect(),
            readers,
        }
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
}

struct MergeSpec<'a> {
    to_merge: Vec<Vec<&'a SegmentInfo>>,
}

fn find_merges(mut segments_in: Vec<&SegmentInfo>) -> MergeSpec {
    segments_in.sort_by(|a, b| a.doc_count.cmp(&b.doc_count).reverse());
    let mut queue = ::std::collections::VecDeque::from(segments_in);
    let mut to_merge: Vec<Vec<&SegmentInfo>> = Vec::new();
    while let Some(first) = queue.pop_front() {
        let mut stage = Vec::new();
        loop {
            if !queue.is_empty() {
                let should_drop = {
                    let info = queue.front().unwrap();
                    info.doc_count as f64 > first.doc_count as f64 * 0.6
                };
                if should_drop {
                    stage.push(queue.pop_front().unwrap())
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        if stage.len() > 10 {
            to_merge.push(stage);
        }
    }
    return MergeSpec { to_merge };
}