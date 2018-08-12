use super::Error;
use seg::write_seg;
use seg::Doc;
use seg::Index;
use seg::IndexReader;
use seg::{SegmentAddress, SegmentInfo};
use std::collections::HashSet;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic;
use std::thread::sleep;
use std::time::Duration;
use threadpool::ThreadPool;

pub struct IndexManager {
    index: Index,
    state: Arc<RwLock<IndexState>>,
    pub pool: ThreadPool,
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
            delete_on_drop: AtomicBool::new(false)
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

impl IndexManager {
    pub fn open(index: Index) -> Result<IndexManager, Error> {
        let mut segments = HashMap::new();
        for segment_address in index.list_segments() {
            segments.insert(segment_address.clone(), Arc::from(SegRef::new(segment_address.read_info()?)));
        }
        Ok(IndexManager {
            index,
            state: Arc::new(RwLock::new(IndexState {
                docs_to_index: Vec::new(),
                active_segments: segments,
                waiting_merge: HashSet::new(),
            })),
            pool: ThreadPool::new(4),
        })
    }

    pub fn open_reader(&self) -> IndexReader {
        self.index.open_reader()
    }

    pub fn add_doc(&self, doc: Doc) {
        self.wait_jobs(5);
        //TODO long-term goal here is to add to some transaction log instead of just adding to in-memory
        {
            let mut local_state = self.state.write().unwrap();
            local_state.docs_to_index.push(doc);
            if local_state.docs_to_index.len() > 10000 {
                let to_commit = mem::replace(&mut local_state.docs_to_index, Vec::new());
                let schema = self.index.schema_template().clone();
                let address = self.index.new_address();
                let state = self.state.clone();
                self.pool.execute(move || {
                    write_seg(&schema, &address, &to_commit).unwrap();
                    let mut local_state = state.write().unwrap();
                    local_state
                        .active_segments
                        .insert(address.clone(), Arc::new(SegRef::new(address.read_info().unwrap())));
                });
                self.submit_merges(&mut local_state);
            }
        }
    }

    pub fn commit(&self) -> Result<(), Error> {
        {
            self.wait_jobs(0);
            let mut local_state = self.state.write().unwrap();
            let address = self.index.new_address();
            write_seg(
                &self.index.schema_template(),
                &address,
                &local_state.docs_to_index,
            )?;
            local_state
                .active_segments
                .insert(address.clone(), Arc::new(SegRef::new(address.read_info().unwrap())));
            local_state.docs_to_index = Vec::new();
            self.submit_merges(&mut local_state);
        }
        self.wait_jobs(0);
        Ok(())
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
            println!("Merging {} segments with {} docs.", segments.len(), segments.iter().map(|i|i.doc_count).sum::<u64>());
            let seg_cloned: Vec<SegmentAddress> =
                segments.iter().map(|info| info.address.clone()).collect();
            self.wait_jobs(10);
            let state = self.state.clone();
            let index = self.index.clone();
            self.pool.execute(move || {
                let addresses_to_merge: Vec<&SegmentAddress> =
                    seg_cloned.iter().map(|address| address).collect();
                let new_segment_address = index.merge(&addresses_to_merge).unwrap();
                let mut local_state = state.write().unwrap();
                for old_segment in seg_cloned.iter() {
                    //TODO inefficient
                    if let Some(old_ref) = local_state.active_segments.remove(old_segment){
                        old_ref.delete_on_drop.store(true, atomic::Ordering::SeqCst)
                    }
                    local_state.waiting_merge.remove(old_segment);
                }
                let new_info = Arc::new(SegRef::new(new_segment_address.read_info().unwrap()));
                local_state.active_segments.insert(new_segment_address, new_info);
            });
        }
    }

    pub fn wait_jobs(&self, num: usize) {
        while self.pool.active_count() + self.pool.queued_count() > num {
            sleep(Duration::from_millis(100));
        }
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
