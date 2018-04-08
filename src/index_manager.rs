use seg::Index;
use seg::IndexReader;
use seg::Doc;
use seg::write_seg;
use std::io::Error;
use std::thread::sleep;
use std::time::Duration;
use threadpool::ThreadPool;
use std::mem;


pub struct IndexManager{
    index: Index,
    pub docs_to_index: Vec<Doc>,
    pub pool: ThreadPool
}

impl IndexManager {

    pub fn new(index: Index) -> IndexManager{
        IndexManager{
            index,
            docs_to_index: Vec::new(),
            pool: ThreadPool::new(4) 
        }
    }

    pub fn open_reader(&self) -> IndexReader{
        self.index.open_reader()
    }

    pub fn add_doc(&mut self, doc : Doc){
        self.wait_jobs(30);
        self.docs_to_index.push(doc);
        if self.docs_to_index.len() > 10000 {
            let to_commit = mem::replace(&mut self.docs_to_index, Vec::new());
            let schema = self.index.schema_template().clone();
            let address = self.index.new_address();
            self.pool.execute(move|| {
                write_seg(&schema, &address, &to_commit).unwrap()
            });
        }
    }

    pub fn commit(&mut self) -> Result<(), Error> {
        write_seg(&self.index.schema_template(), &self.index.new_address(), &self.docs_to_index)?;
        self.docs_to_index = Vec::new();
        self.wait_jobs(0);
        Ok(())
    }

    pub fn wait_jobs(&self, num: usize) {
        while self.pool.active_count() + self.pool.queued_count() > num {
            println!(
                "Awaiting thread pool to reach. {} active {}", 
                self.pool.active_count(),
                self.pool.queued_count()
            );
            sleep(Duration::from_millis(100));
        }
    }
}



