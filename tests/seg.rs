extern crate esmy;
/*
#[macro_use]
extern crate quickcheck;

#[cfg(test)]
mod tests {
    use esmy::analyzis::NoopAnalyzer;
    use esmy::seg::Doc;
    use esmy::seg::FullDoc;
    use esmy::seg::{self, StringIndex, StringValues};
    use quickcheck::TestResult;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::panic;
    use std::path::Path;
    use std::str::from_utf8;
    use std::sync::{Once, ONCE_INIT};

    quickcheck! {

        enum IndexOperation{
            Index(Vec<Doc>),
            Commit,
            Merge
        }

        fn finds_merged(ops: Vec<IndexOperation>)-> TestResult {
            let index_path = env::current_dir().expect("failed to get current dir").join(&Path::new("tmp/tests/index"));
            if index_path.exists() {
                fs::remove_dir_all(&index_path).expect("could not delete directory for test");
            }
            let features : Vec<Box<seg::Feature>> = vec![
                Box::new(StringIndex::new("value", Box::from(NoopAnalyzer{}))),
                Box::new(FullDoc::new()),
            ];
            let index = seg::Index::new(seg::SegmentSchema{features}, &index_path);
            let mut builder = index.new_segment();
            for op in ops {
                match op {
                    IndexOperation::Index(docs) => {
                        for doc in docs{
                            builder.add_doc(doc);
                        }
                    },
                    IndexOperation::Commit => {
                        builder.commit().expect("Failed to commit segment");
                    },
                    IndexOperation::Merge => {
                        index.merge(&index.list_segments()).expect("Could not merge segments");
                    }
                }
            }
            return TestResult::passed();
        }
    }
};*/
