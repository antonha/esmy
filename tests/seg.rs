extern crate esmy;

#[cfg(test)]
#[macro_use]
extern crate proptest;

#[cfg(test)]
mod tests {
    use esmy::analyzis::NoopAnalyzer;
    use esmy::seg::{self, StringIndex};
    use esmy::seg::Doc;
    use esmy::seg::FieldValue;
    use esmy::seg::FullDoc;
    use proptest::collection::hash_map;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use std::env;
    use std::fs;
    use std::path::Path;

    #[derive(Debug, Clone)]
    enum IndexOperation {
        Index(Vec<Doc>),
        Commit,
        Merge,
    }

    fn arb_fieldvalue() -> BoxedStrategy<FieldValue> {
        prop_oneof![".+".prop_map(FieldValue::String),].boxed()
    }

    fn arb_fieldname() -> BoxedStrategy<String> {
        prop_oneof![
            Just("field1".to_owned()),
            Just("field2".to_owned())
        ].boxed()
    }

    fn arb_doc() -> BoxedStrategy<Doc> {
        hash_map(arb_fieldname(), arb_fieldvalue(), 1..10).boxed()
    }

    fn arb_index_op() -> BoxedStrategy<IndexOperation> {
        prop_oneof![
            vec(arb_doc(), 1..100).prop_map(IndexOperation::Index),
            Just(IndexOperation::Commit),
            Just(IndexOperation::Merge)
        ].boxed()
    }

    proptest! {

        #[test]
        fn finds_merged(ref ops  in vec(arb_index_op(), 10)) {
            let index_path = env::current_dir().expect("failed to get current dir").join(&Path::new("tmp/tests/index"));
            if index_path.exists() {
                fs::remove_dir_all(&index_path).expect("could not delete directory for test");
            }
            fs::create_dir_all(&index_path);
            let features : Vec<Box<seg::Feature>> = vec![
                //Box::new(StringIndex::new("value", Box::from(NoopAnalyzer{}))),
                Box::new(FullDoc::new()),
            ];
            let index = seg::Index::new(seg::SegmentSchema{features}, &index_path);
            let mut builder = index.new_segment();
            for op in ops {
                match op {
                    &IndexOperation::Index(ref docs) => {
                        println!("Adding {} docs", docs.len());
                        for doc in docs{
                            builder.add_doc(doc.clone());
                        }
                    },
                    &IndexOperation::Commit => {
                        println!("Committing segment");
                        builder.commit().expect("Failed to commit segment");
                        builder = index.new_segment();
                    },
                    &IndexOperation::Merge => {
                        let segments = &index.list_segments();
                        println!("Merging {} segments", segments.len());
                        index.merge(segments).expect("Could not merge segments");
                    }
                }
            }
        }
    }
}
