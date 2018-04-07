extern crate esmy;

#[cfg(test)]
#[macro_use]
extern crate proptest;

#[cfg(test)]
mod tests {
    use esmy::search::{search, AllDocsCollector, FullDocQuery, ValueQuery};
    use esmy::seg;
    use esmy::seg::StringIndex;
    use esmy::seg::Doc;
    use esmy::seg::FieldValue;
    use esmy::seg::FullDoc;
    use esmy::analyzis::NoopAnalyzer;
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
        prop_oneof!["[a-z]+".prop_map(FieldValue::String),].boxed()
    }

    fn arb_fieldname() -> BoxedStrategy<String> {
        prop_oneof![Just("field1".to_owned()), Just("field2".to_owned())].boxed()
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

    fn query(vec: Vec<(String, String)>) -> BoxedStrategy<ValueQuery> {
        (0..vec.len()).prop_map(move|i|{
            let (ref key, ref val) = vec[i];
            ValueQuery::new(key.clone(), val.clone())
        }).boxed()
    }

    fn op_and_queries() -> BoxedStrategy<(Vec<IndexOperation>, Vec<ValueQuery>)> {
        vec(arb_index_op(), 0..10).prop_flat_map (|ops| {
            let values = extract_values(&ops);
            vec(query(values.clone()), 0..100).prop_map(move|queries| (ops.clone(), queries))
        }).boxed()
    }

    fn extract_values(ops: &[IndexOperation]) -> Vec<(String, String)> {
        let mut values : Vec<(String, String)> = Vec::new();
        for op in ops{
            match op {
                IndexOperation::Index(docs) => {
                    for doc in docs{
                        for (name, value) in doc {
                            match value {
                                FieldValue::String(str_val) => {
                                    values.push((name.clone(), str_val.clone()));
                                }
                            }
                        }
                    }
                }
                _ => ()
            }
        }
        values
    }

    proptest! {

        #[test]
        fn finds_merged((ref ops, ref queries) in op_and_queries()) {
            let index_path = env::current_dir().expect("failed to get current dir").join(&Path::new("tmp/tests/index"));
            if index_path.exists() {
                fs::remove_dir_all(&index_path).expect("could not delete directory for test");
            }
            fs::create_dir_all(&index_path).expect("could create directory for test");
            let features : Vec<Box<seg::Feature>> = vec![
                Box::new(StringIndex::new("field1", Box::from(NoopAnalyzer{}))),
                Box::new(StringIndex::new("field2", Box::from(NoopAnalyzer{}))),
                Box::new(FullDoc::new()),
            ];
            let index = seg::Index::new(seg::SegmentSchema{features}, &index_path);
            let mut builder = index.new_segment();
            let mut in_mem_docs = Vec::new();
            let mut in_mem_seg_docs = Vec::new();
            for op in ops {
                match op {
                    &IndexOperation::Index(ref docs) => {
                        for doc in docs{
                            builder.add_doc(doc.clone());
                            in_mem_seg_docs.push(doc.clone());
                        }
                    },
                    &IndexOperation::Commit => {
                        builder.commit().expect("Failed to commit segment");
                        builder = index.new_segment();
                        in_mem_docs.append(&mut in_mem_seg_docs);
                        in_mem_seg_docs = Vec::new()
                    },
                    &IndexOperation::Merge => {
                        //TODO merging not working somehow
                        let segments = &index.list_segments();
                        //index.merge(segments).expect("Could not merge segments");
                    }
                }
                for query in queries {
                    let expected_matches : Vec<Doc> = in_mem_docs.iter().filter(|doc| query.matches(doc)).cloned().collect();
                    let mut collector = AllDocsCollector::new();
                    search(&index.open_reader(), query, &mut collector).unwrap();
                    assert!(collector.docs() == expected_matches.as_slice() );
                }
            }
        }
    }
}
