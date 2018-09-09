extern crate esmy;

#[cfg(test)]
#[macro_use]
extern crate proptest;

#[cfg(test)]
extern crate tempfile;

#[cfg(test)]
extern crate rayon;

#[cfg(test)]
mod tests {
    use esmy::analyzis::NoopAnalyzer;
    use esmy::doc::Doc;
    use esmy::doc::FieldValue;
    use esmy::full_doc::FullDoc;
    use esmy::index::Index;
    use esmy::index::IndexBuilder;
    use esmy::search::{search, AllDocsCollector, FullDocQuery, SegmentQuery, ValueQuery};
    use esmy::seg::Feature;
    use esmy::seg::SegmentSchema;
    use esmy::string_index::StringIndex;
    use proptest::collection::hash_map;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[derive(Debug, Clone)]
    enum IndexOperation {
        Index(Vec<Doc>),
        Commit,
        Merge,
    }

    fn arb_fieldvalue() -> BoxedStrategy<FieldValue> {
        "[a-z]+".prop_map(FieldValue::String).boxed()
    }

    fn arb_fieldname() -> BoxedStrategy<String> {
        prop_oneof![Just("field1".to_owned()), Just("field2".to_owned())].boxed()
    }

    fn arb_doc() -> BoxedStrategy<Doc> {
        hash_map(arb_fieldname(), arb_fieldvalue(), 1..2).boxed()
    }

    fn arb_index_op() -> BoxedStrategy<IndexOperation> {
        prop_oneof![
            vec(arb_doc(), 20..5001).prop_map(IndexOperation::Index),
            Just(IndexOperation::Commit),
            Just(IndexOperation::Merge)
        ].boxed()
    }

    fn value_query(vec: Vec<(String, String)>) -> BoxedStrategy<ValueQuery> {
        (0..vec.len())
            .prop_map(move |i| {
                let (ref key, ref val) = vec[i];
                ValueQuery::new(key.clone(), val.clone())
            })
            .boxed()
    }

    fn op_and_value_queries() -> BoxedStrategy<(Vec<IndexOperation>, Vec<ValueQuery>)> {
        vec(arb_index_op(), 1..100)
            .prop_flat_map(|ops| {
                let values = extract_values(&ops);
                if values.len() > 0 {
                    vec(value_query(values.clone()), 0..100)
                        .prop_map(move |queries| (ops.clone(), queries))
                        .boxed()
                } else {
                    Just((ops.clone(), Vec::new())).boxed()
                }
            })
            .boxed()
    }

    fn extract_values(ops: &[IndexOperation]) -> Vec<(String, String)> {
        let mut values: Vec<(String, String)> = Vec::new();
        for op in ops {
            match op {
                IndexOperation::Index(docs) => {
                    for doc in docs {
                        for (name, value) in doc {
                            match value {
                                FieldValue::String(str_val) => {
                                    values.push((name.clone(), str_val.clone()));
                                }
                            }
                        }
                    }
                }
                _ => (),
            }
        }
        values
    }

    struct IndexTestState {
        index: Index,
        in_mem_docs: Vec<Doc>,
        in_mem_seg_docs: Vec<Doc>,
    }

    impl IndexTestState {
        fn apply_ops(&mut self, ops: &[IndexOperation]) {
            for op in ops {
                match op {
                    &IndexOperation::Index(ref docs) => {
                        for doc in docs {
                            self.index.add_doc(doc.clone()).unwrap();
                            self.in_mem_seg_docs.push(doc.clone());
                        }
                    }
                    &IndexOperation::Commit => {
                        self.index.commit().expect("Could not commit segments.");
                        self.in_mem_docs.append(&mut self.in_mem_seg_docs);
                        self.in_mem_seg_docs = Vec::new();
                    }
                    &IndexOperation::Merge => {
                        self.index.merge().expect("Could not merge segments.");
                    }
                }
            }
        }

        fn check_queries_match_same<Q>(&self, queries: &[ValueQuery])
        where
            Q: FullDocQuery + SegmentQuery + Send + Sized,
        {
            let reader = self.index.open_reader().unwrap();
            queries.par_iter().for_each(|query| {
                let expected_matches: Vec<Doc> = self
                    .in_mem_docs
                    .iter()
                    .filter(|doc| query.matches(doc))
                    .cloned()
                    .collect();
                let mut collector = AllDocsCollector::new();
                search(&reader, query, &mut collector).unwrap();
                assert_same_docs(&expected_matches, collector.docs());
            });
        }
    }

    fn assert_same_docs(expected: &[Doc], actual: &[Doc]) {
        assert_eq!(expected.len(), actual.len());
        for doc in actual {
            assert!(expected.contains(doc), "Expected = {} did not contain {}")
        }
        for doc in expected {
            assert!(actual.contains(doc), "Actual = {} did not contain {}")
        }
    }

    proptest! {
        #![proptest_config(Config::with_cases(200))]
        #[test]
        fn finds_merged((ref ops, ref queries) in op_and_value_queries()) {
            let index_dir = TempDir::new().unwrap();{
            let index_path = PathBuf::from(index_dir.path());
                let mut features: HashMap<String, Box<dyn Feature>> =  HashMap::new();
                features.insert("1".to_string(), Box::new(StringIndex::new("field1".to_string(), Box::from(NoopAnalyzer{}))));
                features.insert("2".to_string(), Box::new(StringIndex::new("field2".to_string(), Box::from(NoopAnalyzer{}))));
                features.insert("f".to_string(), Box::new(FullDoc::new()));
                let schema = SegmentSchema {features};

                let index = IndexBuilder::new()
                    .auto_commit(false)
                    .auto_merge(false)
                    .create(index_path, schema)
                    .expect("Could not open index.");
                let mut index_test_state = IndexTestState{
                    index,
                    in_mem_docs: Vec::new(),
                    in_mem_seg_docs: Vec::new(),
                };
                index_test_state.apply_ops(ops);
                index_test_state.check_queries_match_same::<ValueQuery>(queries);
            }
            index_dir.close().unwrap();
        }
    }

}
