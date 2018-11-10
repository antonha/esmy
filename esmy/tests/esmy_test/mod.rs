use proptest::collection::vec;
use proptest::collection::SizeRange;
use proptest::prelude::BoxedStrategy;
use proptest::strategy::Just;
use proptest::strategy::Strategy;
use tempfile::TempDir;

use esmy::index::Index;
use esmy::index::IndexBuilder;
use esmy::search::AllDocsCollector;
use esmy::search::Query;
use esmy::seg::SegmentSchema;
use esmy::Doc;
use std::path::PathBuf;

pub mod code;
pub mod query_gen;

#[derive(Debug, Clone)]
pub enum IndexOperation {
    Index(Vec<Doc>),
    Commit,
    ReOpen,
    Merge,
    ForceMerge,
    Delete(Box<Query>),
}

pub fn index_and_assert_search_matches(
    schema: &SegmentSchema,
    ops: &[IndexOperation],
    queries: &[Box<Query>],
) {
    let index_dir = TempDir::new().unwrap();
    {
        let index = IndexBuilder::new()
            .auto_commit(false)
            .auto_merge(false)
            .create(index_dir.path().clone(), schema.clone())
            .expect("Could not open index.");
        let mut index_test_state = IndexTestState {
            index_path: PathBuf::from(index_dir.path()),
            index,
            in_mem_docs: Vec::new(),
            in_mem_seg_docs: Vec::new(),
            docs_to_delete: Vec::new(),
        };
        index_test_state.apply_ops(ops);
        index_test_state.check_queries_match_same(queries);
    }
    index_dir.close().unwrap();
}

struct IndexTestState {
    index_path: PathBuf,
    index: Index,
    in_mem_docs: Vec<Doc>,
    in_mem_seg_docs: Vec<Doc>,
    docs_to_delete: Vec<Doc>,
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
                &IndexOperation::ForceMerge => {
                    self.index.force_merge().expect("Could not merge segments.");
                }
                &IndexOperation::Delete(ref query) => {
                    self.index.delete(query).expect("Could not delete docs.");
                    self.docs_to_delete.extend(
                        self.in_mem_docs
                            .iter()
                            .filter(|d| query.matches(d))
                            .cloned(),
                    );
                }
                &IndexOperation::ReOpen => {
                    self.index.commit().expect("Could not commit segment.");
                    let index = IndexBuilder::new()
                        .auto_commit(false)
                        .auto_merge(false)
                        .open(self.index_path.clone())
                        .expect("Could not re-open index.");
                    ::std::mem::replace(&mut self.index, index);
                    self.in_mem_docs.append(&mut self.in_mem_seg_docs);
                    self.in_mem_seg_docs = Vec::new();
                }
            }
        }
    }

    fn check_queries_match_same(&self, queries: &[Box<Query>]) {
        let reader = self.index.open_reader().unwrap();
        let retained_docs: Vec<Doc> = self
            .in_mem_docs
            .iter()
            .filter(|d| !self.docs_to_delete.contains(d))
            .cloned()
            .collect();
        queries.iter().for_each(|query| {
            let expected_matches: Vec<Doc> = retained_docs
                .iter()
                .filter(|doc| query.matches(doc))
                .cloned()
                .collect();
            let mut collector = AllDocsCollector::new();
            reader.search(query, &mut collector).unwrap();
            assert_same_docs(query, &expected_matches, collector.docs());
        });
    }
}

fn assert_same_docs(query: &Query, expected: &[Doc], actual: &[Doc]) {
    if expected != actual {
        for doc in expected {
            assert!(
                actual.contains(doc),
                format!(
                    "Actual = {:?} did not contain {:?} for query {:?}",
                    actual, doc, query
                )
            )
        }
        for doc in actual {
            assert!(
                expected.contains(doc),
                format!(
                    "Expected = {:?} did not contain {:?} for query {:?}",
                    expected, doc, query
                )
            )
        }
    }
}

pub fn do_gen<Q>(
    num_ops: u32,
    num_docs: impl Into<SizeRange>,
    doc_strategy: BoxedStrategy<Doc>,
    num_queries: impl Into<SizeRange>,
    query_strategy: Q,
) -> BoxedStrategy<(Vec<IndexOperation>, Vec<Box<Query>>)>
where
    Q: Fn(&[&Doc]) -> BoxedStrategy<Box<Query>> + 'static + Copy,
{
    let num_q = num_queries.into();
    let qs = query_strategy;
    arb_index_ops(num_ops, doc_strategy, num_docs.into(), query_strategy)
        .prop_flat_map(move |ops| {
            let docs = extract_docs(&ops);
            let ops = ops.clone();
            vec(qs(&docs), num_q.clone()).prop_map(move |queries| (ops.clone(), queries.clone()))
        })
        .boxed()
}

fn arb_index_ops<Q>(
    num_ops: u32,
    doc_strategy: BoxedStrategy<Doc>,
    num_docs: SizeRange,
    query_gen: Q,
) -> BoxedStrategy<Vec<IndexOperation>>
where
    Q: Fn(&[&Doc]) -> BoxedStrategy<Box<Query>> + 'static + Copy,
{
    Just(Vec::new())
        .prop_recursive(num_ops, num_ops, 1, move |vec| {
            prop_oneof![
                add_index_op(vec.clone(), doc_strategy.clone(), num_docs.clone()),
                add_basic_op(vec.clone(), IndexOperation::Commit),
                add_basic_op(vec.clone(), IndexOperation::Merge),
                add_basic_op(vec.clone(), IndexOperation::ForceMerge),
                add_basic_op(vec.clone(), IndexOperation::ReOpen),
                add_delete_op(vec.clone(), query_gen)
            ]
        })
        .boxed()
}

fn add_basic_op(
    input: BoxedStrategy<Vec<IndexOperation>>,
    op: IndexOperation,
) -> BoxedStrategy<Vec<IndexOperation>> {
    input
        .prop_map(move |mut v| {
            v.push(op.clone());
            v
        })
        .boxed()
}

fn add_index_op(
    input: BoxedStrategy<Vec<IndexOperation>>,
    docs: BoxedStrategy<Doc>,
    num_docs: SizeRange,
) -> BoxedStrategy<Vec<IndexOperation>> {
    input
        .prop_flat_map(move |v| {
            let v2 = v.clone();
            vec(docs.clone(), num_docs.clone()).prop_map(move |docs| {
                let mut vm = v2.clone();
                vm.push(IndexOperation::Index(docs));
                vm.clone()
            })
        })
        .boxed()
}

fn add_delete_op<Q>(
    input: BoxedStrategy<Vec<IndexOperation>>,
    query_gen: Q,
) -> BoxedStrategy<Vec<IndexOperation>>
where
    Q: Fn(&[&Doc]) -> BoxedStrategy<Box<Query>> + 'static,
{
    input
        .prop_flat_map(move |v| {
            let v2 = v.clone();
            query_gen(&extract_docs(&v2)).prop_map(move |query| {
                let mut vm = v2.clone();
                vm.push(IndexOperation::Delete(query));
                vm.clone()
            })
        })
        .boxed()
}

pub fn extract_docs(ops: &[IndexOperation]) -> Vec<&Doc> {
    let mut docs = Vec::new();
    ops.iter()
        .filter_map(|o| match o {
            IndexOperation::Index(docs) => Some(docs),
            _ => None,
        })
        .flat_map(|docs| docs.iter())
        .for_each(|d| docs.push(d));
    docs
}
