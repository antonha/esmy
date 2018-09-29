extern crate esmy;

#[macro_use]
extern crate proptest;

extern crate tempfile;

extern crate serde;
extern crate serde_json;

extern crate flate2;

#[macro_use]
extern crate lazy_static;

use esmy::analyzis::Analyzer;
use esmy::analyzis::NoopAnalyzer;
use esmy::analyzis::UAX29Analyzer;
use esmy::doc::Doc;
use esmy::doc::FieldValue;
use esmy::index::Index;
use esmy::index::IndexBuilder;
use esmy::search::search;
use esmy::search::AllDocsCollector;
use esmy::search::AllQuery;
use esmy::search::MatchAllDocsQuery;
use esmy::search::Query;
use esmy::search::TermQuery;
use esmy::search::TextQuery;
use esmy::search::ValueQuery;
use esmy::seg::SegmentSchema;
use esmy::seg::SegmentSchemaBuilder;
use proptest::collection::vec;
use proptest::collection::SizeRange;
use proptest::prelude::*;
use proptest::test_runner::Config;
use std::collections::HashSet;
use std::path::PathBuf;
use tempfile::TempDir;

proptest! {

    #![proptest_config(Config::with_cases(1000))]
    #[test]
    fn value_query_wiki_body_matching((ref ops, ref queries) in op_and_value_queries(0..10, 0..50, 0..100, "text".to_owned())) {
        let schema = SegmentSchemaBuilder::new()
            .add_string_index("string_index", "text", Box::from(NoopAnalyzer{}))
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }

    #[test]
    fn value_query_wiki_id_matching((ref ops, ref queries) in op_and_value_queries(0..10, 0..50, 0..100, "id".to_owned())) {
        let schema = SegmentSchemaBuilder::new()
            .add_string_index("string_index", "id", Box::from(NoopAnalyzer{}))
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }

    #[test]
    fn term_query_wiki_body_matching((ref ops, ref queries) in op_and_term_queries(0..10, 0..50, 0..100, "text".to_owned(), Box::new(UAX29Analyzer::new()))) {
        let schema = SegmentSchemaBuilder::new()
            .add_string_index("string_index", "text", Box::from(UAX29Analyzer{}))
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }

    #[test]
    fn text_query_wiki_body_matching((ref ops, ref queries) in op_and_text_queries(0..10, 0..50, 0..100, "text".to_owned(), Box::new(UAX29Analyzer::new()))) {
        let schema = SegmentSchemaBuilder::new()
            .add_string_index("string_index", "text", Box::from(UAX29Analyzer{}))
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }

    #[test]
    fn text_query_wiki_body_matching_pos_index((ref ops, ref queries) in op_and_text_queries(0..10, 0..50, 0..100, "text".to_owned(), Box::new(UAX29Analyzer::new()))) {
        let schema = SegmentSchemaBuilder::new()
            .add_string_pos_index("string_pos_index", "text", Box::from(UAX29Analyzer{}))
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }

    #[test]
    fn all_query_wiki_body_matching((ref ops, ref queries) in op_and_all_queries(0..10, 0..50, 0..100, "text".to_owned(), Box::new(UAX29Analyzer::new()))) {
        let schema = SegmentSchemaBuilder::new()
            .add_string_index("string_index", "text", Box::from(UAX29Analyzer{}))
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }
}

proptest! {
    #![proptest_config(Config::with_cases(10))]
    #[test]
    fn all_docs_many_docs_matching((ref ops, ref queries) in op_and_match_all_queries(0..10, 5000..10_000)) {
        let schema = SegmentSchemaBuilder::new()
            .add_full_doc("full_doc")
            .build();
        index_and_assert_search_matches(&schema, ops, queries);
    }
}

fn index_and_assert_search_matches(
    schema: &SegmentSchema,
    ops: &[IndexOperation],
    queries: &[Box<Query>],
) {
    let index_dir = TempDir::new().unwrap();
    {
        let index = IndexBuilder::new()
            .auto_commit(false)
            .auto_merge(false)
            .create(index_dir.path(), schema.clone())
            .expect("Could not open index.");
        let mut index_test_state = IndexTestState {
            index,
            in_mem_docs: Vec::new(),
            in_mem_seg_docs: Vec::new(),
        };
        index_test_state.apply_ops(ops);
        index_test_state.check_queries_match_same(queries);
    }
    index_dir.close().unwrap();
}

static COMPRESSED_JSON_WIKI_DOCS: &[u8] = include_bytes!("../../data/50k_wiki_docs.json.gz");
lazy_static! {
    static ref WIKI_DOCS: Vec<Doc> = {
        let compressed = flate2::read::GzDecoder::new(COMPRESSED_JSON_WIKI_DOCS);
        serde_json::Deserializer::from_reader(compressed)
            .into_iter::<Doc>()
            .map(|r| r.unwrap())
            .collect()
    };
}

fn op_and_value_queries(
    num_ops: impl Into<SizeRange>,
    num_docs: impl Into<SizeRange>,
    num_queries: impl Into<SizeRange>,
    field: String,
) -> BoxedStrategy<(Vec<IndexOperation>, Vec<Box<dyn Query>>)> {
    let num_queries: SizeRange = num_queries.into();
    vec(arb_index_op(num_docs), num_ops)
        .prop_flat_map(move |ops| {
            let num_queries = num_queries.clone();
            let values = extract_values(&ops, &field);
            if values.len() > 0 {
                vec(value_query(field.to_owned(), values.clone()), num_queries)
                    .prop_map(move |queries| (ops.clone(), queries))
                    .boxed()
            } else {
                let vec: Vec<Box<Query>> = Vec::new();
                Just((ops.clone(), vec)).boxed()
            }
        }).boxed()
}

fn op_and_term_queries(
    num_ops: impl Into<SizeRange>,
    num_docs: impl Into<SizeRange>,
    num_queries: impl Into<SizeRange>,
    field: String,
    analyzer: Box<dyn Analyzer>,
) -> BoxedStrategy<(Vec<IndexOperation>, Vec<Box<Query>>)> {
    let num_queries: SizeRange = num_queries.into();
    vec(arb_index_op(num_docs), num_ops)
        .prop_flat_map(move |ops| {
            let num_queries = num_queries.clone();
            let values = extract_terms(&ops, &field, &*analyzer);
            if values.len() > 0 {
                vec(
                    term_query(field.to_owned(), analyzer.clone(), values.clone()),
                    num_queries,
                ).prop_map(move |queries| (ops.clone(), queries))
                .boxed()
            } else {
                let vec: Vec<Box<Query>> = Vec::new();
                Just((ops.clone(), vec)).boxed()
            }
        }).boxed()
}

fn op_and_text_queries(
    num_ops: impl Into<SizeRange>,
    num_docs: impl Into<SizeRange>,
    num_queries: impl Into<SizeRange>,
    field: String,
    analyzer: Box<dyn Analyzer>,
) -> BoxedStrategy<(Vec<IndexOperation>, Vec<Box<Query>>)> {
    let num_queries: SizeRange = num_queries.into();
    vec(arb_index_op(num_docs), num_ops)
        .prop_flat_map(move |ops| {
            let num_queries = num_queries.clone();
            let field = field.clone();
            let analyzer = analyzer.clone();
            (1usize..4usize)
                .prop_flat_map(move |ngram_length| {
                    let ops = ops.clone();
                    let num_queries = num_queries.clone();
                    let token_ngrams = extract_token_ngrams(&ops, &field, &*analyzer, ngram_length);
                    if token_ngrams.len() > 0 {
                        vec(
                            text_query(field.to_owned(), analyzer.clone(), token_ngrams.clone()),
                            num_queries,
                        ).prop_map(move |queries| (ops.clone(), queries))
                        .boxed()
                    } else {
                        let vec: Vec<Box<Query>> = Vec::new();
                        Just((ops.clone(), vec)).boxed()
                    }
                }).boxed()
        }).boxed()
}

fn op_and_match_all_queries(
    num_ops: impl Into<SizeRange>,
    num_docs: impl Into<SizeRange>,
) -> BoxedStrategy<(Vec<IndexOperation>, Vec<Box<Query>>)> {
    vec(arb_index_op(num_docs), num_ops)
        .prop_map(move |ops| {
            (
                ops.clone(),
                vec![Box::new(MatchAllDocsQuery::new()) as Box<Query>],
            )
        }).boxed()
}

fn op_and_all_queries(
    num_ops: impl Into<SizeRange>,
    num_docs: impl Into<SizeRange>,
    num_queries: impl Into<SizeRange>,
    field: String,
    analyzer: Box<dyn Analyzer>,
) -> BoxedStrategy<(Vec<IndexOperation>, Vec<Box<Query>>)> {
    let num_queries: SizeRange = num_queries.into();
    vec(arb_index_op(num_docs), num_ops)
        .prop_flat_map(move |ops| {
            let num_queries = num_queries.clone();
            let values = extract_terms(&ops, &field, &*analyzer);
            if values.len() > 0 {
                vec(
                    all_queries(field.to_owned(), analyzer.clone(), values.clone()),
                    num_queries,
                ).prop_map(move |queries| (ops.clone(), queries))
                .boxed()
            } else {
                let vec: Vec<Box<Query>> = Vec::new();
                Just((ops.clone(), vec)).boxed()
            }
        }).boxed()
}

fn value_query(field_name: String, values: Vec<String>) -> BoxedStrategy<Box<Query>> {
    (0..values.len())
        .prop_map(move |i| {
            let val = &values[i];
            Box::new(ValueQuery::new(field_name.to_owned(), val.clone())) as Box<Query>
        }).boxed()
}

fn term_query(
    field_name: String,
    analyzer: Box<dyn Analyzer>,
    terms: Vec<String>,
) -> BoxedStrategy<Box<Query>> {
    (0..terms.len())
        .prop_map(move |i| {
            let term = &terms[i];
            Box::new(TermQuery::new(
                field_name.to_owned(),
                term.clone(),
                analyzer.clone(),
            )) as Box<Query>
        }).boxed()
}

fn text_query(
    field_name: String,
    analyzer: Box<dyn Analyzer>,
    token_ngrams: Vec<Vec<String>>,
) -> BoxedStrategy<Box<Query>> {
    (0..token_ngrams.len())
        .prop_map(move |i| {
            let ngram = &token_ngrams[i];
            Box::new(TextQuery::new(
                field_name.to_owned(),
                ngram.join(" ").clone(),
                analyzer.clone(),
            )) as Box<Query>
        }).boxed()
}

fn all_queries(
    field_name: String,
    analyzer: Box<dyn Analyzer>,
    terms: Vec<String>,
) -> BoxedStrategy<Box<Query>> {
    vec(
        term_query(field_name.clone(), analyzer.clone(), terms.clone()),
        1..5,
    ).prop_map(|sub_queries| Box::new(AllQuery::new(sub_queries)) as Box<Query>)
    .boxed()
}

fn arb_index_op(num_docs: impl Into<SizeRange>) -> BoxedStrategy<IndexOperation> {
    prop_oneof![
        vec(arb_doc(), num_docs).prop_map(IndexOperation::Index),
        Just(IndexOperation::Commit),
        Just(IndexOperation::Merge),
        Just(IndexOperation::ForceMerge)
    ].boxed()
}

fn arb_doc() -> BoxedStrategy<Doc> {
    (0..WIKI_DOCS.len())
        .prop_map(|i| WIKI_DOCS[i].clone())
        .boxed()
}

#[derive(Debug, Clone)]
enum IndexOperation {
    Index(Vec<Doc>),
    Commit,
    Merge,
    ForceMerge,
}

fn extract_token_ngrams(
    ops: &[IndexOperation],
    field_name: &str,
    analyzer: &Analyzer,
    ngram_size: usize,
) -> Vec<Vec<String>> {
    let values = extract_values(ops, field_name);
    let mut ngrams = HashSet::new();
    for v in values {
        let tokens = analyzer
            .analyze(&v)
            .map(|c| c.to_string())
            .collect::<Vec<String>>();
        for ngram in tokens.windows(ngram_size) {
            ngrams.insert(ngram.to_vec());
        }
    }
    ngrams.into_iter().collect()
}

fn extract_terms(ops: &[IndexOperation], field_name: &str, analyzer: &Analyzer) -> Vec<String> {
    let values = extract_values(ops, field_name);
    let mut tokens = HashSet::new();
    for v in values {
        for t in analyzer.analyze(&v) {
            tokens.insert(t.into_owned());
        }
    }
    tokens.into_iter().collect()
}

fn extract_values(ops: &[IndexOperation], field_name: &str) -> Vec<String> {
    let mut values: Vec<String> = Vec::new();
    for op in ops {
        match op {
            IndexOperation::Index(docs) => {
                for doc in docs {
                    match doc.get(field_name) {
                        Some(val) => match val {
                            FieldValue::String(str_val) => {
                                values.push(str_val.clone());
                            }
                        },
                        None => (),
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
                &IndexOperation::ForceMerge => {
                    self.index.force_merge().expect("Could not merge segments.");
                }
            }
        }
    }

    fn check_queries_match_same(&self, queries: &[Box<Query>]) {
        let reader = self.index.open_reader().unwrap();
        queries.iter().for_each(|query| {
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
    for doc in actual {
        assert!(
            expected.contains(doc),
            format!("Expected = {:?} did not contain {:?}", expected, doc)
        )
    }
    for doc in expected {
        assert!(
            actual.contains(doc),
            format!("Actual = {:?} did not contain {:?}", actual, doc)
        )
    }
}
