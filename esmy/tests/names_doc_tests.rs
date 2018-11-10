extern crate esmy;
extern crate flate2;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate proptest;
extern crate rayon;
extern crate serde;
extern crate serde_json;
extern crate tempfile;

use proptest::strategy::BoxedStrategy;
use proptest::strategy::Strategy;
use proptest::test_runner::Config;
use proptest::test_runner::TestError;
use proptest::test_runner::TestRunner;

use esmy::analyzis::NoopAnalyzer;
use esmy::analyzis::UAX29Analyzer;
use esmy::doc::Doc;
use esmy::seg::SegmentSchemaBuilder;

pub mod esmy_test;

#[test]
fn value_query_name_matching() {
    let mut runner = TestRunner::new(Config::with_cases(1000));
    let ops_and_queries = esmy_test::do_gen(10, 0..10, arb_name_doc(), 0..10, {
        |docs| esmy_test::query_gen::value_query(docs, "name")
    });
    let schema = SegmentSchemaBuilder::new()
        .add_string_index("string_index", "name", Box::from(NoopAnalyzer {}))
        .add_full_doc_with_compression("full_doc", 0)
        .build();
    let result = runner.run(&ops_and_queries, |(ops, queries)| {
        esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
        Ok(())
    });
    match result {
        Ok(()) => (),
        Err(e) => match e {
            TestError::Fail(reason, input) => {
                println!("{}", esmy_test::code::format_input(&input));
                panic!("Test failed due to: {:?}", reason)
            }
            TestError::Abort(reason) => {
                panic!("Test aborted due to: {:?}", reason);
            }
        },
    }
}

#[test]
fn term_query_name_matching() {
    let mut runner = TestRunner::new(Config::with_cases(1000));
    let ops_and_queries = esmy_test::do_gen(10, 0..10, arb_name_doc(), 0..10, {
        |docs| esmy_test::query_gen::term_query(docs, "name", Box::from(UAX29Analyzer::new()))
    });
    let schema = SegmentSchemaBuilder::new()
        .add_string_index("string_index", "name", Box::from(UAX29Analyzer {}))
        .add_full_doc_with_compression("full_doc", 0)
        .build();
    runner
        .run(&ops_and_queries, |(ops, queries)| {
            esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
            Ok(())
        })
        .unwrap();
}

#[test]
fn text_query_name_matching() {
    let mut runner = TestRunner::new(Config::with_cases(1000));
    let ops_and_queries = esmy_test::do_gen(10, 0..10, arb_name_doc(), 0..10, {
        |docs| esmy_test::query_gen::text_query(docs, "name", Box::from(UAX29Analyzer::new()), 5)
    });
    let schema = SegmentSchemaBuilder::new()
        .add_string_index("string_index", "name", Box::from(UAX29Analyzer {}))
        .add_full_doc_with_compression("full_doc", 0)
        .build();
    runner
        .run(&ops_and_queries, |(ops, queries)| {
            esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
            Ok(())
        })
        .unwrap();
}

#[test]
fn all_query_name_matching() {
    let mut runner = TestRunner::new(Config::with_cases(100));
    let ops_and_queries = esmy_test::do_gen(10, 0..100, arb_name_doc(), 0..200, {
        |docs| esmy_test::query_gen::all_queries(docs, "name", Box::from(UAX29Analyzer::new()))
    });
    let schema = SegmentSchemaBuilder::new()
        .add_string_index("string_index", "name", Box::from(UAX29Analyzer {}))
        .add_full_doc_with_compression("full_doc", 0)
        .build();
    runner
        .run(&ops_and_queries, |(ops, queries)| {
            esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
            Ok(())
        })
        .unwrap();
}

#[test]
fn all_docs_many_docs_matching() {
    let mut runner = TestRunner::new(Config::with_cases(10));
    let ops_and_queries = esmy_test::do_gen(10, 5000..10000, arb_name_doc(), 1..=1, {
        |_docs| esmy_test::query_gen::match_all_docs()
    });
    let schema = SegmentSchemaBuilder::new()
        .add_full_doc_with_compression("full_doc", 0)
        .build();
    runner
        .run(&ops_and_queries, |(ops, queries)| {
            esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
            Ok(())
        })
        .unwrap();
}

#[test]
fn text_query_name_matching_pos_index() {
    let mut runner = TestRunner::new(Config::with_cases(10000));
    let ops_and_queries = esmy_test::do_gen(10, 0..10, arb_name_doc(), 1..5, {
        |docs| esmy_test::query_gen::text_query(docs, "name", Box::from(UAX29Analyzer::new()), 4)
    });
    let schema = SegmentSchemaBuilder::new()
        .add_string_pos_index("string_pos_index", "name", Box::from(UAX29Analyzer::new()))
        .add_full_doc_with_compression("full_doc", 0)
        .build();
    runner
        .run(&ops_and_queries, |(ops, queries)| {
            esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
            Ok(())
        })
        .unwrap();
}

fn arb_name_doc() -> BoxedStrategy<Doc> {
    (0..NAME_DOCS.len())
        .prop_map(|i| NAME_DOCS[i].clone())
        .boxed()
}

static COMPRESSED_JSON_NAME_DOCS: &[u8] = include_bytes!("../../data/1k_names.json.gz");
lazy_static! {
    static ref NAME_DOCS: Vec<Doc> = {
        let compressed = flate2::read::GzDecoder::new(COMPRESSED_JSON_NAME_DOCS);
        serde_json::Deserializer::from_reader(compressed)
            .into_iter::<Doc>()
            .map(|r| r.unwrap())
            .collect()
    };
}
