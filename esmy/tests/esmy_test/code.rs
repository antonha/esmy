use esmy::doc::FieldValue;
use esmy::search::Query;
use esmy::Doc;
use esmy_test::IndexOperation;

pub fn format_input(input: &(Vec<IndexOperation>, Vec<Box<Query>>)) -> String {
    let mut to_print = String::new();
    to_print.push_str(&format!(
        "
#[test]
extern crate esmy;
extern crate flate2;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate proptest;
extern crate serde;
extern crate serde_json;
extern crate tempfile;


use proptest::strategy::BoxedStrategy;
use proptest::strategy::Strategy;
use proptest::test_runner::Config;
use proptest::test_runner::TestRunner;

use esmy::analyzis::NoopAnalyzer;
use esmy::analyzis::UAX29Analyzer;
use esmy::doc::Doc;
use esmy::doc::DocDecorator;
use esmy::seg::SegmentSchemaBuilder;
use proptest::test_runner::TestError;
use esmy::search::Query;
use esmy_test::IndexOperation;

pub mod esmy_test;
fn retest() {{
    let schema = //TODO;
    let operations = vec![
        {}
    ];
    let queries = vec![
        {}
    ];
    esmy_test::index_and_assert_search_matches(&schema, &ops, &queries);
}}
",
        format_ops(&input.0),
        format_queries(&input.1)
    ));
    to_print
}

fn format_ops(operations: &[IndexOperation]) -> String {
    operations
        .iter()
        .map(|o| match o {
            IndexOperation::Index(docs) => format!(
                "IndexOperation::Index(vec![\n            {}\n        ])",
                docs.iter()
                    .map(|d| format_doc(d))
                    .collect::<Vec<String>>()
                    .join(",\n            ")
            ),
            IndexOperation::Commit => "IndexOperation::Commit".to_string(),
            IndexOperation::Merge => "IndexOperation::Merge".to_string(),
            IndexOperation::ForceMerge => "IndexOperation::ForceMerge".to_string(),
            IndexOperation::ReOpen => "IndexOperation::ReOpen".to_string(),
            IndexOperation::Delete(_q) => "IndexOperation::Delete()".to_string(),
        })
        .collect::<Vec<String>>()
        .join(",\n        ")
}

fn format_doc(doc: &Doc) -> String {
    let mut ret = String::new();
    ret.push_str("Doc::new()");
    for (field_name, field) in doc.iter() {
        match field {
            FieldValue::String(s) => ret.push_str(&format!(
                ".string_field(\"{}\", \"{}\")",
                field_name,
                s.replace("\n", "\\n").replace("\"", "\\\"")
            )),
        }
    }
    ret
}

fn format_queries<'a>(queries: &'a [Box<Query>]) -> String {
    format!(
        "vec![{}]",
        queries
            .iter()
            .map(|q| format_query(q))
            .collect::<Vec<String>>()
            .join(",\n")
    )
}

fn format_query(query: &Box<Query>) -> String {
    format!("{:?}", query)
}
