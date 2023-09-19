use std::collections::HashSet;

use proptest::collection::vec;
use proptest::prelude::BoxedStrategy;
use proptest::strategy::Just;
use proptest::strategy::Strategy;

use esmy::analyzis::Analyzer;
use esmy::doc::FieldValue;
use esmy::search::AllQuery;
use esmy::search::MatchAllDocsQuery;
use esmy::search::Query;
use esmy::search::TermQuery;
use esmy::search::TextQuery;
use esmy::search::ValueQuery;
use esmy::Doc;

pub fn match_all_docs() -> BoxedStrategy<Box<dyn Query>> {
    Just(Box::new(MatchAllDocsQuery::new()) as Box<dyn Query>).boxed()
}

pub fn value_query(docs: &[&Doc], field_name: &'static str) -> BoxedStrategy<Box<dyn Query>> {
    let values = extract_doc_values(&docs, field_name);
    if !values.is_empty() {
        (0..values.len())
            .prop_map(move |i| {
                let val = &values[i];
                Box::new(ValueQuery::new(field_name.to_owned(), val.clone())) as Box<dyn Query>
            })
            .boxed()
    } else {
        prop_oneof!("foobar", "cat", "Anne")
            .prop_map(move |term| {
                Box::new(ValueQuery::new(field_name.to_owned(), term)) as Box<dyn Query>
            })
            .boxed()
    }
}

pub fn term_query(
    docs: &[&Doc],
    field_name: &'static str,
    analyzer: Box<dyn Analyzer>,
) -> BoxedStrategy<Box<dyn Query>> {
    let terms = extract_doc_terms(docs, field_name, analyzer.clone());
    if !terms.is_empty() {
        (0..terms.len())
            .prop_map(move |i| {
                let term = &terms[i];
                Box::new(TermQuery::new(
                    field_name.to_owned(),
                    term.clone(),
                    (analyzer).clone(),
                )) as Box<dyn Query>
            })
            .boxed()
    } else {
        prop_oneof!("foobar", "cat", "Anne")
            .prop_map(move |term| {
                Box::new(TermQuery::new(
                    field_name.to_owned(),
                    term,
                    analyzer.clone(),
                )) as Box<dyn Query>
            })
            .boxed()
    }
}

pub fn text_query(
    docs: &[&Doc],
    field_name: &'static str,
    analyzer: Box<dyn Analyzer>,
    ngram_size: usize,
) -> BoxedStrategy<Box<dyn Query>> {
    let token_ngrams = extract_token_ngrams(docs, field_name, analyzer.clone(), ngram_size);
    if token_ngrams.is_empty() {
        prop_oneof!("foobar", "cat", "Anne", "Cat fish")
            .prop_map(move |text| {
                Box::new(TextQuery::new(
                    field_name.to_owned(),
                    text,
                    analyzer.clone(),
                )) as Box<dyn Query>
            })
            .boxed()
    } else {
        (0..token_ngrams.len())
            .prop_map(move |i| {
                let ngram = &token_ngrams[i];
                Box::new(TextQuery::new(
                    field_name.to_owned(),
                    ngram.join(" ").clone(),
                    analyzer.clone(),
                )) as Box<dyn Query>
            })
            .boxed()
    }
}

pub fn all_queries(
    docs: &[&Doc],
    field_name: &'static str,
    analyzer: Box<dyn Analyzer>,
) -> BoxedStrategy<Box<dyn Query>> {
    vec(term_query(docs, field_name, analyzer), 1..5)
        .prop_map(|sub_queries| Box::new(AllQuery::new(sub_queries)) as Box<dyn Query>)
        .boxed()
}

fn extract_token_ngrams(
    docs: &[&Doc],
    field_name: &str,
    analyzer: Box<dyn Analyzer>,
    ngram_size: usize,
) -> Vec<Vec<String>> {
    let values = extract_doc_values(docs, field_name);
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

fn extract_doc_terms(docs: &[&Doc], field_name: &str, analyzer: Box<dyn Analyzer>) -> Vec<String> {
    let values = extract_doc_values(docs, field_name);
    let mut tokens = HashSet::new();
    for v in values {
        for t in analyzer.analyze(&v) {
            tokens.insert(t.into_owned());
        }
    }
    tokens.into_iter().collect()
}

fn extract_doc_values(docs: &[&Doc], field_name: &str) -> Vec<String> {
    let mut values: Vec<String> = Vec::new();
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
    values
}
