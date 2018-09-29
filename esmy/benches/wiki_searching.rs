#![feature(test)]

extern crate test;
#[macro_use]
extern crate lazy_static;

extern crate tempfile;

extern crate flate2;
extern crate serde;
extern crate serde_json;

extern crate esmy;

use esmy::analyzis::UAX29Analyzer;
use esmy::doc::Doc;
use esmy::index::Index;
use esmy::index::IndexBuilder;
use esmy::search::search;
use esmy::search::CountCollector;
use esmy::search::TextQuery;
use esmy::seg::SegmentSchema;
use esmy::seg::SegmentSchemaBuilder;
use esmy::Error;
use tempfile::TempDir;
use test::black_box;
use test::Bencher;

#[bench]
fn search_phrases_with_string_index(b: &mut Bencher) {
    let analyzer = Box::new(UAX29Analyzer::new());
    let schema = SegmentSchemaBuilder::new()
        .add_full_doc("full_doc")
        .add_string_index("text_string_index", "text", analyzer.clone())
        .build();
    let index = index_docs(schema).unwrap();
    let reader = index.open_reader().unwrap();

    let queries = vec![TextQuery::new(
        "text".to_string(),
        "anton the".to_string(),
        analyzer.clone(),
    )];
    b.iter(|| {
        for q in &queries {
            let mut c = CountCollector::new();
            search(&reader, q, &mut c).unwrap();
            black_box(c.total_count());
        }
    })
}

#[bench]
fn search_phrases_with_string_pos_index(b: &mut Bencher) {
    let analyzer = Box::new(UAX29Analyzer::new());
    let schema = SegmentSchemaBuilder::new()
        .add_full_doc("full_doc")
        .add_string_pos_index("text_string_pos_index", "text", analyzer.clone())
        .build();
    let index = index_docs(schema).unwrap();
    let reader = index.open_reader().unwrap();

    let queries = vec![TextQuery::new(
        "text".to_string(),
        "anton the".to_string(),
        analyzer.clone(),
    )];
    b.iter(|| {
        for q in &queries {
            let mut c = CountCollector::new();
            search(&reader, q, &mut c).unwrap();
            black_box(c.total_count());
        }
    })
}


fn index_docs(schema: SegmentSchema) -> Result<Index, Error> {
    let index_dir = TempDir::new().unwrap();
    let index = IndexBuilder::new()
        .auto_commit(false)
        .auto_merge(false)
        .create(index_dir.into_path(), schema)?;
    for d in WIKI_DOCS.iter() {
        index.add_doc(d.clone())?;
    }
    index.commit()?;
    index.force_merge()?;
    Ok(index)
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
