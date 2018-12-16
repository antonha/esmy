/*!

# Esmy

Esmy is a library for full text search, written in Rust. It is inspired by Lucene, but aims to be more flexible.

## Features

* Text indexing with different analyzers.
* Text search, including phrases.
* Parallel indexing

## Roadmap

* Document scoring
* Document deletions
* Doc-values data structures (fast access to values of fields)
* Improve merge concurrency
* More query types (e.g. spans, more boolean logic)

## Example

```ignore

let schema = SegmentSchemaBuilder::new()
    .add_full_doc("full_doc_feature") //features have names
    .add_string_index(
        "text_string_index",
        "text",
        Box::new(UAX29Analyzer::new())) //Unicode tokenization
    .build();

let index = IndexBuilder::new().create("path/to/index", schema).unwrap();
let doc1 = Doc::new().string_field("text", "The quick brown fox jumps over the lazy dog");
index.add_doc(doc1).unwrap();
let doc2 = Doc::new().string_field("text", "Foxes are generally smaller than some other members of the family Canidae");
index.add_doc(doc2).unwrap();

index.commit().unwrap();

let query = TextQuery::new(
    "text",                         //field
    "brown fox",                    //value
    Box::new(UAX29Analyzer::new()), //Search with the same analyzer as we indexed
);
let mut collector = CountCollector::new();

let reader = index.open_reader().unwrap();
reader.search(&query, &mut collector).unwrap();
assert_eq!(1, collector.total_count());
```

## Design

Esmy is an information retrieval system, and takes a lot of inspiration from Lucene. The main idea is to have an inverted index, which allows you to look up which documents contain a certain term. However, often additional data structures are needed in order to be able to visualize or process the data, e.g. to create histograms of result sets or being able to do geo-search. Thus, Esmy is structured to accommodate adding new data structures.

Esmy, as e.g. Lucene, is structured around indexes and segments. A segment is a collection of on-disk data structures, and an index is a set of segments. Segments are immutable. When adding documents to Esmy, you add some documents which are at some point *commited* to disk, at which point a segment is created. Over time, this will mean many small segments. In order to prevent having so many small segments, Esmy can merge segments into larger segments. The on-disk data structures of the segments can then be used to do something useful, e.g. searching for text.

Apart from not being on the JVM, there are a few differences from Lucene.

One is that Lucene treats the inverted index as the core of the Library. While it is an important feature of Esmy, it's only one kind of useful data structure. Esmy instead has a concept of a *segment feature*. The inverted index is one such segment feature. The requirements on a segment feature is that you can create one from a set of documents, and that the feature can merge files that it wrote into larger files.

Features are identified by names, and since they are decoupled from fields you can add more than one type of index for a particular field. This means that you, for example, can have a document indexed with different analyzers without having to have separate fields for them, as you would in Lucene.

Another one is that Esmy has more opinionated (but open) view of what a document is. Lucene treats a document as a set of fields at input, but has no notion of a document when reading. This leads to e.g. Elasticsearch having a JSON-structure emulate this, by storing the JSON as a string field. Since Lucene is not Elasticsearch, Lucene can not use that `_source` field, Lucene can't use that field. Esmy instead has a notion of a document, and an on-disk data structure. This means that Esmy can use the document.


*/

#![allow(unknown_lints)]
#![cfg_attr(feature = "cargo-clippy", feature(tool_lints))]
#![cfg_attr(feature = "cargo-clippy", warn(clippy::all))]
#![cfg_attr(feature = "cargo-clippy", allow(clippy::implicit_hasher))]

extern crate bit_vec;
extern crate byteorder;
extern crate fasthash;
extern crate fst;
extern crate indexmap;
extern crate rand;
extern crate smallvec;
extern crate unicode_segmentation;
extern crate walkdir;

extern crate num_cpus;
extern crate rayon;

extern crate rmp_serde as rmps;
extern crate serde;

#[macro_use]
extern crate serde_derive;

extern crate lz4;

#[cfg(test)]
#[macro_use]
extern crate proptest;

#[macro_use]
extern crate lazy_static;

pub mod analyzis;
pub mod doc;
pub mod doc_iter;
pub mod error;
pub mod full_doc;
pub mod index;
pub mod search;
pub mod seg;
pub mod string_index;
pub mod string_pos_index;
mod util;
pub use error::Error;

pub type DocId = u64;
pub use doc::Doc;
