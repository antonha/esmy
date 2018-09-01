extern crate afsort;
extern crate byteorder;
extern crate fst;
extern crate rand;
extern crate threadpool;
extern crate unicode_segmentation;
extern crate walkdir;

extern crate num_cpus;
extern crate core_affinity;
extern crate rayon;

extern crate crossbeam_channel;

extern crate rmp_serde as rmps;
extern crate serde;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate proptest;

pub mod analyzis;
pub mod doc;
pub mod error;
pub mod full_doc;
pub mod index;
pub mod search;
pub mod seg;
pub mod string_index;
mod util;
pub use error::Error;
