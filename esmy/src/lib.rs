extern crate afsort;
extern crate byteorder;
extern crate fst;
extern crate rand;
extern crate threadpool;
extern crate unicode_segmentation;
extern crate walkdir;

extern crate rmp_serde as rmps;
extern crate serde;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate proptest;

pub mod analyzis;
pub mod index_manager;
pub mod search;
pub mod seg;
