extern crate afsort;
extern crate byteorder;
extern crate fst;
extern crate rand;
extern crate unicode_segmentation;
extern crate walkdir;

extern crate rmp_serde as rmps;
extern crate serde;

#[cfg(test)]
#[macro_use]
extern crate proptest;

pub mod seg;
pub mod analyzis;
pub mod search;
