
#![feature(conservative_impl_trait)]
extern crate byteorder;
extern crate fst;
extern crate rand;
extern crate walkdir;
extern crate unicode_segmentation;
extern crate afsort;

extern crate serde;
extern crate rmp_serde as rmps;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

pub mod seg;
pub mod analyzis;
pub mod search;
