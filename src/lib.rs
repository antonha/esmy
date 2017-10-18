
#![feature(conservative_impl_trait)]
extern crate byteorder;
extern crate fst;
extern crate rand;
extern crate walkdir;
extern crate unicode_segmentation;
extern crate afsort;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;

pub mod seg;
pub mod analyzis;
pub mod search;
