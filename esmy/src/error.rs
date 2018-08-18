use rmps;
use std;
use std::convert::From;

#[derive(Debug)]
pub enum Error {
    IOError,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::IOError => write!(f, "please use a vector with at least one element"),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IOError => "foobar",
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IOError => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(_e: std::io::Error) -> Self {
        return Error::IOError;
    }
}

impl From<rmps::decode::Error> for Error {
    fn from(_e: rmps::decode::Error) -> Self {
        return Error::IOError;
    }
}

impl From<rmps::encode::Error> for Error {
    fn from(_e: rmps::encode::Error) -> Self {
        return Error::IOError;
    }
}
