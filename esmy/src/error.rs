use rayon::ThreadPoolBuildError;
use rmps;
use std;
use std::convert::From;

#[derive(Debug)]
pub enum Error {
    IOError(std::io::Error),
    Other(Box<std::error::Error + Send>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::IOError(ref io) => io.fmt(f),
            Error::Other(ref err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::IOError(ref io) => io.description(),
            Error::Other(ref other) => other.description(),
        }
    }

    fn cause(&self) -> Option<&std::error::Error> {
        match *self {
            Error::IOError(ref io) => Some(io),
            Error::Other(ref other) => Some(&**other),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IOError(e)
    }
}

impl From<rmps::decode::Error> for Error {
    fn from(e: rmps::decode::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

impl From<rmps::encode::Error> for Error {
    fn from(e: rmps::encode::Error) -> Self {
        Error::Other(Box::new(e))
    }
}

impl From<ThreadPoolBuildError> for Error {
    fn from(e: ThreadPoolBuildError) -> Self {
        Error::Other(Box::new(e))
    }
}
