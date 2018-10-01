use std::borrow::Cow;
use std::fmt::Debug;
use std::iter;
use unicode_segmentation::UnicodeSegmentation;

pub trait Analyzer: AnalyzerClone + Send + Sync + Debug {
    fn analyzer_type(&self) -> &'static str;
    fn analyze<'a>(&self, value: &'a str) -> Box<Iterator<Item = Cow<'a, str>> + 'a>;
}

impl Analyzer {
    pub fn for_name(name: &str) -> Box<Analyzer> {
        match name {
            "uax29" => Box::new(UAX29Analyzer),
            "whitespace" => Box::new(WhiteSpaceAnalyzer),
            "noop" => Box::new(NoopAnalyzer),
            _ => panic!("No such analyzer"),
        }
    }
}

pub trait AnalyzerClone {
    fn clone_box(&self) -> Box<Analyzer>;
}

impl<T> AnalyzerClone for T
where
    T: 'static + Analyzer + Clone,
{
    fn clone_box(&self) -> Box<Analyzer> {
        Box::new(self.clone())
    }
}

impl Clone for Box<Analyzer> {
    fn clone(&self) -> Box<Analyzer> {
        self.clone_box()
    }
}

#[derive(Clone, Debug, Default)]
pub struct UAX29Analyzer;

impl UAX29Analyzer {
    pub fn new() -> UAX29Analyzer {
        UAX29Analyzer {}
    }

    pub fn boxed(self) -> Box<UAX29Analyzer> {
        Box::new(self)
    }
}

impl Analyzer for UAX29Analyzer {
    fn analyzer_type(&self) -> &'static str {
        "uax29"
    }

    fn analyze<'a>(&self, value: &'a str) -> Box<Iterator<Item = Cow<'a, str>> + 'a> {
        Box::from(
            value
                .split_word_bounds()
                .filter(|token| !is_only_whitespace_or_control_char(token))
                .map(|token| {
                    if token.find(char::is_uppercase).is_some() {
                        Cow::Owned(token.to_lowercase())
                    } else {
                        Cow::Borrowed(token)
                    }
                }),
        )
    }
}

fn is_only_whitespace_or_control_char(s: &str) -> bool {
    for c in s.chars() {
        if !(c.is_whitespace() || c.is_control()) {
            return false;
        }
    }
    true
}

#[derive(Clone, Debug)]
pub struct WhiteSpaceAnalyzer;

impl WhiteSpaceAnalyzer {
    pub fn new() -> UAX29Analyzer {
        UAX29Analyzer {}
    }

    pub fn boxed(self) -> Box<WhiteSpaceAnalyzer> {
        Box::new(self)
    }
}

impl Analyzer for WhiteSpaceAnalyzer {
    fn analyzer_type(&self) -> &'static str {
        "whitespace"
    }

    fn analyze<'a>(&self, value: &'a str) -> Box<Iterator<Item = Cow<'a, str>> + 'a> {
        Box::from(value.split_whitespace().map(|s| Cow::Borrowed(s)))
    }
}

#[derive(Clone, Debug)]
pub struct NoopAnalyzer;

impl NoopAnalyzer {
    pub fn new() -> UAX29Analyzer {
        UAX29Analyzer {}
    }

    pub fn boxed(self) -> Box<NoopAnalyzer> {
        Box::new(self)
    }
}

impl Analyzer for NoopAnalyzer {
    fn analyzer_type(&self) -> &'static str {
        "noop"
    }

    fn analyze<'a>(&self, value: &'a str) -> Box<Iterator<Item = Cow<'a, str>> + 'a> {
        Box::from(iter::once(Cow::Borrowed(value)))
    }
}
