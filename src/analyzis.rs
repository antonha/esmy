
use unicode_segmentation::UnicodeSegmentation;
use std::borrow::Cow;

pub trait Analyzer : AnalyzerClone{
    fn analyze<'a>(&self, value: &'a str) -> Box<Iterator<Item=Cow<'a, str>> +'a>;
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

#[derive(Clone)]
pub struct UAX29Analyzer{
}

impl Analyzer for UAX29Analyzer{
    fn analyze<'a>(&self, value: &'a str) -> Box<Iterator<Item=Cow<'a, str>> +'a>{
        Box::from(value.split_word_bounds()
                  .filter(|token| !is_only_whitespace_or_control_char(token))
                  .map(|token| 
                       if token.find(char::is_uppercase).is_some(){
                           Cow::Owned(token.to_lowercase())
                       }
                       else{
                           Cow::Borrowed(token)
                       }
                       ))
    }
}


fn is_only_whitespace_or_control_char(s: &str) -> bool{
    for c in s.chars(){
        if !(c.is_whitespace() || c.is_control()){
            return false;
        }
    }
    return true;
}
