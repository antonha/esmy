use serde::de::{self, Visitor};
use std::fmt;
use std::collections::HashMap;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use serde::Deserializer;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FieldValue {
    String(String),
}

pub type Doc = HashMap<String, FieldValue>;

impl<'a> Serialize for FieldValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        match *self {
            FieldValue::String(ref value) => serializer.serialize_str(&value),
        }
    }
}

impl<'de> Deserialize<'de> for FieldValue {
    fn deserialize<D>(deserializer: D) -> Result<FieldValue, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_str(FieldValueVisitor)
    }
}


struct FieldValueVisitor;

impl<'de> Visitor<'de> for FieldValueVisitor {
    type Value = FieldValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("A string value")
    }

    fn visit_str<E>(self, value: &str) -> Result<FieldValue, E>
        where
            E: de::Error,
    {
        Ok(FieldValue::String(String::from(value)))
    }
}

#[cfg(test)]
mod tests {

    use super::Doc;
    use super::FieldValue;
    use proptest::collection::hash_map;
    use proptest::prelude::*;
    use rmps::{Deserializer, Serializer};
    use serde::{Deserialize, Serialize};

    fn arb_fieldvalue() -> BoxedStrategy<FieldValue> {
        prop_oneof![".*".prop_map(FieldValue::String),].boxed()
    }

    fn arb_fieldname() -> BoxedStrategy<String> {
        "[a-z]+".prop_map(|s| s).boxed()
    }

    fn arb_doc() -> BoxedStrategy<Doc> {
        hash_map(arb_fieldname(), arb_fieldvalue(), 0..100).boxed()
    }

    proptest!{
        #[test]
        fn serializes_doc_correct(ref doc in arb_doc()) {
            let mut buf = Vec::new();
            doc.serialize(&mut Serializer::new(&mut buf)).unwrap();
            let mut de = Deserializer::new(&buf[..]);
            assert!(doc == &Deserialize::deserialize(&mut de).unwrap());
        }
    }
}
