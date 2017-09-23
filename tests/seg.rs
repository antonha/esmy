#[macro_use]
extern crate quickcheck;
extern crate esmy;


#[cfg(test)]
mod tests {
    use esmy::seg::{self,StringIndex,StringValues};
    use quickcheck::TestResult;
    use std::collections::HashMap;
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::str::from_utf8;
    quickcheck! {
        fn finds(docs: HashMap<String, String>) -> TestResult {
            if docs.is_empty() {
                return TestResult::discard();
            }
            if docs.values().find(|&s| s.is_empty()).is_some(){
                return TestResult::discard();
            }
            if docs.keys().find(|&s| s.is_empty()).is_some(){
                return TestResult::discard();
            }

            let index_path = env::current_dir().unwrap().join(&Path::new("tmp/tests/index"));
            if index_path.exists() {
                fs::remove_dir_all(&index_path);
            }
            let features : Vec<Box<seg::Feature>> = vec![
                Box::new(StringIndex::new("value")),
                Box::new(StringValues::new("key")),
            ];
            let index = seg::Index::new(seg::SegmentSchema{features}, &index_path);
            {
                let mut builder = index.new_segment();
                for (key, value) in docs.iter(){
                    builder.add_doc(
                        vec![
                            seg::Field {name: "key",value: seg::FieldValue::StringField(vec![key.clone()])},
                            seg::Field {name: "value",value: seg::FieldValue::StringField(vec![value.clone()])},
                        ]);
                }
                builder.commit().unwrap();
            }
            let indexReader = index.open_reader();
            let ref reader = &indexReader.segment_readers()[0];
            for (key, value) in docs.iter() {
                for doc in reader.string_index("value").unwrap().doc_iter("value", &value).unwrap() {
                    let docid = doc.unwrap();
                    let actual_keys = reader.string_values("key").unwrap().read_values(docid).unwrap();
                    let actual_key = &actual_keys[0];
                    if from_utf8(&actual_key).unwrap() != key {
                        return TestResult::failed();
                    }
                }
            }
            return TestResult::passed();
        }
    }
}
