#[macro_use]
extern crate quickcheck;
extern crate esmy;


#[cfg(test)]
mod tests {
    use esmy::seg::{self, StringIndex, StringValues};
    use esmy::analyzis::{NoopAnalyzer};
    use quickcheck::TestResult;
    use std::collections::HashMap;
    use std::env;
    use std::fs;

    use std::panic;
    use std::path::Path;
    use std::str::from_utf8;
    use std::sync::{ONCE_INIT, Once};


    quickcheck! {
        /*
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
        }*/
        
        fn finds_merged(docs1: HashMap<String, String>, docs2: HashMap<String, String>) -> TestResult {
            if docs1.is_empty() {
                return TestResult::discard();
            }
            if docs1.values().find(|&s| s.is_empty()).is_some(){
                return TestResult::discard();
            }
            if docs1.keys().find(|&s| s.is_empty()).is_some(){
                return TestResult::discard();
            }
            if docs2.is_empty() {
                return TestResult::discard();
            }
            if docs2.values().find(|&s| s.is_empty()).is_some(){
                return TestResult::discard();
            }
            if docs2.keys().find(|&s| s.is_empty()).is_some(){
                return TestResult::discard();
            }
    


            let index_path = env::current_dir().expect("failed to get current dir").join(&Path::new("tmp/tests/index"));
            if index_path.exists() {
                fs::remove_dir_all(&index_path).expect("could not delete directory for test");
            }
            let features : Vec<Box<seg::Feature>> = vec![
                Box::new(StringIndex::new("value", Box::from(NoopAnalyzer{}))),
                Box::new(StringValues::new("key")),
            ];
            let index = seg::Index::new(seg::SegmentSchema{features}, &index_path);
            {
                let mut builder = index.new_segment();
                for (key, value) in docs1.iter(){
                    builder.add_doc(
                        vec![
                            seg::Field {name: "key",value: seg::FieldValue::StringField(vec![key.clone()])},
                            seg::Field {name: "value",value: seg::FieldValue::StringField(vec![value.clone()])},
                        ]);
                }
                builder.commit().expect("Failed to commit first segment");
            }
            {
                let mut builder = index.new_segment();
                for (key, value) in docs2.iter(){
                    builder.add_doc(
                        vec![
                            seg::Field {name: "key",value: seg::FieldValue::StringField(vec![key.clone()])},
                            seg::Field {name: "value",value: seg::FieldValue::StringField(vec![value.clone()])},
                        ]);
                }
                builder.commit().expect("Failed to commit second segment");
            }
            index.merge(&index.list_segments()).expect("Could not merge segments");
            let indexReader = index.open_reader();
            let ref reader = &indexReader.segment_readers()[0];
            for (key, value) in docs1.iter() {
                for doc in reader.string_index("value").expect("Could not read string index 1").doc_iter("value", &value).expect("Could not read doc iter") {
                    let docid = doc.expect("Could not read docid");
                    let actual_keys = reader.string_values("key").expect("No file").read_values(docid).expect("Could not read values");
                    let actual_key = &actual_keys[0];
                    if from_utf8(&actual_key).unwrap() != key {
                        return TestResult::failed();
                    }
                }
            }
            for (key, value) in docs2.iter() {
                for doc in reader.string_index("value").expect("Could not read string index 1").doc_iter("value", &value).expect("Could not read doc iter") {
                    let docid = doc.expect("Could not read docid");
                    let actual_keys = reader.string_values("key").expect("No file").read_values(docid).expect("Could not read values");
                    let actual_key = &actual_keys[0];
                    if from_utf8(&actual_key).unwrap() != key {
                        return TestResult::failed();
                    }
                }
            }
            for (key, value) in docs2.iter() {
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
