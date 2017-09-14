#[macro_use]
extern crate quickcheck;
extern crate esmy;


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use esmy::seg;
    use std::path::Path;
    use std::str::from_utf8;
    use std::fs;
    use std::env;
    use quickcheck::TestResult;
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
            
            println!("{:?}", index_path);
            let index = seg::Index::new(&index_path);
            {
                let mut builder = index.new_segment();
                for (key, value) in docs.iter(){
                    builder.add_doc(
                        vec![
                            seg::Field {name: "key",values: vec![seg::FieldValue::StringField(key.clone())]},
                            seg::Field {name: "value",values: vec![seg::FieldValue::StringField(value.clone())]},
                        ]);
                }
                builder.commit().unwrap();
            }
            let readers = index.segment_readers();
            let ref reader = readers[0];
            for (key, value) in docs.iter() {
                for doc in reader.doc_iter("value", &value.as_bytes()).unwrap() {
                    let docid = doc.unwrap();
                    let actual_keys = reader.read_values("key", docid).unwrap();
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
