use std::collections::BTreeMap;
use std::collections::HashMap;
use serde_json::{Value};

#[derive(Clone, Serialize, Deserialize)]
pub struct Index {
  pub key: String,
  pub data: BTreeMap<String, HashMap<String, Value>>
}
