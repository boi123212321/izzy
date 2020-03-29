use std::collections::BTreeMap;
use std::collections::HashMap;

#[derive(Clone, Serialize, Deserialize)]
pub struct Index {
  pub key: String,
  pub data: BTreeMap<String, HashMap<String, String>>
}
