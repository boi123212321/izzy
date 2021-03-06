use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Clone, Serialize, Deserialize)]
pub struct Index {
  pub key: String,
  pub data: HashMap<String, HashSet<String>>,
}
