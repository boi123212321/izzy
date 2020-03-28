use std::collections::BTreeMap;
use std::collections::HashMap;
use std::vec::Vec;
use std::sync::Mutex;

use lazy_static::lazy_static;
use rocket_contrib::json::{Json, JsonValue};
use rocket::http::Status;
use serde_json::{Value};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::index;

#[derive(Clone, Serialize, Deserialize)]
struct Collection {
  name: String,
  data: HashMap<String, Value>,
  file: Option<String>,
  indexes: HashMap<String, index::Index>
}

lazy_static! {
  static ref COLLECTIONS: Mutex<HashMap<String, Collection>> = Mutex::new(HashMap::new());
}

fn copy_json(value: &Value) -> Value {
  return parse_json(serde_json::to_string(value).unwrap());
}

fn parse_json(datastr: String) -> Value {
  return serde_json::from_str(&datastr).unwrap();
}

fn get_json(value: Json<JsonValue>) -> Value {
  let datastr = value.to_string();
  return serde_json::from_str(&datastr).unwrap();
}

fn append_to_file(file: String, line: String) {
  let mut file = OpenOptions::new()
    .create(true)
    .append(true)
    .open(file)
    .unwrap();
  if let Err(e) = writeln!(file, "{}", line) {
    eprintln!("Couldn't write to file: {}", e);
  }
}

fn append_delete_marker(file: String, id: String) {
  let mut file = OpenOptions::new()
    .create(true)
    .append(true)
    .open(file)
    .unwrap();
  let line = format!("{{\"$$deleted\":true,\"_id\":\"{}\"}}", id);
  if let Err(e) = writeln!(file, "{}", line) {
    eprintln!("Couldn't write to file: {}", e);
  }
}

fn insert_into_collection(collection: &mut Collection, id: String, json_content: Value, modify_fs: bool) {
  if modify_fs && !collection.file.is_none() {
    let line = serde_json::to_string(&json_content).unwrap();
    append_to_file(collection.file.as_ref().unwrap().to_string(), line);
  }
  
  collection.data.insert(id.clone(), copy_json(&json_content));

  for (_name, index) in collection.indexes.iter_mut() {
    let key_value = json_content[index.key.clone()].as_str().unwrap_or("$$null");
    println!("Indexing {:?}/{:?}/{:?}", collection.name, key_value, id);
    if !index.data.contains_key(key_value) {
      println!("New index tree {:?} -> {:?}", key_value, id);
      let mut tree = HashMap::new();
      tree.insert(id.clone(), parse_json(json_content.to_string()));
      index.data.insert(key_value.to_string(), tree);
    }
    else {
      println!("Inserting into index tree {:?} -> {:?}", key_value, id);
      let tree = index.data.get_mut(key_value).unwrap();
      tree.insert(id.clone(), parse_json(json_content.to_string()));
    }
  }
}

fn remove_from_collection(collection: &mut Collection, id: String, modify_fs: bool) -> Value {
  if modify_fs && !collection.file.is_none() {
    append_delete_marker(collection.file.as_ref().unwrap().to_string(), id.clone());
  }

  let item = collection.data.remove(&id).unwrap();

  for (name, index) in collection.indexes.iter_mut() {
    let key_value = item[index.key.clone()].as_str().unwrap();
    println!("Unindexing {:?}/{:?}", name, key_value);
    if index.data.contains_key(key_value) {
      println!("Unindexing from index tree {:?} -> {:?}", key_value, id);
      let tree = index.data.get_mut(key_value).unwrap();
      tree.remove(&id);
    }
  }

  return item;
}

#[delete("/<name>/<id>")]
fn delete_item(name: String, id: String) -> Json<JsonValue> {
  println!("Trying to delete {:?}/{:?}...", name, id);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return Json(json!({
      "status": 404,
      "message": "Not found",
      "error": true
    }));
  }
  else {
    let collection = collection_map.get_mut(&name).unwrap();
    if !collection.data.contains_key(&id) {
      return Json(json!({
        "status": 404,
        "message": "Not found",
        "error": true
      }));
    }
    else {
      let item = remove_from_collection(collection, id, true);
      return Json(json!(item));
    }
  }
}

#[get("/<name>/<index>/<key>")]
fn retrieve_indexed(name: String, index: String, key: String) -> Json<JsonValue> {
  println!("Trying to retrieve indexed {:?}/{:?}/{:?}...", name, index, key);
  let collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return Json(json!({
      "status": 404,
      "message": "Not found",
      "error": true
    }));
  }
  else {
    let collection = collection_map.get(&name).unwrap();
    if !collection.indexes.contains_key(&index) {
      return Json(json!({
        "status": 404,
        "message": "Not found",
        "error": true
      }));
    }
    else {
      let index_obj = collection.indexes.get(&index).unwrap();

      let result_tree = index_obj.data.get(&key);

      if result_tree.is_none() {
        return Json(json!({
          "items": []
        }));
      }
      else {
        let results: Vec<_> = result_tree.unwrap().values().collect();
        return Json(json!({
          "items": results
        }));
      }
    }
  }
}

#[get("/<name>/<id>")]
fn retrieve_item(name: String, id: String) -> Json<JsonValue> {
  println!("Trying to retrieve {:?}/{:?}...", name, id);
  let collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return Json(json!({
      "status": 404,
      "message": "Not found",
      "error": true
    }));
  }
  else {
    let collection = collection_map.get(&name).unwrap();
    if !collection.data.contains_key(&id) {
      return Json(json!({
        "status": 404,
        "message": "Not found",
        "error": true
      }));
    }
    else {
      let item = collection.data.get(&id).unwrap();
      return Json(json!(item));
    }
  }
}

#[post("/<name>/<id>", data = "<input>")]
fn insert_item(name: String, id: String, input: Json<JsonValue>) -> Status {
  println!("Trying to insert {:?}/{:?}...", name, id);
  let mut collection_map = COLLECTIONS.lock().unwrap();

  if !collection_map.contains_key(&name) {
    return Status::NotFound;
  }
  else {
    let json_content = parse_json(input.to_string());
    let collection = collection_map.get_mut(&name).unwrap();
    insert_into_collection(collection, id, json_content, true);
    return Status::Ok;
  }
}

#[derive(Clone, Serialize, Deserialize)]
struct CreatedIndex {
  name: String,
  key: String
}

#[derive(Clone, Serialize, Deserialize)]
struct CollectionData {
  file: Option<String>,
  indexes: Vec<CreatedIndex>
}

#[derive(Clone, Serialize, Deserialize)]
struct IndexData {
  key: String
}

#[post("/<name>/index/<index>", data = "<data>")]
fn create_index(name: String, index: String, data: Json<IndexData>) -> Status {
  println!("Trying to create index {:?}/{:?}...", name, index);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return Status::NotFound;
  }
  else {
    let collection = collection_map.get_mut(&name).unwrap();
    
    if collection.indexes.contains_key(&index) {
      return Status::Conflict;
    }
    else {
      // Create index
      let created_index = index::Index {
        key: data.key.clone(),
        data: BTreeMap::new()
      };
      collection.indexes.insert(index, created_index);
      return Status::Ok;
    }
  }
}

#[post("/<name>", data = "<data>")]
fn create(name: String, data: Json<CollectionData>) -> Status {
  println!("Trying to create collection {:?}...", name);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if collection_map.contains_key(&name) {
    return Status::Conflict;
  }
  else {
    let mut collection = Collection {
      name: name.clone(),
      data: HashMap::new(),
      file: data.file.clone(),
      indexes: HashMap::new(),
    };

    for index in data.indexes.iter() {
      // Create index
      let created_index = index::Index {
        key: index.key.clone(),
        data: BTreeMap::new()
      };
      collection.indexes.insert(index.name.clone(), created_index);
    }

    if !data.file.is_none() {
      let filename = &data.file.as_ref().unwrap().clone();
      let path = Path::new(filename);
      if path.exists() {
        println!("Reading file {}", data.file.as_ref().unwrap());

        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);

        for (index, line) in reader.lines().enumerate() {
          let line = line.unwrap();

          if line.len() > 0 {
            println!("{:?}", line);
            let json_content: Value = serde_json::from_str(&line).unwrap();

            if json_content["$$indexCreated"].is_object() {}
            else if json_content["$$deleted"].is_boolean() {
              let id = json_content["_id"].as_str().unwrap().to_string();
              remove_from_collection(&mut collection, id, false);
            }
            else {
              let id = json_content["_id"].as_str().unwrap().to_string();
              insert_into_collection(&mut collection, id, json_content, false);
            }
          }
        }
      }
    }

    collection_map.insert(name.clone(), collection);
    return Status::Ok;
  }
}

#[delete("/<name>")]
fn delete(name: String) -> Status {
  println!("Trying to delete collection {:?}...", name);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if collection_map.contains_key(&name) {
    collection_map.remove(&name);
    return Status::Ok;
  }
  else {
    return Status::NotFound;
  }
}

#[get("/")]
fn get() -> Json<JsonValue> {
  let collection_map = COLLECTIONS.lock().unwrap();
  let collections: Vec<_> = collection_map.iter().collect();
  Json(json!(collections))
}

pub fn routes() -> std::vec::Vec<rocket::Route> {
  routes![create_index, create, get, delete, insert_item, retrieve_item, retrieve_indexed, delete_item]
}
