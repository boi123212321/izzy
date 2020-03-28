use std::collections::HashMap;
use std::vec::Vec;
use std::sync::Mutex;

use lazy_static::lazy_static;
use rocket_contrib::json::{Json, JsonValue};
use rocket::http::Status;
use serde_json::{Value};
use std::fs::OpenOptions;
use std::io::prelude::*;

#[derive(Clone, Serialize, Deserialize)]
struct Collection {
  name: String,
  num_items: u32,
  data: HashMap<String, Value>,
  file: Option<String>
}

lazy_static! {
  static ref COLLECTIONS: Mutex<HashMap<String, Collection>> = Mutex::new(HashMap::new());
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
  let line = format!("{{\"$$deleted\":true,\"id\":{}}}", id);
  if let Err(e) = writeln!(file, "{}", line) {
    eprintln!("Couldn't write to file: {}", e);
  }
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
      let item = collection.data.remove(&id);
      append_delete_marker(collection.file.as_ref().unwrap().to_string(), id);
      return Json(json!(item));
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
    let json_content = get_json(input);

    let mut collection = collection_map.get_mut(&name).unwrap();

    if !collection.file.is_none() {
      let line = serde_json::to_string(&json_content).unwrap();
      append_to_file(collection.file.as_ref().unwrap().to_string(), line);
    }
    
    collection.data.insert(id, json_content);
    collection.num_items += 1;

    return Status::Ok;
  }
}

#[derive(Clone, Serialize, Deserialize)]
struct CollectionData {
  file: Option<String>
}

#[post("/<name>", data = "<data>")]
fn create(name: String, data: Json<CollectionData>) -> Status {
  println!("Trying to create collection {:?}...", name);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if collection_map.contains_key(&name) {
    return Status::Conflict;
  }
  else {
    let collection = Collection {
      name: name.clone(),
      num_items: 0,
      data: HashMap::new(),
      file: data.file.clone()
    };
    collection_map.insert(name.clone(), collection);

    // TODO: read file if it exists

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
  routes![create, get, delete, insert_item, retrieve_item, delete_item]
}
