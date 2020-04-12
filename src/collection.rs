use std::collections::{HashMap, BTreeMap, VecDeque};
use std::vec::Vec;
use std::sync::Mutex;

use lazy_static::lazy_static;
use rocket_contrib::json::{Json, JsonValue};
use rocket::http::{Status, ContentType};
use serde_json::{Value};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::Path;
use std::fs::{File, rename};
use std::io::{BufRead, BufReader};
use pct_str::PctStr;
use rocket::response;
use rocket::response::{Responder, Response};
use rocket::request::Request;
use std::time::{SystemTime, Instant};

use crate::index;

#[derive(Debug)]
struct ApiResponse {
  json: JsonValue,
  status: Status,
}

impl<'r> Responder<'r> for ApiResponse {
  fn respond_to(self, req: &Request) -> response::Result<'r> {
    Response::build_from(self.json.respond_to(&req).unwrap())
      .status(self.status)
      .header(ContentType::JSON)
      .ok()
  }
}

#[derive(Clone, Serialize, Deserialize)]
struct Collection {
  name: String,
  data: HashMap<String, String>,
  file: Option<String>,
  indexes: HashMap<String, index::Index>,
  query_times: VecDeque<(u64, u64)>
}

lazy_static! {
  static ref COLLECTIONS: Mutex<HashMap<String, Collection>> = Mutex::new(HashMap::new());
}

fn parse_json(datastr: String) -> Value {
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
    let line = serde_json::to_string(&json_content).unwrap().to_string();
    append_to_file(collection.file.as_ref().unwrap().to_string(), line);
  }
  
  collection.data.insert(id.clone(), serde_json::to_string(&json_content).unwrap());

  for (_name, index) in collection.indexes.iter_mut() {
    let key_value = json_content[index.key.clone()].as_str().unwrap_or("$$null");
    // println!("Indexing {:?}/{:?}/{:?}", collection.name, key_value, id);
    if !index.data.contains_key(key_value) {
      println!("New index tree {:?} -> {:?}", key_value, id);
      let mut tree = HashMap::new();
      tree.insert(id.clone(), serde_json::to_string(&json_content).unwrap());
      index.data.insert(key_value.to_string(), tree);
    }
    else {
      // println!("Inserting into index tree {:?} -> {:?}", key_value, id);
      let tree = index.data.get_mut(key_value).unwrap();
      tree.insert(id.clone(), serde_json::to_string(&json_content).unwrap());
    }
  }
}

fn remove_from_collection(collection: &mut Collection, id: String, modify_fs: bool) -> Value {
  if modify_fs && !collection.file.is_none() {
    append_delete_marker(collection.file.as_ref().unwrap().to_string(), id.clone());
  }

  if collection.data.contains_key(&id) {
    let item = collection.data.remove(&id).unwrap();
    let parsed = parse_json(item);

    for (_name, index) in collection.indexes.iter_mut() {
      let key_value = parsed[index.key.clone()].as_str().unwrap_or("$$null");
      // println!("Unindexing {:?}/{:?}", name, key_value);
      if index.data.contains_key(key_value) {
        // println!("Unindexing from index tree {:?} -> {:?}", key_value, id);
        let tree = index.data.get_mut(key_value).unwrap();
        tree.remove(&id);
      }
    }

    return parsed;
  }
  return parse_json("null".to_string());
}

#[post("/compact/<name>", rank = 0)]
fn compact_collection(name: String) -> Status {
  println!("Trying to compact {:?}...", name);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return Status::NotFound;
  }
  else {
    let collection = collection_map.get_mut(&name).unwrap();
    let old_filename = collection.file.as_ref().unwrap();
    let filename = format!("{}~", old_filename);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename.clone())
        .unwrap();
    for value in collection.data.values() {
      if let Err(e) = writeln!(file, "{}", value) {
        eprintln!("Couldn't write to file: {}", e);
      }
    }
    println!("Finalising compaction {} -> {}", filename, old_filename);
    rename(filename, old_filename).unwrap();
    return Status::Ok;
  }
}

#[delete("/<name>/<id>")]
fn delete_item(name: String, id: String) -> ApiResponse {
  // println!("Trying to delete {:?}/{:?}...", name, id);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get_mut(&name).unwrap();
    if !collection.data.contains_key(&id) {
      return ApiResponse {
        json: json!({
          "status": 404,
          "message": "Item not found",
          "error": true
        }),
        status: Status::NotFound
      }
    }
    else {
      let item = remove_from_collection(collection, id, true);
      return ApiResponse {
        json: json!(item),
        status: Status::Ok
      }
    }
  }
}

#[get("/<name>/<index>/<key>")]
fn retrieve_indexed(name: String, index: String, key: Option<String>) -> ApiResponse {
  let key_value;
  if key.is_none() {
    key_value = String::from("$$null");
  }
  else {
    key_value = String::from(key.clone().unwrap());
  }
  println!("Trying to retrieve indexed {:?}/{:?}/{:?}...", name, index, key_value);
  let collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get(&name).unwrap();
    if !collection.indexes.contains_key(&index) {
      return ApiResponse {
        json: json!({
          "status": 404,
          "message": "Item not found",
          "error": true
        }),
        status: Status::NotFound
      }
    }
    else {
      let index_obj = collection.indexes.get(&index).unwrap();

      let result_tree = index_obj.data.get(&key_value);

      if result_tree.is_none() {
        return ApiResponse {
          json: json!({
            "items": []
          }),
          status: Status::Ok
        }
      }
      else {
        let results: Vec<_> = result_tree.unwrap().values().collect();
        let parsed_results: Vec<_> = results
          .into_iter()
          .map(|x| parse_json(x.to_string()))
          .collect();
        return ApiResponse {
          json: json!({
            "items": parsed_results
          }),
          status: Status::Ok
        }
      }
    }
  }
}

#[get("/<name>/<id>", rank = 1)]
fn retrieve_item(name: String, id: String) -> ApiResponse {
  println!("Trying to retrieve {:?}/{:?}...", name, id);
  let now = Instant::now();
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get_mut(&name).unwrap();
    if !collection.data.contains_key(&id) {
      return ApiResponse {
        json: json!({
          "status": 404,
          "message": "Item not found",
          "error": true
        }),
        status: Status::NotFound
      }
    }
    else {
      let item = collection.data.get(&id).unwrap();

      let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64;
      let query_time = now.elapsed().as_nanos() as u64;
      collection.query_times.push_back(
        (timestamp, query_time)
      );
      if collection.query_times.len() > 2500 {
        collection.query_times.pop_front().unwrap();
      }

      return ApiResponse {
        json: json!(parse_json(item.to_string())),
        status: Status::Ok
      }
    }
  }
}

#[derive(Clone, Serialize, Deserialize)]
struct BulkOptions {
  items: Vec<String>
}

#[post("/<name>/bulk", data="<input>")]
fn retrieve_bulk(name: String, input: Json<BulkOptions>) -> ApiResponse {
  println!("Trying to retrieve bulk {:?}...", name);
  let mut collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get_mut(&name).unwrap();

    let results: Vec<_> = input.into_inner().items
      .into_iter()
      .map(|x| {
        let item = collection.data.get(&x);
        if item.is_none() {
          return parse_json(String::from("null"));
        }
        return parse_json(item.unwrap().to_string());
      })
      .collect();

    return ApiResponse {
      json: json!({
        "items": results
      }),
      status: Status::Ok
    }
  }
}

#[post("/<name>/<id>", data = "<input>", rank = 1)]
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

#[post("/<name>", data = "<data>", rank = 7)]
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
      query_times: VecDeque::new()
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

        for (_index, line) in reader.lines().enumerate() {
          let line = line.unwrap();

          if line.len() > 0 {
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
fn delete_collection(name: String) -> Status {
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

#[get("/<name>")]
fn get_collection(name: String) -> ApiResponse {
  let collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get(&name).unwrap();
    let results: Vec<_> = collection.data.values().collect();
    let parsed_results: Vec<_> = results
          .into_iter()
          .map(|x| parse_json(x.to_string()))
          .collect();
    return ApiResponse {
      json: json!({
        "items": parsed_results
      }),
      status: Status::Ok
    }
  }
}

#[get("/<name>/times")]
fn get_times(name: String) -> ApiResponse {
  let collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get(&name).unwrap();
    return ApiResponse {
      json: json!({
        "query_times": collection.query_times
      }),
      status: Status::Ok
    }
  }
}

#[get("/<name>/count")]
fn get_count(name: String) -> ApiResponse {
  let collection_map = COLLECTIONS.lock().unwrap();
  if !collection_map.contains_key(&name) {
    return ApiResponse {
      json: json!({
        "status": 404,
        "message": "Collection not found",
        "error": true
      }),
      status: Status::NotFound
    }
  }
  else {
    let collection = collection_map.get(&name).unwrap();
    return ApiResponse {
      json: json!({
        "count": collection.data.len()
      }),
      status: Status::Ok
    }
  }
}

#[delete("/")]
fn reset() -> Status {
  let mut collection_map = COLLECTIONS.lock().unwrap();
  collection_map.clear();
  collection_map.shrink_to_fit();
  return Status::Ok;
}

pub fn routes() -> std::vec::Vec<rocket::Route> {
  routes![retrieve_bulk, get_times, get_count, compact_collection, get_collection, create_index, create, /*get,*/ reset, delete_collection, insert_item, retrieve_item, retrieve_indexed, delete_item]
}
