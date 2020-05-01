#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;
#[macro_use]
extern crate serde_derive;
extern crate pct_str;

use rocket::config::{Config, Environment};
use rocket_contrib::json::{Json, JsonValue};
use std::env;

mod collection;
mod index;

#[get("/")]
fn root() -> Json<JsonValue> {
  Json(json!({
    "version": "0.0.10"
  }))
}

fn main() {
  let mut config = Config::build(Environment::Production)
    .unwrap();

  let args: Vec<String> = env::args().collect();

  config.port = 7999;

  for (i, arg) in args.iter().enumerate() {
    if arg.cmp(&String::from("--port")) == std::cmp::Ordering::Equal {
      let port_num = args[i + 1].parse();
      if !port_num.is_err() {
        config.port = port_num.unwrap();
      }
    }
  }

  let app = rocket::custom(config);

  app
  .mount("/", routes![root])
  .mount("/collection", collection::routes())
  .launch();
}
