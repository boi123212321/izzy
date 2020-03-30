#![feature(proc_macro_hygiene, decl_macro)]

#[macro_use]
extern crate rocket;
#[macro_use]
extern crate rocket_contrib;
#[macro_use]
extern crate serde_derive;

use rocket::config::{Config, Environment};
use rocket_contrib::json::{Json, JsonValue};

mod collection;
mod index;

#[get("/")]
fn root() -> Json<JsonValue> {
  Json(json!({
    "version": "0.0.4"
  }))
}

fn main() {
  let mut config = Config::build(Environment::Production)
    .unwrap();

  config.port = 7999;

  let app = rocket::custom(config);

  app
  .mount("/", routes![root])
  .mount("/collection", collection::routes())
  .launch();
}
