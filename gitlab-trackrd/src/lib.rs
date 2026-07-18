//! Library surface for `gitlab-trackrd`.
//!
//! The daemon itself lives in `main.rs`. This library target exists so the
//! crate's binaries (`gitlab-trackrd`, `gen-config-template`) and the local
//! Criterion benches can link the daemon internals; it is **not** a public
//! API. Everything here is an implementation detail with no stability
//! guarantees — the crate is consumed as binaries only.

pub mod boards;
pub mod cache;
pub mod config;
pub mod db;
pub mod error;
pub mod gitlab;
pub mod handlers;
pub mod history;
pub mod queue;
pub mod reconnect;
pub mod refresh_meta;
pub mod reload;
pub mod search;
pub mod secrets;
pub mod server;
pub mod service;
