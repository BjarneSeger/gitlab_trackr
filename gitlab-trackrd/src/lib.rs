//! Library surface for `gitlab-trackrd`.
//!
//! The daemon itself lives in `main.rs`; this crate-level library exists so
//! build/packaging tooling (the `gen-config-template` binary) can reuse the
//! self-contained [`config`] module without pulling in the daemon binary.

pub mod config;
