//! One module per `tt` subcommand. Each exposes a single `run(...)` entry
//! point invoked from [`crate::main`].

pub mod assign;
pub mod close;
pub mod config;
pub mod history;
pub mod hook;
pub mod list;
pub mod log;
pub mod login;
pub mod logout;
pub mod project;
pub mod prompt;
pub mod queue;
pub mod refresh;
pub mod tick;
pub mod unassign;
pub mod whoami;
