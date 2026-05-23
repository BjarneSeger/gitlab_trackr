use std::path::PathBuf;

use clap::CommandFactory;
use clap_complete::{
    generate_to,
    shells::{Bash, Fish, Zsh},
};
use clap_complete_nushell::Nushell;

// Share the CLI definition without duplicating it.
mod cli {
    include!("src/cli.rs");
}

fn main() {
    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("completions");
    std::fs::create_dir_all(&out_dir).unwrap();
    let mut cmd = cli::Cli::command();
    generate_to(Bash, &mut cmd, "tt", &out_dir).unwrap();
    generate_to(Fish, &mut cmd, "tt", &out_dir).unwrap();
    generate_to(Zsh, &mut cmd, "tt", &out_dir).unwrap();
    generate_to(Nushell, &mut cmd, "tt", &out_dir).unwrap();
}
