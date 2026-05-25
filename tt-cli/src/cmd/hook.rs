//! `tt hook <shell>` — print the pre-prompt-hook snippet for the user's shell.
//!
//! Snippets live as plain text under `src/hooks/` (not inline) so editing one
//! doesn't require touching any Rust. They're `include_str!`'d so the binary
//! is still self-contained.

use crate::cli::Shell;

const FISH: &str = include_str!("../hooks/fish.txt");
const ZSH: &str = include_str!("../hooks/zsh.txt");
const BASH: &str = include_str!("../hooks/bash.txt");
const NU: &str = include_str!("../hooks/nu.txt");

pub fn run(shell: Shell) {
    let snippet = match shell {
        Shell::Fish => FISH,
        Shell::Zsh => ZSH,
        Shell::Bash => BASH,
        Shell::Nu => NU,
    };
    print!("{snippet}");
}
