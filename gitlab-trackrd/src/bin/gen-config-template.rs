//! Print the annotated default `gitlab-trackrd` config to stdout.
//!
//! Packaging tool, not shipped to `/usr/bin`. The output is installed as the
//! package-provided default at `/usr/share/gitlab-trackrd/config.toml`:
//!
//! ```sh
//! cargo run -p gitlab-trackrd --bin gen-config-template > packaging/config.toml
//! ```

fn main() {
    print!("{}", gitlab_trackrd::config::template());
}
