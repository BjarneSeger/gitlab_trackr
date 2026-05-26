//! Daemon-wide error type.
//!
//! Each `#[from]`-annotated variant lets `thiserror` derive the matching
//! `From` impl, so call sites use `?` instead of `.map_err(...)` chains.

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GitLab error: {0}")]
    Gitlab(String),

    /// Transient network error — safe to retry.
    #[error("network error: {0}")]
    Transient(String),

    #[error(transparent)]
    CacheDatabase(#[from] redb::DatabaseError),

    #[error(transparent)]
    CacheTransaction(#[from] redb::TransactionError),

    #[error(transparent)]
    CacheTable(#[from] redb::TableError),

    #[error(transparent)]
    CacheStorage(#[from] redb::StorageError),

    #[error(transparent)]
    CacheCommit(#[from] redb::CommitError),

    // `PoisonError` is generic over the guard's lifetime, which makes `#[from]`
    // awkward; the poison payload also carries no useful info, so we collapse
    // it to a payload-free variant at the lock site.
    #[error("cache lock poisoned")]
    CachePoisoned,

    #[error("cache: {0}")]
    Cache(&'static str),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Environment variable '{0}' is not set")]
    Env(&'static str),

    #[error("Varlink error: {0}")]
    Varlink(#[from] varlink::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
