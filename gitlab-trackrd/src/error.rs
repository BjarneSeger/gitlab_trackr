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
    DbOpen(#[from] redb::DatabaseError),

    #[error(transparent)]
    DbTransaction(#[from] redb::TransactionError),

    #[error(transparent)]
    DbTable(#[from] redb::TableError),

    #[error(transparent)]
    DbStorage(#[from] redb::StorageError),

    #[error(transparent)]
    DbCommit(#[from] redb::CommitError),

    #[error("db: {0}")]
    Db(&'static str),

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
