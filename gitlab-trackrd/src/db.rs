//! Generic fjall-backed key/value store.
//!
//! `KvStore<K, V>` wraps a single fjall keyspace and exposes a typed
//! `get`/`put`/`remove` interface over JSON-serialized values.  Keys are
//! encoded via [`KeyBytes`]; values only need serde `Serialize` +
//! `DeserializeOwned`.
//!
//! Durability is chosen per store: [`KvStore::open`] leaves writes to the
//! journal's lazy flushing (crash-consistent, but the newest writes may be
//! lost on power failure — fine for re-syncable caches), while
//! [`KvStore::open_durable`] fsyncs after every mutation (queue stores).
//!
//! A corrupt stored value surfaces as `Err(Error::Json)` from `get`/`scan`
//! rather than panicking.

use std::marker::PhantomData;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// Encodes a key as fjall bytes.
///
/// Integers use big-endian so byte order matches numeric order for
/// non-negative values (negative `i64`s would mis-sort, but no `i64` store
/// iterates); `&str` is its UTF-8 bytes.
pub trait KeyBytes: Copy {
    type Bytes: AsRef<[u8]>;
    fn to_key_bytes(self) -> Self::Bytes;
}

impl KeyBytes for u64 {
    type Bytes = [u8; 8];
    fn to_key_bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl KeyBytes for i64 {
    type Bytes = [u8; 8];
    fn to_key_bytes(self) -> [u8; 8] {
        self.to_be_bytes()
    }
}

impl<'a> KeyBytes for &'a str {
    type Bytes = &'a [u8];
    fn to_key_bytes(self) -> &'a [u8] {
        self.as_bytes()
    }
}

/// A single fjall keyspace with typed key/value access.
///
/// Clone is cheap — `Database` and `Keyspace` are reference-counted handles.
pub struct KvStore<K, V> {
    /// Kept alongside the keyspace because `persist` lives on the database.
    db: Database,
    ks: Keyspace,
    durable: bool,
    _types: PhantomData<fn(K) -> V>,
}

impl<K, V> Clone for KvStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            ks: self.ks.clone(),
            durable: self.durable,
            _types: PhantomData,
        }
    }
}

impl<K: KeyBytes, V: Serialize + DeserializeOwned> KvStore<K, V> {
    /// Open (or create) the keyspace `name` with lazy durability.
    pub fn open(db: &Database, name: &str) -> Result<Self> {
        Self::open_inner(db, name, false)
    }

    /// Open (or create) the keyspace `name`, fsyncing after every mutation.
    pub fn open_durable(db: &Database, name: &str) -> Result<Self> {
        Self::open_inner(db, name, true)
    }

    fn open_inner(db: &Database, name: &str, durable: bool) -> Result<Self> {
        Ok(Self {
            db: db.clone(),
            ks: db.keyspace(name, KeyspaceCreateOptions::default)?,
            durable,
            _types: PhantomData,
        })
    }

    fn persist_if_durable(&self) -> Result<()> {
        if self.durable {
            self.db.persist(PersistMode::SyncAll)?;
        }
        Ok(())
    }

    /// Return the stored value for `key`, or `None` if absent.
    pub fn get(&self, key: K) -> Result<Option<V>> {
        let Some(bytes) = self.ks.get(key.to_key_bytes())? else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    /// Insert or overwrite `key` with `value`.
    pub fn put(&self, key: K, value: &V) -> Result<()> {
        self.ks
            .insert(key.to_key_bytes().as_ref(), serde_json::to_vec(value)?)?;
        self.persist_if_durable()
    }

    /// Remove `key`; a missing key is silently ignored.
    pub fn remove(&self, key: K) -> Result<()> {
        self.ks.remove(key.to_key_bytes().as_ref())?;
        self.persist_if_durable()
    }

    /// Drop every entry in the keyspace.
    pub fn clear(&self) -> Result<()> {
        self.ks.clear()?;
        self.persist_if_durable()
    }
}

impl<V: Serialize + DeserializeOwned> KvStore<u64, V> {
    /// Iterate all entries, applying `f(key, value)` to each.
    ///
    /// Specialized to `u64` keys — the only current callers that need a full
    /// scan.
    pub fn scan<U, F>(&self, mut f: F) -> Result<Vec<U>>
    where
        F: FnMut(u64, V) -> Result<U>,
    {
        let mut out = Vec::new();
        for guard in self.ks.iter() {
            let (k, v) = guard.into_inner()?;
            let key = u64::from_be_bytes(
                k.as_ref()
                    .try_into()
                    .map_err(|_| Error::Db("non-u64 key in keyspace"))?,
            );
            out.push(f(key, serde_json::from_slice(&v)?)?);
        }
        Ok(out)
    }
}
