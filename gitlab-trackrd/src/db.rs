//! Generic redb-backed key/value store.
//!
//! `KvStore<K, V>` wraps a single redb table and exposes a typed
//! `get`/`put`/`remove` interface.  All redb transaction plumbing is hidden
//! here; callers never touch a transaction.
//!
//! The `Owned` trait bridges redb's borrowed `SelfType<'_>` and the owned
//! form returned from `get`.  Blanket impls cover `&[u8]`, `&str`, and
//! the common integer types.  For domain structs, use `impl_redb_json_value!`
//! to generate a JSON-serialized `redb::Value` + `Owned` implementation.

use std::path::Path;
use std::sync::Arc;

use redb::{Database, Key, ReadableDatabase, ReadableTable, TableDefinition, Value};

use crate::error::{Error, Result};

// ── Owned trait ──────────────────────────────────────────────────────────────

/// Converts a borrowed redb value into an owned form.
///
/// Implement this for any redb [`Value`] type you want to store in
/// [`KvStore`].  The blanket impls below cover `&[u8]`, `&str`, and the
/// common integer types.  For domain structs, use [`impl_redb_json_value!`].
pub trait Owned: Value + 'static {
    type OwnedType: 'static;
    fn to_owned<'a>(v: Self::SelfType<'a>) -> Self::OwnedType
    where
        Self: 'a;
}

impl Owned for &'static [u8] {
    type OwnedType = Vec<u8>;
    fn to_owned<'a>(v: &'a [u8]) -> Vec<u8>
    where
        Self: 'a,
    {
        v.to_vec()
    }
}

impl Owned for &'static str {
    type OwnedType = String;
    fn to_owned<'a>(v: &'a str) -> String
    where
        Self: 'a,
    {
        v.to_string()
    }
}

macro_rules! impl_owned_copy {
    ($($t:ty),*) => {
        $(impl Owned for $t {
            type OwnedType = $t;
            fn to_owned<'a>(v: Self::SelfType<'a>) -> $t where Self: 'a { v }
        })*
    };
}
impl_owned_copy!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);

// ── JSON value macro ──────────────────────────────────────────────────────────

/// Implement [`redb::Value`] and [`Owned`] for a type via JSON serialization.
///
/// `SelfType<'a>` is the type itself (always owned), so there is no borrowing
/// from the raw database bytes.
///
/// ```rust,ignore
/// impl_redb_json_value!(MyStruct, "MyStruct");
/// ```
#[macro_export]
macro_rules! impl_redb_json_value {
    ($t:ty, $name:expr) => {
        impl redb::Value for $t {
            type SelfType<'a>
                = $t
            where
                Self: 'a;
            type AsBytes<'a>
                = Vec<u8>
            where
                Self: 'a;

            fn fixed_width() -> Option<usize> {
                None
            }

            fn from_bytes<'a>(data: &'a [u8]) -> $t
            where
                Self: 'a,
            {
                serde_json::from_slice(data)
                    .unwrap_or_else(|e| panic!("corrupt db entry for {}: {e}", $name))
            }

            fn as_bytes<'a, 'b: 'a>(value: &'a $t) -> Vec<u8>
            where
                Self: 'a + 'b,
            {
                serde_json::to_vec(value)
                    .unwrap_or_else(|e| panic!("failed to serialize {}: {e}", $name))
            }

            fn type_name() -> redb::TypeName {
                redb::TypeName::new($name)
            }
        }

        impl $crate::db::Owned for $t {
            type OwnedType = $t;
            fn to_owned<'a>(v: $t) -> $t
            where
                Self: 'a,
            {
                v
            }
        }
    };
}

// ── KvStore ───────────────────────────────────────────────────────────────────

/// A single-table redb database with typed key/value access.
///
/// `K` is the redb key type (e.g. `&str`, `u64`).
/// `V` is the value type; use [`impl_redb_json_value!`] to make domain structs
/// usable as `V`, or use the built-in `Owned` impls for primitives and slices.
///
/// Clone is cheap — the inner `Arc<Database>` is reference-counted.
pub struct KvStore<K: Key + 'static, V: Owned> {
    db: Arc<Database>,
    table: TableDefinition<'static, K, V>,
}

impl<K: Key + 'static, V: Owned> Clone for KvStore<K, V> {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            table: self.table,
        }
    }
}

impl<K: Key + 'static, V: Owned> KvStore<K, V> {
    /// Open (or create) the database at `path`, ensuring `table` exists.
    ///
    /// Creates all parent directories as needed.
    pub fn open(path: &Path, table: TableDefinition<'static, K, V>) -> Result<Self> {
        let parent = path
            .parent()
            .ok_or(Error::Db("db path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;
        let db = Database::create(path)?;
        {
            let txn = db.begin_write()?;
            txn.open_table(table)?;
            txn.commit()?;
        }
        Ok(Self {
            db: Arc::new(db),
            table,
        })
    }

    /// Return the stored value for `key`, or `None` if absent.
    pub fn get<'k>(&self, key: K::SelfType<'k>) -> Result<Option<V::OwnedType>>
    where
        K: 'k,
    {
        let txn = self.db.begin_read()?;
        let tbl = txn.open_table(self.table)?;
        Ok(tbl.get(key)?.map(|g| V::to_owned(g.value())))
    }

    /// Insert or overwrite `key` with `value`.
    pub fn put<'k, 'v>(&self, key: K::SelfType<'k>, value: V::SelfType<'v>) -> Result<()>
    where
        K: 'k,
        V: 'v,
    {
        let txn = self.db.begin_write()?;
        {
            let mut tbl = txn.open_table(self.table)?;
            tbl.insert(key, value)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Remove `key`; a missing key is silently ignored.
    pub fn remove<'k>(&self, key: K::SelfType<'k>) -> Result<()>
    where
        K: 'k,
    {
        let txn = self.db.begin_write()?;
        {
            let mut tbl = txn.open_table(self.table)?;
            tbl.remove(key)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Drop every entry in the table.
    pub fn clear(&self) -> Result<()> {
        let txn = self.db.begin_write()?;
        txn.delete_table(self.table)?;
        txn.open_table(self.table)?;
        txn.commit()?;
        Ok(())
    }
}

impl<V: Owned> KvStore<u64, V> {
    /// Iterate all entries, applying `f(key, owned_value)` to each.
    ///
    /// Specialized to `u64` keys — the only current caller that needs a full
    /// table scan.
    pub fn scan<U, F>(&self, mut f: F) -> Result<Vec<U>>
    where
        F: FnMut(u64, V::OwnedType) -> Result<U>,
    {
        let txn = self.db.begin_read()?;
        let tbl = txn.open_table(self.table)?;
        let mut out = Vec::new();
        for result in tbl.iter()? {
            let (k, v) = result?;
            out.push(f(k.value(), V::to_owned(v.value()))?);
        }
        Ok(out)
    }
}
