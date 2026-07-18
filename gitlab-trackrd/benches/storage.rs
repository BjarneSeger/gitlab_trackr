//! The storage substrate: raw `KvStore` scans (fjall iteration + per-entry
//! JSON decode), the `IssueCache` whole-blob round-trip, and the scan-heavy
//! `HistoryCache`/`SearchCache` mutations.
//!
//! No `RetryQueue` benches on purpose: its stores fsync after every mutation
//! (`open_durable`), so a bench would measure the disk, not the code.

mod support;

use std::collections::HashSet;
use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gitlab_trackrd::db::KvStore;
use gitlab_trackrd::history::StoredTimelog;

use support::{
    dormant_env, now_secs, search_issue, seed_history, seed_search_corpus, stored_timelog,
    wire_issue,
};

const SIZES: [u64; 3] = [1_000, 10_000, 50_000];

fn kvstore_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("kvstore_scan");
    group.sample_size(30);
    let now = now_secs();
    for n in SIZES {
        let dir = tempfile::tempdir().unwrap();
        let db = fjall::Database::builder(dir.path().join("db"))
            .open()
            .unwrap();
        let store: KvStore<u64, StoredTimelog> = KvStore::open(&db, "bench_scan").unwrap();
        for i in 0..n {
            store.put(i, &stored_timelog(i, now)).unwrap();
        }
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(store.scan(|_, v| Ok(v)).unwrap()));
        });
    }
    group.finish();
}

fn issue_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("issue_cache");
    for n in [100u64, 1_000, 10_000] {
        let env = dormant_env();
        let issues: Vec<_> = (0..n).map(wire_issue).collect();
        // Steady-state: the single "assigned" blob is overwritten in place.
        env.h.cache.put(&issues).unwrap();
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("put", n), &n, |b, _| {
            b.iter(|| env.h.cache.put(black_box(&issues)).unwrap());
        });
        group.bench_with_input(BenchmarkId::new("get", n), &n, |b, _| {
            b.iter(|| black_box(env.h.cache.get().unwrap()));
        });
    }
    group.finish();
}

fn history(c: &mut Criterion) {
    let mut group = c.benchmark_group("history");
    group.sample_size(30);
    let now = now_secs();
    for n in SIZES {
        let env = dormant_env();
        seed_history(&env, n, now);
        // spent_at is uniform over 30 days; a 9-day cutoff selects ~30%.
        let cutoff = now - 9 * 86_400;
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("all_since", n), &n, |b, _| {
            b.iter(|| black_box(env.h.history.all_since(cutoff).unwrap()));
        });
    }
    for n in [1_000u64, 10_000] {
        let env = dormant_env();
        seed_history(&env, n, now);
        // The oldest ~10% band; each iteration clears it, setup reseeds only
        // that band so the timed scan always runs over the full n entries.
        let (band_min, band_max) = (now - 30 * 86_400, now - 27 * 86_400);
        let band: Vec<_> = (0..n)
            .map(|i| stored_timelog(i, now))
            .filter(|t| t.spent_at_secs >= band_min && t.spent_at_secs < band_max)
            .collect();
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("clear_between", n), &n, |b, _| {
            b.iter_batched(
                || env.h.history.upsert(&band).unwrap(),
                |()| black_box(env.h.history.clear_between(band_min, band_max).unwrap()),
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

fn search_cache_mut(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_cache_mut");
    group.sample_size(30);
    for n in [1_000u64, 10_000] {
        let env = dormant_env();
        seed_search_corpus(&env, n);
        // The full-resync deletion diff: keep 90%, reseed the stale 10% tail
        // each iteration so the timed scan always sees n entries.
        let keep_n = n * 9 / 10;
        let keep: HashSet<u64> = (0..keep_n).collect();
        let stale: Vec<_> = (keep_n..n).map(search_issue).collect();
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::new("retain_issues", n), &n, |b, _| {
            b.iter_batched(
                || {
                    let guard = env.h.search.try_begin_sync().unwrap();
                    guard.upsert_issues(&stale).unwrap();
                },
                |()| {
                    let guard = env.h.search.try_begin_sync().unwrap();
                    black_box(guard.retain_issues(&keep).unwrap());
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_with_input(BenchmarkId::new("update_mr", n), &n, |b, _| {
            let mut flip = false;
            b.iter(|| {
                flip = !flip;
                let guard = env.h.search.try_begin_sync().unwrap();
                black_box(
                    guard
                        .update_mr(1, 1, |m| {
                            m.state = if flip { "opened" } else { "closed" }.to_string();
                        })
                        .unwrap(),
                );
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    kvstore_scan,
    issue_cache,
    history,
    search_cache_mut
);
criterion_main!(benches);
