//! The `Search` handler over a scale-parameterized corpus — the daemon's
//! heaviest pure-local read path — plus the pure text matchers under it.

mod support;

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gitlab_trackr_api::{AsyncCall, Call_Search, VarlinkInterface};
use gitlab_trackrd::search::{parse_iid_query, text_matches};

use support::{dormant_env, seed_search_corpus};

const SIZES: [u64; 3] = [1_000, 10_000, 50_000];

fn search_handler(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_handler");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    for n in SIZES {
        let env = dormant_env();
        seed_search_corpus(&env, n);
        group.throughput(Throughput::Elements(n));
        let variants: [(&str, &str, Option<Vec<String>>); 4] = [
            // Needle matching nothing: the pure per-entry filter cost.
            ("issues_miss", "zzz-nomatch", Some(vec!["issues".into()])),
            // ~1% hits: adds sort + truncate + per-hit boards.get reads.
            ("issues_hits", "flaky", Some(vec!["issues".into()])),
            // No kind filter: all four corpora scanned.
            ("all_kinds", "flaky", None),
            // Exact-reference query: parse + iid comparison path.
            ("iid_ref", "#123", Some(vec!["issues".into()])),
        ];
        for (variant, query, kinds) in variants {
            group.bench_with_input(BenchmarkId::new(variant, n), &n, |b, _| {
                b.to_async(&env.rt).iter(|| {
                    let kinds = kinds.clone();
                    let h = &env.h;
                    async move {
                        let mut call = AsyncCall::default();
                        h.search(
                            &mut call as &mut dyn Call_Search,
                            query.to_string(),
                            kinds,
                            None,
                        )
                        .await
                        .unwrap();
                        black_box(call.take_reply())
                    }
                });
            });
        }
    }
    group.finish();
}

fn search_micro(c: &mut Criterion) {
    let mut group = c.benchmark_group("search_micro");
    let short = "Fix login bug";
    let long = "Issue 4711: flaky search sync in component 42 — investigate retry \
                backoff and dedupe on the incremental poll path (repro attached)";
    for (name, needle, hay) in [
        ("text_matches/short_miss", "zzz", short),
        ("text_matches/short_hit", "login", short),
        ("text_matches/long_miss", "zzz", long),
        ("text_matches/long_hit", "backoff", long),
    ] {
        group.bench_function(name, |b| {
            b.iter(|| text_matches(black_box(needle), black_box(hay)))
        });
    }
    group.bench_function("parse_iid_query/ref", |b| {
        b.iter(|| parse_iid_query(black_box("#4711")))
    });
    group.bench_function("parse_iid_query/text", |b| {
        b.iter(|| parse_iid_query(black_box("login bug")))
    });
    group.finish();
}

criterion_group!(benches, search_handler, search_micro);
criterion_main!(benches);
