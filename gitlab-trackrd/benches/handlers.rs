//! The remaining pure-cache-reader handlers: the assigned-MR view and the
//! history merge, plus the pure timelog-enrichment join.

mod support;

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gitlab_trackr_api::{
    AsyncCall, Call_GetAssignedMergeRequests, Call_GetHistory, Issue, VarlinkInterface,
};
use gitlab_trackrd::gitlab::{FetchedTimelog, Issuable};
use gitlab_trackrd::handlers::enrich_timelog;
use gitlab_trackrd::search::SearchMr;

use support::{
    dormant_env, now_secs, search_mr, seed_history, seed_issue_cache, seed_mr_corpus, wire_issue,
};

const SIZES: [u64; 3] = [1_000, 10_000, 50_000];

fn assigned_mrs(c: &mut Criterion) {
    let mut group = c.benchmark_group("assigned_mrs");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    for n in SIZES {
        let env = dormant_env();
        seed_mr_corpus(&env, n);
        group.throughput(Throughput::Elements(n));
        for (variant, groups) in [
            ("all", None),
            // Adds the per-MR namespace_of + in_group pass.
            ("group_filter", Some(vec!["team".to_string()])),
        ] {
            group.bench_with_input(BenchmarkId::new(variant, n), &n, |b, _| {
                b.to_async(&env.rt).iter(|| {
                    let groups = groups.clone();
                    let h = &env.h;
                    async move {
                        let mut call = AsyncCall::default();
                        h.get_assigned_merge_requests(
                            &mut call as &mut dyn Call_GetAssignedMergeRequests,
                            groups,
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

fn get_history(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_history");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    let now = now_secs();
    for n in SIZES {
        let env = dormant_env();
        seed_history(&env, n, now);
        seed_issue_cache(&env, 1_000);
        group.throughput(Throughput::Elements(n));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.to_async(&env.rt).iter(|| {
                let h = &env.h;
                async move {
                    let mut call = AsyncCall::default();
                    h.get_history(&mut call as &mut dyn Call_GetHistory, Some(7))
                        .await
                        .unwrap();
                    black_box(call.take_reply())
                }
            });
        });
    }
    group.finish();
}

fn enrich(c: &mut Criterion) {
    let issues: Vec<Issue> = (0..1_000).map(wire_issue).collect();
    let by_url: HashMap<&str, &Issue> = issues.iter().map(|i| (i.web_url.as_str(), i)).collect();
    let by_iid: HashMap<i64, &Issue> = issues.iter().map(|i| (i.iid, i)).collect();
    let mrs: Vec<SearchMr> = (0..500).map(search_mr).collect();
    let mr_by_url: HashMap<&str, &SearchMr> = mrs.iter().map(|m| (m.web_url.as_str(), m)).collect();
    // project_id 0 forces the cache-fallback joins; URLs reuse the corpus
    // generators so lookups mostly hit, like a warm production cache.
    let timelogs: Vec<FetchedTimelog> = (0..1_000u64)
        .map(|i| {
            let (kind, web_url) = if i % 4 == 0 {
                (Issuable::MergeRequest, search_mr(i % 500).web_url)
            } else {
                (Issuable::Issue, wire_issue(i).web_url)
            };
            FetchedTimelog {
                timelog_id: i,
                spent_at_secs: 1_700_000_000 + i,
                kind,
                project_id: 0,
                iid: (i % 1_000 + 1) as i64,
                title: format!("Timelog {i}"),
                web_url,
                duration: "1h".to_string(),
                summary: String::new(),
            }
        })
        .collect();

    let mut group = c.benchmark_group("enrich_timelog");
    group.throughput(Throughput::Elements(timelogs.len() as u64));
    group.bench_function("join_1k", |b| {
        b.iter(|| {
            timelogs
                .iter()
                .cloned()
                .map(|t| enrich_timelog(t, &by_url, &by_iid, &mr_by_url))
                .collect::<Vec<_>>()
        });
    });
    group.finish();
}

criterion_group!(benches, assigned_mrs, get_history, enrich);
criterion_main!(benches);
