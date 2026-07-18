//! Background supervisor that re-establishes the GitLab session whenever it goes
//! dormant because GitLab was unreachable — whether at boot or lost mid-run.
//!
//! `GitlabClient::connect` is otherwise only called at startup and on `tt login`,
//! and neither retries. So if the stored (known-good) token can't reach GitLab,
//! the session lands `Dormant(Unreachable)` — at boot (initial connect failed)
//! or at runtime (a background-refresh or write-handler call failed transiently
//! and called [`commit_unreachable`]) — and, without this task, would stay there
//! until the user runs `tt login` or restarts the daemon.
//!
//! The supervisor watches for that state and retries the connection with the same
//! style of exponential back-off as the retry queue (see `queue`), reusing the
//! `[reconnect]` config and the shared session slot as the integration seam. The
//! moment it flips the slot back to `Connected`, the queue worker's defer loop
//! and the background refresh loops resume on their own; we additionally nudge
//! the queue and kick an immediate refresh so recovery is instant. Between
//! engagements it parks on [`Handlers::reconnect_signal`], woken by the next
//! runtime demotion or a periodic re-check tick; a reconnect whose warm-up
//! immediately re-fails backs off before retrying (see [`supervise`]).
//!
//! Only a *transient* dormancy (`DormancyReason::is_auto_retryable`) is retried:
//! a rejected token, missing credentials, or an explicit logout all need the
//! user and would just spin.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{info, warn};

use crate::config::{SharedConfig, next_backoff};
use crate::error::{DormancyReason, Error};
use crate::gitlab::{GitlabApi, GitlabClient};
use crate::handlers::{ConnState, Handlers, Session, SessionSlot};
use crate::secrets::{self, Credentials};

/// Outcome of one connection attempt, abstracted so the loop's state machine can
/// be unit-tested without a live GitLab (mirrors the fake `GitlabApi` the queue
/// and handler tests inject).
enum Attempt {
    /// A live session was established.
    Connected(Session),
    /// Transient/network failure — keep retrying (carries a detail for logging).
    Transient(String),
    /// Permanent failure — commit this dormancy reason and stop (e.g. the token
    /// was rejected). Don't hammer auth.
    Permanent(DormancyReason),
}

/// Spawn the background reconnect supervisor. It lives for the whole daemon run:
/// a connected start pays only a task parked on [`Handlers::reconnect_signal`],
/// and it (re-)engages the retry loop whenever the session is dormant for an
/// auto-retryable reason — at boot, or after a runtime [`commit_unreachable`].
pub fn spawn(handlers: Arc<Handlers>) {
    let signal = Arc::clone(&handlers.reconnect_signal);
    let config = Arc::clone(&handlers.config);
    tokio::spawn(supervise(config, signal, move || {
        engage_once(Arc::clone(&handlers))
    }));
}

/// Outcome of one [`engage_once`], telling the supervisor how to wait before the
/// next engagement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engaged {
    /// Nothing to do, reconnected-and-healthy, or deliberately stopped — park
    /// until the next demotion signal or periodic re-check.
    Stable,
    /// Reconnected, but the post-reconnect warm-up immediately failed and
    /// re-demoted the session. Re-engaging at once would spin against a partial
    /// outage, so the supervisor backs off first.
    Flapping,
}

/// The supervisor loop, factored out of [`spawn`] so it can be unit-tested with
/// an injected `engage` step.
///
/// Two waits guard the two failure shapes:
/// * A `Flapping` engagement (reconnect succeeded but warm-up re-failed) sleeps
///   an exponential back-off before re-engaging, so a partial outage — the cheap
///   `connect` probe reachable while the heavier fetches are not — can't storm.
/// * A `Stable` engagement parks until the next runtime demotion signal *or* a
///   periodic re-check tick. The tick re-arms an already-dormant slot that no
///   signal can reach (keychain recovered, token fixed out-of-band, or
///   auto-reconnect re-enabled mid-outage), since [`commit_unreachable`] only
///   fires the signal on a live `Connected`→`Dormant` transition.
///
/// No wakeup is lost: [`commit_unreachable`] writes `Dormant` before it notifies
/// and `engage` re-reads the slot at its top, so a permit stored between `engage`
/// returning and the park below is consumed on the next iteration — the same
/// guarantee the queue worker relies on for its drain waker.
async fn supervise<E, F>(config: SharedConfig, signal: Arc<Notify>, mut engage: E)
where
    E: FnMut() -> F,
    F: Future<Output = Engaged>,
{
    let mut flap_delay: Option<Duration> = None;
    loop {
        match engage().await {
            Engaged::Flapping => {
                let (base, max) = {
                    let c = config.read().unwrap();
                    (c.reconnect.base_delay(), c.reconnect.max_delay())
                };
                let d = flap_delay.unwrap_or(base);
                warn!(
                    delay_secs = d.as_secs(),
                    "reconnected but warm-up re-failed; backing off before retrying"
                );
                tokio::time::sleep(d).await;
                flap_delay = Some(next_backoff(d, max));
                // Loop straight back into `engage`: the slot is still dormant and
                // we've already paced the retry, so don't wait for a signal.
            }
            Engaged::Stable => {
                flap_delay = None;
                // Re-read `max_delay` each park so a hot reload retunes the tick.
                let tick = config.read().unwrap().reconnect.max_delay();
                tokio::select! {
                    _ = signal.notified() => {}
                    _ = tokio::time::sleep(tick) => {}
                }
            }
        }
    }
}

/// One reconnect engagement: if the session is dormant for an auto-retryable
/// reason (and auto-reconnect is enabled), re-establish it with bounded
/// exponential back-off and, on success, run the recovery side effects. Returns
/// [`Engaged::Flapping`] when the reconnect succeeded but its warm-up immediately
/// re-demoted the session (so the supervisor backs off), or [`Engaged::Stable`]
/// otherwise. A no-op returning `Stable` when the slot isn't retryable, so a
/// connected session costs only the guard check.
///
/// The keychain is read once *per engagement*, not per retry, and only after the
/// `enabled` guard — a disabled daemon never touches it. Re-reading on every retry
/// was both wasteful and unsafe: a keychain hiccup mid-loop used to strand the
/// slot on a stale `Unreachable`, so the CLI kept claiming the daemon was
/// "retrying" while it had actually stopped. `resolve_credentials` instead
/// commits the real reason (keychain error / no credentials) and bails, and
/// `reconnect_loop` reuses the one pre-loaded credential across its retries.
/// Reading per engagement (rather than once for the daemon's life) lets a token
/// rotated by `tt login` between outages take effect.
async fn engage_once(handlers: Arc<Handlers>) -> Engaged {
    if !slot_is_retryable(&handlers.session).await {
        return Engaged::Stable;
    }
    // Bail before the keychain read and the "starting" log
    if !handlers.config.read().unwrap().reconnect.enabled {
        return Engaged::Stable;
    }
    let creds = match resolve_credentials(secrets::load().await, &handlers.session).await {
        Some(c) => c,
        None => return Engaged::Stable,
    };
    info!("GitLab unreachable; starting background auto-reconnect");
    let committed = reconnect_loop(
        Arc::clone(&handlers.session),
        Arc::clone(&handlers.config),
        || connect_once(&creds),
    )
    .await;
    if committed {
        handlers.queue.drain_waker().notify_one();
        handlers.warm_up().await;
        if slot_is_retryable(&handlers.session).await {
            return Engaged::Flapping;
        }
    }
    Engaged::Stable
}

/// Resolve the credentials for a reconnect attempt. Loading the keychain fails
/// with either "no credentials" (logged out / cleared) or a read error; both
/// mean the current `Dormant(Unreachable)` is no longer the true reason, so we
/// commit the honest one (via the same CAS as `commit_dormant`, so a racing
/// `tt login` is never clobbered) and return `None` to stop the task.
async fn resolve_credentials(
    loaded: crate::error::Result<Option<Credentials>>,
    session: &SessionSlot,
) -> Option<Credentials> {
    match loaded {
        Ok(Some(c)) => Some(c),
        Ok(None) => {
            commit_dormant(session, DormancyReason::NoCredentials).await;
            None
        }
        Err(e) => {
            commit_dormant(session, DormancyReason::KeychainError(e.to_string())).await;
            None
        }
    }
}

/// One connection attempt with pre-loaded credentials, mapped to an [`Attempt`]
/// for `reconnect_loop`. No keychain read — the credentials are loaded once by
/// the caller. A non-transient error is a rejected token (the only remaining
/// possibility once the network-error case is peeled off), so build it directly.
async fn connect_once(creds: &Credentials) -> Attempt {
    match GitlabClient::connect(&creds.host, &creds.token).await {
        Ok(client) => Attempt::Connected(Session::from_client(client)),
        Err(Error::Transient(detail)) => Attempt::Transient(detail),
        Err(e) => Attempt::Permanent(DormancyReason::TokenRejected {
            host: creds.host.clone(),
            detail: e.to_string(),
        }),
    }
}

/// Retry `connect` with exponential back-off while the session stays dormant for
/// an auto-retryable reason. Returns `true` iff it committed a fresh `Connected`
/// session (the caller then runs the recovery side effects).
async fn reconnect_loop<C, F>(session: SessionSlot, config: SharedConfig, mut connect: C) -> bool
where
    C: FnMut() -> F,
    F: Future<Output = Attempt>,
{
    let mut delay: Option<Duration> = None;
    loop {
        // Stop the instant the slot is no longer dormant-for-a-retryable-reason:
        // a concurrent `tt login` (Connected) or `tt logout` (LoggedOut) won, or
        // a previous iteration already succeeded.
        if !slot_is_retryable(&session).await {
            return false;
        }

        let (base, max, enabled) = {
            let c = config.read().unwrap();
            (
                c.reconnect.base_delay(),
                c.reconnect.max_delay(),
                c.reconnect.enabled,
            )
        };
        if !enabled {
            info!("auto-reconnect disabled in config; stopping");
            return false;
        }

        match connect().await {
            Attempt::Connected(new_session) => {
                return commit_connected(&session, new_session).await;
            }
            Attempt::Permanent(reason) => {
                commit_dormant(&session, reason).await;
                return false;
            }
            Attempt::Transient(detail) => {
                let d = delay.unwrap_or(base);
                warn!(error = %detail, delay_secs = d.as_secs(), "reconnect attempt failed; retrying");
                tokio::time::sleep(d).await;
                delay = Some(next_backoff(d, max));
            }
        }
    }
}

/// Whether the slot is currently `Dormant` for an auto-retryable reason.
async fn slot_is_retryable(session: &SessionSlot) -> bool {
    matches!(&*session.read().await, ConnState::Dormant(r) if r.is_auto_retryable())
}

/// Compare-and-set the slot to `Connected` iff it is *still* dormant-and-
/// retryable, so a racing `tt login` / `tt logout` is never clobbered. Returns
/// whether it committed.
async fn commit_connected(session: &SessionSlot, new_session: Session) -> bool {
    let mut slot = session.write().await;
    if !matches!(&*slot, ConnState::Dormant(r) if r.is_auto_retryable()) {
        return false;
    }
    info!(
        host = %new_session.host,
        user_id = new_session.user_id,
        "reconnected to GitLab"
    );
    *slot = ConnState::Connected(new_session);
    true
}

/// Commit a non-retryable dormancy (a rejected token, or missing/unreadable
/// credentials) iff the slot is still retryable, so we stop retrying without
/// clobbering a concurrent login and the CLI sees the true reason.
async fn commit_dormant(session: &SessionSlot, reason: DormancyReason) {
    let mut slot = session.write().await;
    if matches!(&*slot, ConnState::Dormant(r) if r.is_auto_retryable()) {
        warn!(reason = ?reason, "reconnect: giving up");
        *slot = ConnState::Dormant(reason);
    }
}

/// Demote a *live* session to `Dormant(Unreachable)` after a runtime GitLab call
/// on `failed_client` failed transiently, and wake the reconnect supervisor via
/// `signal`. The live session's `host` is captured so the CLI's "unreachable"
/// message stays specific.
///
/// A compare-and-set on session *identity*: the slot transitions only if it is
/// still `Connected` to the very client that failed (`Arc::ptr_eq`). This never
/// clobbers a `LoggedOut` / `TokenRejected` / `NoCredentials` reason a concurrent
/// `tt logout` / rejection set, *nor* a fresh session a concurrent `tt login`
/// established — a stale in-flight fetch against a superseded client can't tear
/// down the new connection. Repeated detections against the same client (e.g.
/// both the issues and the timelog fetch in one refresh pass) are idempotent: the
/// second sees `Dormant` and no-ops, so only the first detail and one wakeup
/// survive. Notifying only inside the transition keeps the invariant "a permit
/// means a real disconnect happened".
pub(crate) async fn commit_unreachable(
    session: &SessionSlot,
    signal: &Notify,
    failed_client: &Arc<dyn GitlabApi>,
    detail: String,
) {
    let mut slot = session.write().await;
    if let ConnState::Connected(s) = &*slot {
        if !Arc::ptr_eq(&s.gitlab, failed_client) {
            return;
        }
        let host = s.host.clone();
        warn!(host = %host, error = %detail, "GitLab unreachable at runtime; going dormant and auto-reconnecting");
        *slot = ConnState::Dormant(DormancyReason::Unreachable { host, detail });
        signal.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use tokio::sync::RwLock;

    use crate::gitlab::{FetchedTimelog, GitlabApi, Issuable, IssueWithLabels};

    struct NoopGitlab;

    #[async_trait::async_trait]
    impl GitlabApi for NoopGitlab {
        async fn fetch_assigned_issues(
            &self,
            _group: Option<String>,
        ) -> crate::error::Result<Vec<IssueWithLabels>> {
            unimplemented!()
        }
        async fn add_spent_time(
            &self,
            _k: Issuable,
            _p: i64,
            _i: i64,
            _d: &str,
            _s: Option<&str>,
        ) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn create_timelog(
            &self,
            _k: Issuable,
            _id: i64,
            _d: &str,
            _s: &str,
            _at: chrono::DateTime<chrono::Utc>,
        ) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn fetch_my_timelogs(
            &self,
            _since: chrono::DateTime<chrono::Utc>,
        ) -> crate::error::Result<Vec<FetchedTimelog>> {
            unimplemented!()
        }
        async fn close(&self, _k: Issuable, _p: i64, _i: i64) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn assign_self(&self, _k: Issuable, _p: i64, _i: i64) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn unassign_self(&self, _k: Issuable, _p: i64, _i: i64) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn fetch_board_list_labels(&self, _p: i64) -> crate::error::Result<Vec<String>> {
            unimplemented!()
        }
        async fn fetch_issues_for_search(
            &self,
            _p: Option<i64>,
            _after: Option<chrono::DateTime<chrono::Utc>>,
        ) -> crate::error::Result<Vec<crate::search::SearchIssue>> {
            unimplemented!()
        }
        async fn fetch_merge_requests_for_search(
            &self,
            _p: Option<i64>,
            _after: Option<chrono::DateTime<chrono::Utc>>,
        ) -> crate::error::Result<Vec<crate::search::SearchMr>> {
            unimplemented!()
        }
        async fn fetch_member_projects(
            &self,
        ) -> crate::error::Result<Vec<crate::search::SearchProject>> {
            unimplemented!()
        }
        async fn fetch_member_groups(
            &self,
        ) -> crate::error::Result<Vec<crate::search::SearchGroup>> {
            unimplemented!()
        }
    }

    fn unreachable_slot() -> SessionSlot {
        let reason = DormancyReason::Unreachable {
            host: "gitlab.example.com".into(),
            detail: "connection refused".into(),
        };
        Arc::new(RwLock::new(ConnState::Dormant(reason)))
    }

    /// Defaults with the back-off zeroed so retry tests don't actually sleep.
    fn instant_config() -> SharedConfig {
        let mut cfg = crate::config::defaults();
        cfg.reconnect.base_delay_secs = 0;
        cfg.reconnect.max_delay_secs = 0;
        Arc::new(std::sync::RwLock::new(cfg))
    }

    fn connected_session() -> Session {
        Session {
            gitlab: Arc::new(NoopGitlab),
            host: "gitlab.example.com".into(),
            user_id: 42,
        }
    }

    #[tokio::test]
    async fn stops_without_connecting_when_not_retryable() {
        let session: SessionSlot = Arc::new(RwLock::new(ConnState::Dormant(
            DormancyReason::TokenRejected {
                host: "gitlab.example.com".into(),
                detail: "401".into(),
            },
        )));
        let committed = reconnect_loop(session.clone(), instant_config(), || async {
            panic!("connect must not run when the slot is not auto-retryable")
        })
        .await;
        assert!(!committed);
    }

    #[tokio::test]
    async fn permanent_error_commits_token_rejected_and_stops() {
        let session = unreachable_slot();
        let committed = reconnect_loop(session.clone(), instant_config(), || async {
            Attempt::Permanent(DormancyReason::TokenRejected {
                host: "gitlab.example.com".into(),
                detail: "401".into(),
            })
        })
        .await;
        assert!(!committed);
        assert!(matches!(
            &*session.read().await,
            ConnState::Dormant(DormancyReason::TokenRejected { .. })
        ));
    }

    #[tokio::test]
    async fn missing_credentials_commits_no_credentials_and_stops() {
        let session = unreachable_slot();
        let creds = resolve_credentials(Ok(None), &session).await;
        assert!(creds.is_none());
        assert!(matches!(
            &*session.read().await,
            ConnState::Dormant(DormancyReason::NoCredentials)
        ));
    }

    #[tokio::test]
    async fn keychain_error_commits_keychain_error_and_stops() {
        let session = unreachable_slot();
        let creds =
            resolve_credentials(Err(Error::Secrets("keyring locked".into())), &session).await;
        assert!(creds.is_none());
        assert!(matches!(
            &*session.read().await,
            ConnState::Dormant(DormancyReason::KeychainError(_))
        ));
    }

    #[tokio::test]
    async fn resolve_credentials_passes_through_and_leaves_slot_when_present() {
        let session = unreachable_slot();
        let creds = resolve_credentials(
            Ok(Some(Credentials {
                host: "gitlab.example.com".into(),
                token: "t".into(),
            })),
            &session,
        )
        .await;
        assert!(creds.is_some());
        assert!(matches!(
            &*session.read().await,
            ConnState::Dormant(r) if r.is_auto_retryable()
        ));
    }

    #[tokio::test]
    async fn retries_transient_then_reconnects() {
        let session = unreachable_slot();
        let calls = AtomicUsize::new(0);
        let committed = reconnect_loop(session.clone(), instant_config(), || {
            let n = calls.fetch_add(1, SeqCst);
            async move {
                if n < 2 {
                    Attempt::Transient("connection refused".into())
                } else {
                    Attempt::Connected(connected_session())
                }
            }
        })
        .await;
        assert!(committed);
        assert_eq!(calls.load(SeqCst), 3, "two transient failures then success");
        assert!(matches!(&*session.read().await, ConnState::Connected(_)));
    }

    #[tokio::test]
    async fn disabled_config_stops_without_connecting() {
        let session = unreachable_slot();
        let cfg = {
            let mut c = crate::config::defaults();
            c.reconnect.enabled = false;
            Arc::new(std::sync::RwLock::new(c))
        };
        let committed = reconnect_loop(session.clone(), cfg, || async {
            panic!("connect must not run when auto-reconnect is disabled")
        })
        .await;
        assert!(!committed);
        assert!(matches!(
            &*session.read().await,
            ConnState::Dormant(r) if r.is_auto_retryable()
        ));
    }

    #[tokio::test]
    async fn commit_unreachable_demotes_connected_and_signals() {
        let client: Arc<dyn GitlabApi> = Arc::new(NoopGitlab);
        let session: SessionSlot = Arc::new(RwLock::new(ConnState::Connected(Session {
            gitlab: Arc::clone(&client),
            host: "gitlab.example.com".into(),
            user_id: 42,
        })));
        let signal = Notify::new();

        commit_unreachable(&session, &signal, &client, "connection refused".into()).await;

        let slot = session.read().await;
        let ConnState::Dormant(DormancyReason::Unreachable { host, detail }) = &*slot else {
            panic!("expected Dormant(Unreachable)");
        };
        assert_eq!(
            host, "gitlab.example.com",
            "captures the live session's host"
        );
        assert_eq!(detail, "connection refused");
        drop(slot);

        // A permit is stored for the supervisor, so `notified()` returns at once.
        tokio::time::timeout(std::time::Duration::from_millis(200), signal.notified())
            .await
            .expect("commit_unreachable notifies the supervisor");
    }

    #[tokio::test]
    async fn commit_unreachable_noop_when_not_connected() {
        let client: Arc<dyn GitlabApi> = Arc::new(NoopGitlab);
        for reason in [
            DormancyReason::NoCredentials,
            DormancyReason::LoggedOut,
            DormancyReason::TokenRejected {
                host: "h".into(),
                detail: "401".into(),
            },
            DormancyReason::Unreachable {
                host: "h".into(),
                detail: "first".into(),
            },
        ] {
            let session: SessionSlot = Arc::new(RwLock::new(ConnState::Dormant(reason)));
            let signal = Notify::new();

            commit_unreachable(&session, &signal, &client, "second".into()).await;

            let slot = session.read().await;
            match &*slot {
                ConnState::Dormant(DormancyReason::Unreachable { detail, .. }) => {
                    assert_eq!(detail, "first", "existing Unreachable detail is retained");
                }
                ConnState::Dormant(_) => {}
                ConnState::Connected(_) => panic!("a dormant slot must not become Connected"),
            }
            drop(slot);

            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(50), signal.notified())
                    .await
                    .is_err(),
                "no wakeup for a non-Connected slot"
            );
        }
    }

    #[tokio::test]
    async fn commit_unreachable_noop_when_client_superseded() {
        let client_a: Arc<dyn GitlabApi> = Arc::new(NoopGitlab);
        let client_b: Arc<dyn GitlabApi> = Arc::new(NoopGitlab);
        let session: SessionSlot = Arc::new(RwLock::new(ConnState::Connected(Session {
            gitlab: Arc::clone(&client_b),
            host: "gitlab.example.com".into(),
            user_id: 1,
        })));
        let signal = Notify::new();

        commit_unreachable(&session, &signal, &client_a, "stale error".into()).await;

        assert!(
            matches!(&*session.read().await, ConnState::Connected(s) if Arc::ptr_eq(&s.gitlab, &client_b)),
            "a stale client's error must not demote the newer session"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), signal.notified())
                .await
                .is_err(),
            "no wakeup when the failing client was superseded"
        );
    }

    #[tokio::test]
    async fn supervisor_engages_at_boot_then_on_each_signal() {
        let signal = Arc::new(Notify::new());
        // Default config: `max_delay` is 60 s, so the periodic re-check tick can't
        // fire during this test — only the signal drives re-engagement here.
        let config: SharedConfig = Arc::new(std::sync::RwLock::new(crate::config::defaults()));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let sig = Arc::clone(&signal);
        // A trivial engage step that just reports each invocation, so we test the
        // supervise loop's re-engage-on-signal contract without the keychain.
        let handle = tokio::spawn(async move {
            supervise(config, sig, move || {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(());
                    Engaged::Stable
                }
            })
            .await;
        });

        rx.recv().await.expect("boot engagement");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "supervisor must park between engagements, not spin"
        );

        signal.notify_one();
        rx.recv().await.expect("re-engaged after the first signal");
        signal.notify_one();
        rx.recv().await.expect("re-engaged after the second signal");

        handle.abort();
    }

    #[tokio::test]
    async fn supervise_backs_off_on_flapping() {
        let config: SharedConfig = {
            let mut c = crate::config::defaults();
            c.reconnect.base_delay_secs = 0;
            c.reconnect.max_delay_secs = 3600;
            Arc::new(std::sync::RwLock::new(c))
        };
        let signal = Arc::new(Notify::new());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
        let calls = Arc::new(AtomicUsize::new(0));
        let sig = Arc::clone(&signal);
        let c2 = Arc::clone(&calls);
        let handle = tokio::spawn(async move {
            supervise(config, sig, move || {
                let n = c2.fetch_add(1, SeqCst);
                let tx = tx.clone();
                async move {
                    let _ = tx.send(n);
                    // First two engagements flap, then it stabilizes.
                    if n < 2 {
                        Engaged::Flapping
                    } else {
                        Engaged::Stable
                    }
                }
            })
            .await;
        });

        assert_eq!(rx.recv().await, Some(0), "boot engagement");
        assert_eq!(
            rx.recv().await,
            Some(1),
            "re-engaged after the first flap without a signal"
        );
        assert_eq!(
            rx.recv().await,
            Some(2),
            "re-engaged after the second flap without a signal"
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv())
                .await
                .is_err(),
            "supervisor parks once it stops flapping"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn supervisor_re_engages_on_periodic_tick() {
        let config: SharedConfig = {
            let mut c = crate::config::defaults();
            c.reconnect.max_delay_secs = 1;
            Arc::new(std::sync::RwLock::new(c))
        };
        let signal = Arc::new(Notify::new());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let sig = Arc::clone(&signal);
        let handle = tokio::spawn(async move {
            supervise(config, sig, move || {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(());
                    Engaged::Stable
                }
            })
            .await;
        });

        rx.recv().await.expect("boot engagement");
        tokio::time::timeout(std::time::Duration::from_secs(3), rx.recv())
            .await
            .expect("periodic tick re-engaged without a signal")
            .expect("channel open");

        handle.abort();
    }
}
