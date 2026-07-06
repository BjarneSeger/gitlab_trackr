//! Background task that re-establishes the GitLab session after the daemon has
//! gone dormant because GitLab was unreachable.
//!
//! `GitlabClient::connect` is otherwise only called at startup and on `tt login`,
//! and neither retries. So if the stored (known-good) token can't reach GitLab
//! when the daemon boots, it lands `Dormant(Unreachable)` and — without this
//! task — stays there until the user runs `tt login` or restarts the daemon.
//!
//! This task watches for that state and retries the connection with the same
//! style of exponential back-off as the retry queue (see `queue`), reusing the
//! `[reconnect]` config and the shared session slot as the integration seam. The
//! moment it flips the slot back to `Connected`, the queue worker's defer loop
//! and the background refresh loops resume on their own; we additionally nudge
//! the queue and kick an immediate refresh so recovery is instant.
//!
//! Only a *transient* dormancy (`DormancyReason::is_auto_retryable`) is retried:
//! a rejected token, missing credentials, or an explicit logout all need the
//! user and would just spin.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::config::{SharedConfig, next_backoff};
use crate::error::{DormancyReason, Error};
use crate::gitlab::GitlabClient;
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

/// Spawn the background reconnect task. A no-op unless the daemon is currently
/// dormant for an auto-retryable reason, so the common (connected) start pays
/// nothing.
pub fn spawn(handlers: Arc<Handlers>) {
    tokio::spawn(async move {
        if !slot_is_retryable(&handlers.session).await {
            return;
        }
        // Read the keychain exactly once, up front. Re-reading it on every retry
        // was both wasteful and unsafe: a keychain hiccup mid-loop used to strand
        // the slot on a stale `Unreachable`, so the CLI kept claiming the daemon
        // was "retrying" while it had actually stopped. `resolve_credentials`
        // instead commits the real reason (keychain error / no credentials) and
        // bails, and the loop below only ever does the network connect.
        let creds = match resolve_credentials(secrets::load().await, &handlers.session).await {
            Some(c) => c,
            None => return,
        };
        info!("GitLab unreachable; starting background auto-reconnect");
        let committed = reconnect_loop(
            Arc::clone(&handlers.session),
            Arc::clone(&handlers.config),
            || connect_once(&creds),
        )
        .await;
        if committed {
            // Recovery side effects: flush deferred writes and warm the caches at
            // once, instead of waiting for the queue's `session_wait` tick and the
            // next refresh interval. `notify_one` stores a permit if the worker
            // hasn't parked yet, so the nudge can't be lost to a race with the
            // commit above.
            handlers.queue.drain_waker().notify_one();
            handlers.warm_up().await;
        }
    });
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

        // Re-read config each iteration so a hot reload / opt-out takes effect;
        // the std lock guard drops before any `.await`.
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
            // A failed commit means a racing login/logout already won the slot;
            // the next `slot_is_retryable` check would return false anyway, so
            // return its result directly rather than looping.
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
    // `matches!` borrows the guard only for the check, releasing it before the
    // assignment below; holding the write lock across both keeps it atomic.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use tokio::sync::RwLock;

    use crate::gitlab::{FetchedTimelog, GitlabApi, IssueWithLabels};

    /// A trait object stand-in for a live client. `reconnect_loop` only ever
    /// *stores* the session, never calls through it, so every method is `unreachable`.
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
            _p: i64,
            _i: i64,
            _d: &str,
            _s: Option<&str>,
        ) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn create_timelog(
            &self,
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
        async fn close_issue(&self, _p: i64, _i: i64) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn assign_self(&self, _p: i64, _i: i64) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn unassign_self(&self, _p: i64, _i: i64) -> crate::error::Result<()> {
            unimplemented!()
        }
        async fn fetch_board_list_labels(&self, _p: i64) -> crate::error::Result<Vec<String>> {
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
}
