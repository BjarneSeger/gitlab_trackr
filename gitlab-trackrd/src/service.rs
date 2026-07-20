//! Varlink protocol dispatcher.
//!
//! Splits the framework-level `org.varlink.service.*` methods from the
//! trackrd methods so each match stays short and self-evident.

use std::sync::Arc;

use tokio::io::AsyncWrite;
use tracing::{debug, warn};
use varlink::Reply;
use varlink::sansio::ServerEvent;

use gitlab_trackr_api::{
    AssignSelf_Args, AsyncCall, Call_AssignSelf, Call_ClearCache, Call_ClearFailures, Call_Close,
    Call_DismissFailure, Call_GetAssignedIssues, Call_GetAssignedMergeRequests, Call_GetFailures,
    Call_GetHistory, Call_Login, Call_Logout, Call_PostTime, Call_RetryFailure, Call_Search,
    Call_UnassignSelf, Call_WhoAmI, ClearCache_Args, Close_Args, DismissFailure_Args,
    GetAssignedIssues_Args, GetAssignedMergeRequests_Args, GetHistory_Args, Login_Args,
    PostTime_Args, RetryFailure_Args, Search_Args, UnassignSelf_Args,
    VARLINK_INTERFACE_DESCRIPTION, VarlinkInterface as _,
};

use crate::handlers::Handlers;

const ORG_VARLINK_SERVICE_DESCRIPTION: &str = r#"interface org.varlink.service

method GetInfo() -> (
  vendor: string,
  product: string,
  version: string,
  url: string,
  interfaces: []string
)

method GetInterfaceDescription(interface: string) -> (description: string)

error InterfaceNotFound (interface: string)
error MethodNotFound (method: string)
error MethodNotImplemented (method: string)
error InvalidParameter (parameter: string)
"#;

pub struct ServiceHandler {
    handlers: Arc<Handlers>,
}

impl ServiceHandler {
    pub fn new(handlers: Arc<Handlers>) -> Self {
        ServiceHandler { handlers }
    }
}

/// The `Search` method name — the one method with a streaming (`more`) path.
const SEARCH_METHOD: &str = "org.thehoster.gitlab.trackrd.Search";

#[async_trait::async_trait]
impl crate::server::ConnectionHandler for ServiceHandler {
    async fn handle(
        &self,
        server: &mut varlink::sansio::Server,
        out: &mut (dyn AsyncWrite + Send + Unpin),
        _upgraded: Option<String>,
    ) -> varlink::Result<Option<String>> {
        while let Some(event) = server.poll_event() {
            match event {
                ServerEvent::Request { request } => {
                    debug!(method = request.method.as_ref(), "varlink request");
                    let method = request.method.as_ref();
                    let reply = if let Some(reply) = handle_varlink_meta(method, &request) {
                        Some(reply)
                    } else if method == SEARCH_METHOD && request.more == Some(true) {
                        handle_search_streamed(
                            request.parameters.clone(),
                            &self.handlers,
                            server,
                            out,
                        )
                        .await?;
                        None
                    } else if method.starts_with("org.thehoster.gitlab.trackrd.") {
                        handle_trackrd(method, request.parameters, &self.handlers).await?
                    } else {
                        warn!(method, "unknown varlink method");
                        Some(Reply::error(
                            "org.varlink.service.MethodNotFound",
                            Some(serde_json::json!({"method": method})),
                        ))
                    };
                    if let Some(reply) = reply {
                        server.send_reply(reply)?;
                    }
                }
                ServerEvent::Upgrade { interface } => return Ok(Some(interface)),
            }
        }
        Ok(None)
    }
}

/// `Search` with `more: true` — the streaming path: an instant reply from the
/// local corpus, flushed to the socket with `continues: true`, then the full
/// transparent search (live micro-sync + merge) as the terminal reply.
///
/// The frame count is deliberately deterministic: an error is one terminal
/// frame (varlink errors always end an exchange), a success is exactly two —
/// even while dormant, where phase 2 degrades to a cache re-read. varlink
/// 13's async client does not expose the `continues` flag, so `tt` counts
/// frames instead of reading it; this contract is documented in
/// `docs/varlink_interface.md`.
async fn handle_search_streamed(
    params: Option<serde_json::Value>,
    handlers: &Handlers,
    server: &mut varlink::sansio::Server,
    out: &mut (dyn AsyncWrite + Send + Unpin),
) -> varlink::Result<()> {
    let Some(args_val) = params else {
        return server.send_reply(Reply::error(
            "org.varlink.service.InvalidParameter",
            Some(serde_json::json!({"parameter": "parameters"})),
        ));
    };
    let args: Search_Args = serde_json::from_value(args_val).map_err(|e| {
        varlink::Error(
            varlink::ErrorKind::InvalidParameter(e.to_string()),
            None,
            None,
        )
    })?;

    // Phase 1: the pure cache read, served immediately.
    let mut call = AsyncCall::default();
    handlers
        .search_cached(
            &mut call as &mut dyn Call_Search,
            args.query.clone(),
            args.kinds.clone(),
            args.limit,
        )
        .await?;
    let Some(mut first) = call.take_reply() else {
        return Ok(());
    };
    if first.error.is_some() {
        return server.send_reply(first);
    }
    first.continues = Some(true);
    server.send_reply(first)?;
    // The flush is the point: the cached frame must hit the socket before
    // the live fetch spends its deadline.
    crate::server::flush_transmits(server, out).await?;

    // Phase 2: the transparent search; its reply ends the exchange.
    let mut call = AsyncCall::default();
    handlers
        .search(
            &mut call as &mut dyn Call_Search,
            args.query,
            args.kinds,
            args.limit,
        )
        .await?;
    if let Some(reply) = call.take_reply() {
        server.send_reply(reply)?;
    }
    Ok(())
}

/// Replies for the framework-level `org.varlink.service.*` methods, or `None`
/// if the method isn't one of them.
fn handle_varlink_meta(method: &str, request: &varlink::Request) -> Option<Reply> {
    match method {
        "org.varlink.service.GetInfo" => Some(Reply::parameters(Some(serde_json::json!({
            "vendor": "org.thehoster",
            "product": "gitlab-trackrd",
            "version": env!("CARGO_PKG_VERSION"),
            "url": "https://github.com/bjarneseger/gitlab_trackr",
            "interfaces": ["org.varlink.service", "org.thehoster.gitlab.trackrd"]
        })))),
        "org.varlink.service.GetInterfaceDescription" => {
            let name = request
                .parameters
                .as_ref()
                .and_then(|p| p.get("interface"))
                .and_then(|v| v.as_str());
            let desc = match name {
                Some("org.varlink.service") => Some(ORG_VARLINK_SERVICE_DESCRIPTION),
                Some("org.thehoster.gitlab.trackrd") => Some(VARLINK_INTERFACE_DESCRIPTION),
                _ => None,
            };
            Some(match desc {
                Some(d) => Reply::parameters(Some(serde_json::json!({"description": d}))),
                None => Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "interface"})),
                ),
            })
        }
        _ => None,
    }
}

async fn handle_trackrd(
    method: &str,
    params: Option<serde_json::Value>,
    handlers: &Handlers,
) -> varlink::Result<Option<Reply>> {
    let mut call = AsyncCall::default();
    match method {
        "org.thehoster.gitlab.trackrd.ClearCache" => {
            // `scope` is optional, so an omitted `parameters` block is valid
            // and means "clear everything".
            let args: ClearCache_Args = match params {
                Some(v) => serde_json::from_value(v).map_err(|e| {
                    varlink::Error(
                        varlink::ErrorKind::InvalidParameter(e.to_string()),
                        None,
                        None,
                    )
                })?,
                None => ClearCache_Args { scope: None },
            };
            handlers
                .clear_cache(&mut call as &mut dyn Call_ClearCache, args.scope)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.GetHistory" => {
            // `days` is optional; an omitted `parameters` block falls back to
            // the default window in the handler.
            let args: GetHistory_Args = match params {
                Some(v) => serde_json::from_value(v).map_err(|e| {
                    varlink::Error(
                        varlink::ErrorKind::InvalidParameter(e.to_string()),
                        None,
                        None,
                    )
                })?,
                None => GetHistory_Args { days: None },
            };
            handlers
                .get_history(&mut call as &mut dyn Call_GetHistory, args.days)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.GetFailures" => {
            handlers
                .get_failures(&mut call as &mut dyn Call_GetFailures)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.RetryFailure" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: RetryFailure_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .retry_failure(&mut call as &mut dyn Call_RetryFailure, args.id)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.DismissFailure" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: DismissFailure_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .dismiss_failure(&mut call as &mut dyn Call_DismissFailure, args.id)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.ClearFailures" => {
            handlers
                .clear_failures(&mut call as &mut dyn Call_ClearFailures)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.GetAssignedIssues" => {
            // Every field of `GetAssignedIssues_Args` is optional, so an
            // omitted `parameters` block is a valid call (e.g.
            // `varlinkctl call ... {}`). Default the args when missing.
            let args: GetAssignedIssues_Args = match params {
                Some(v) => serde_json::from_value(v).map_err(|e| {
                    varlink::Error(
                        varlink::ErrorKind::InvalidParameter(e.to_string()),
                        None,
                        None,
                    )
                })?,
                None => GetAssignedIssues_Args { groups: None },
            };

            handlers
                .get_assigned_issues(&mut call as &mut dyn Call_GetAssignedIssues, args.groups)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.GetAssignedMergeRequests" => {
            // Same defaulting pattern as `GetAssignedIssues`: every field is
            // optional, so a missing `parameters` block is a valid call.
            let args: GetAssignedMergeRequests_Args = match params {
                Some(v) => serde_json::from_value(v).map_err(|e| {
                    varlink::Error(
                        varlink::ErrorKind::InvalidParameter(e.to_string()),
                        None,
                        None,
                    )
                })?,
                None => GetAssignedMergeRequests_Args { groups: None },
            };

            handlers
                .get_assigned_merge_requests(
                    &mut call as &mut dyn Call_GetAssignedMergeRequests,
                    args.groups,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.Search" => {
            // `query` is required, so a missing `parameters` block is an error
            // (the `PostTime` pattern, not the defaulting one).
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: Search_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .search(
                    &mut call as &mut dyn Call_Search,
                    args.query,
                    args.kinds,
                    args.limit,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.PostTime" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: PostTime_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .post_time(
                    &mut call as &mut dyn Call_PostTime,
                    args.project_id,
                    args.iid,
                    args.kind,
                    args.duration,
                    args.summary,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.Close" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: Close_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .close(
                    &mut call as &mut dyn Call_Close,
                    args.project_id,
                    args.iid,
                    args.kind,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.AssignSelf" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: AssignSelf_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .assign_self(
                    &mut call as &mut dyn Call_AssignSelf,
                    args.project_id,
                    args.iid,
                    args.kind,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.UnassignSelf" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: UnassignSelf_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .unassign_self(
                    &mut call as &mut dyn Call_UnassignSelf,
                    args.project_id,
                    args.iid,
                    args.kind,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.Login" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: Login_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .login(&mut call as &mut dyn Call_Login, args.host, args.token)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.Logout" => {
            handlers.logout(&mut call as &mut dyn Call_Logout).await?;
        }
        "org.thehoster.gitlab.trackrd.WhoAmI" => {
            handlers.who_am_i(&mut call as &mut dyn Call_WhoAmI).await?;
        }
        _ => {
            warn!(method, "unknown trackrd method");
            return Ok(Some(Reply::error(
                "org.varlink.service.MethodNotFound",
                Some(serde_json::json!({"method": method})),
            )));
        }
    }
    Ok(call.take_reply())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use crate::server::ConnectionHandler as _;

    /// One NUL-terminated varlink request frame.
    fn frame(method: &str, params: serde_json::Value, more: bool) -> Vec<u8> {
        let mut req = serde_json::json!({
            "method": method,
            "parameters": params,
        });
        if more {
            req["more"] = serde_json::Value::Bool(true);
        }
        let mut bytes = serde_json::to_vec(&req).unwrap();
        bytes.push(0);
        bytes
    }

    fn search_frame(query: &str, more: bool) -> Vec<u8> {
        frame(
            "org.thehoster.gitlab.trackrd.Search",
            serde_json::json!({"query": query, "kinds": ["issues"]}),
            more,
        )
    }

    /// Feed one request frame through the dispatcher and split the wire
    /// output back into parsed reply frames.
    async fn drive_frames(handler: &ServiceHandler, input: &[u8]) -> Vec<serde_json::Value> {
        let mut server = varlink::sansio::Server::new();
        server.handle_input(input).unwrap();
        let mut out: Vec<u8> = Vec::new();
        handler.handle(&mut server, &mut out, None).await.unwrap();
        crate::server::flush_transmits(&mut server, &mut out)
            .await
            .unwrap();
        out.split(|b| *b == 0)
            .filter(|f| !f.is_empty())
            .map(|f| serde_json::from_slice(f).unwrap())
            .collect()
    }

    fn issue_ids(reply: &serde_json::Value) -> Vec<i64> {
        reply["parameters"]["issues"]
            .as_array()
            .expect("issues array")
            .iter()
            .map(|i| i["id"].as_i64().unwrap())
            .collect()
    }

    fn continues(reply: &serde_json::Value) -> bool {
        reply.get("continues").and_then(|v| v.as_bool()) == Some(true)
    }

    #[tokio::test]
    async fn search_with_more_streams_cached_then_live_merged() {
        let (h, _dir) = crate::handlers::tests::connected_with_live_hit(None);
        let handler = ServiceHandler::new(Arc::new(h));

        let frames = drive_frames(&handler, &search_frame("oauth", true)).await;

        assert_eq!(frames.len(), 2, "streamed search replies twice: {frames:?}");
        assert!(continues(&frames[0]), "the cached frame carries continues");
        assert_eq!(
            issue_ids(&frames[0]),
            vec![1],
            "phase 1 is the local corpus only"
        );
        assert!(!continues(&frames[1]), "the merged frame ends the exchange");
        assert!(
            issue_ids(&frames[1]).contains(&70),
            "phase 2 folds in the live hit"
        );
    }

    #[tokio::test]
    async fn search_with_more_while_dormant_still_sends_two_frames() {
        let (h, _dir) = crate::handlers::tests::dormant_with_seeded_corpus();
        let handler = ServiceHandler::new(Arc::new(h));

        let frames = drive_frames(&handler, &search_frame("oauth", true)).await;

        // A success is ALWAYS two frames — the client counts frames because
        // varlink 13's async client hides the continues flag. Dormant phase 2
        // is just the cache re-read.
        assert_eq!(frames.len(), 2, "deterministic frame count: {frames:?}");
        assert!(continues(&frames[0]));
        assert_eq!(issue_ids(&frames[0]), vec![1]);
        assert!(!continues(&frames[1]));
        assert_eq!(issue_ids(&frames[1]), vec![1]);
    }

    #[tokio::test]
    async fn search_with_more_rejects_bad_args_with_one_error_frame() {
        let (h, _dir) = crate::handlers::tests::connected_with_live_hit(None);
        let handler = ServiceHandler::new(Arc::new(h));

        let frames = drive_frames(&handler, &search_frame("", true)).await;

        assert_eq!(frames.len(), 1, "errors always end the exchange");
        assert!(!continues(&frames[0]));
        assert_eq!(
            frames[0]["error"].as_str(),
            Some("org.thehoster.gitlab.trackrd.GitlabError")
        );
    }

    #[tokio::test]
    async fn search_without_more_replies_once_with_the_live_merge() {
        let (h, _dir) = crate::handlers::tests::connected_with_live_hit(None);
        let handler = ServiceHandler::new(Arc::new(h));

        let frames = drive_frames(&handler, &search_frame("oauth", false)).await;

        assert_eq!(frames.len(), 1, "no more flag → the single bounded reply");
        assert!(!continues(&frames[0]));
        assert!(
            issue_ids(&frames[0]).contains(&70),
            "the single reply already carries the live merge"
        );
    }

    #[tokio::test]
    async fn search_with_more_flushes_the_cached_frame_before_the_live_fetch() {
        use tokio::io::AsyncReadExt;

        let (h, _dir) =
            crate::handlers::tests::connected_with_live_hit(Some(Duration::from_millis(400)));
        let handler = ServiceHandler::new(Arc::new(h));
        let (mut client, mut daemon_side) = tokio::io::duplex(64 * 1024);

        let drive = async {
            let mut server = varlink::sansio::Server::new();
            server.handle_input(&search_frame("oauth", true)).unwrap();
            handler
                .handle(&mut server, &mut daemon_side, None)
                .await
                .unwrap();
            crate::server::flush_transmits(&mut server, &mut daemon_side)
                .await
                .unwrap();
        };
        let read_first_frame = async {
            let started = std::time::Instant::now();
            let mut buf = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                client.read_exact(&mut byte).await.unwrap();
                if byte[0] == 0 {
                    break;
                }
                buf.push(byte[0]);
            }
            let reply: serde_json::Value = serde_json::from_slice(&buf).unwrap();
            (reply, started.elapsed())
        };

        let ((), (first, elapsed)) = tokio::join!(drive, read_first_frame);
        assert!(continues(&first));
        assert!(
            elapsed < Duration::from_millis(300),
            "the cached frame must hit the wire while the live fetch is still \
             sleeping (took {elapsed:?})"
        );
    }

    /// Pins the hand-written dispatch above: a method that exists in the
    /// generated `VarlinkInterface` trait but has no arm in `handle_trackrd`
    /// compiles fine and only fails at runtime as `MethodNotFound` — this
    /// test turns that silent trap into a red test.
    #[tokio::test]
    async fn dispatch_has_an_arm_for_search() {
        let (handlers, _dir) = crate::handlers::tests::dormant_handlers();
        let reply = handle_trackrd(
            "org.thehoster.gitlab.trackrd.Search",
            Some(serde_json::json!({"query": "x"})),
            &handlers,
        )
        .await
        .unwrap()
        .expect("a reply");
        assert_ne!(
            reply.error.as_deref(),
            Some("org.varlink.service.MethodNotFound"),
            "Search is missing its dispatch arm in handle_trackrd"
        );
    }
}
