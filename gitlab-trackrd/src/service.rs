//! Varlink protocol dispatcher.
//!
//! Splits the framework-level `org.varlink.service.*` methods from the
//! trackrd methods so each match stays short and self-evident.

use std::sync::Arc;

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

#[async_trait::async_trait]
impl varlink::AsyncConnectionHandler for ServiceHandler {
    async fn handle(
        &self,
        server: &mut varlink::sansio::Server,
        _upgraded: Option<String>,
    ) -> varlink::Result<Option<String>> {
        while let Some(event) = server.poll_event() {
            match event {
                ServerEvent::Request { request } => {
                    debug!(method = request.method.as_ref(), "varlink request");
                    let method = request.method.as_ref();
                    let reply = if let Some(reply) = handle_varlink_meta(method, &request) {
                        Some(reply)
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
