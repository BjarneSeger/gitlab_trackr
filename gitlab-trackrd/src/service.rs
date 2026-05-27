//! Varlink protocol dispatcher.
//!
//! Splits the framework-level `org.varlink.service.*` methods from the
//! trackrd methods so each match stays short and self-evident.

use std::sync::Arc;

use tracing::{debug, warn};
use varlink::Reply;
use varlink::sansio::ServerEvent;

use gitlab_trackr_api::{
    AssignSelf_Args, AsyncCall, Call_AssignSelf, Call_ClearCache, Call_CloseIssue,
    Call_GetAssignedIssues, Call_GetHistory, Call_Login, Call_Logout, Call_PostTime,
    Call_UnassignSelf, Call_WhoAmI, CloseIssue_Args, GetAssignedIssues_Args, Login_Args,
    PostTime_Args, UnassignSelf_Args, VARLINK_INTERFACE_DESCRIPTION, VarlinkInterface as _,
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
            handlers
                .clear_cache(&mut call as &mut dyn Call_ClearCache)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.GetHistory" => {
            handlers
                .get_history(&mut call as &mut dyn Call_GetHistory)
                .await?;
        }
        "org.thehoster.gitlab.trackrd.GetAssignedIssues" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: GetAssignedIssues_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;

            handlers
                .get_assigned_issues(&mut call as &mut dyn Call_GetAssignedIssues, args.groups)
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
                    args.issue_iid,
                    args.duration,
                    args.summary,
                )
                .await?;
        }
        "org.thehoster.gitlab.trackrd.CloseIssue" => {
            let Some(args_val) = params else {
                return Ok(Some(Reply::error(
                    "org.varlink.service.InvalidParameter",
                    Some(serde_json::json!({"parameter": "parameters"})),
                )));
            };
            let args: CloseIssue_Args = serde_json::from_value(args_val).map_err(|e| {
                varlink::Error(
                    varlink::ErrorKind::InvalidParameter(e.to_string()),
                    None,
                    None,
                )
            })?;
            handlers
                .close_issue(
                    &mut call as &mut dyn Call_CloseIssue,
                    args.project_id,
                    args.issue_iid,
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
                    args.issue_iid,
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
                    args.issue_iid,
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
