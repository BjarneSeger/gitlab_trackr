use std::sync::Arc;

use tracing::{debug, warn};
use varlink::sansio::ServerEvent;

use gitlab_trackr_api::{
    self, AsyncCall, Call_ClearCache, Call_GetAssignedIssues, Call_PostTime, PostTime_Args,
    VarlinkInterface as _,
};

use crate::daemon::Daemon;

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

const TRACKRD_INTERFACE_DESCRIPTION: &str = include_str!("org.thehoster.gitlab.trackrd.varlink");

pub struct ServiceHandler {
    daemon: Arc<Daemon>,
}

impl ServiceHandler {
    pub fn new(daemon: Arc<Daemon>) -> Self {
        ServiceHandler { daemon }
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
                    match request.method.as_ref() {
                        "org.varlink.service.GetInfo" => {
                            server.send_reply(varlink::Reply::parameters(Some(serde_json::json!({
                            "vendor": "org.thehoster",
                            "product": "gitlab_trackrd",
                            "version": env!("CARGO_PKG_VERSION"),
                            "url": "https://github.com/lordi/gitlab_trackrd",
                            "interfaces": ["org.varlink.service", "org.thehoster.gitlab.trackrd"]
                        }))))?;
                        }
                        "org.varlink.service.GetInterfaceDescription" => {
                            let desc = request
                                .parameters
                                .as_ref()
                                .and_then(|p| p.get("interface"))
                                .and_then(|v| v.as_str())
                                .and_then(|name| match name {
                                    "org.varlink.service" => Some(ORG_VARLINK_SERVICE_DESCRIPTION),
                                    "org.thehoster.gitlab.trackrd" => {
                                        Some(TRACKRD_INTERFACE_DESCRIPTION)
                                    }
                                    _ => None,
                                });
                            match desc {
                                Some(d) => server.send_reply(varlink::Reply::parameters(Some(
                                    serde_json::json!({"description": d}),
                                )))?,
                                None => server.send_reply(varlink::Reply::error(
                                    "org.varlink.service.InvalidParameter",
                                    Some(serde_json::json!({"parameter": "interface"})),
                                ))?,
                            }
                        }
                        "org.thehoster.gitlab.trackrd.ClearCache" => {
                            let mut call = AsyncCall::default();
                            self.daemon
                                .clear_cache(&mut call as &mut dyn Call_ClearCache)
                                .await?;
                            if let Some(reply) = call.take_reply() {
                                server.send_reply(reply)?;
                            }
                        }
                        "org.thehoster.gitlab.trackrd.GetAssignedIssues" => {
                            let mut call = AsyncCall::default();
                            self.daemon
                                .get_assigned_issues(&mut call as &mut dyn Call_GetAssignedIssues)
                                .await?;
                            if let Some(reply) = call.take_reply() {
                                server.send_reply(reply)?;
                            }
                        }
                        "org.thehoster.gitlab.trackrd.PostTime" => {
                            if let Some(args_val) = request.parameters {
                                let args: PostTime_Args = serde_json::from_value(args_val)
                                    .map_err(|e| {
                                        varlink::Error(
                                            varlink::ErrorKind::InvalidParameter(e.to_string()),
                                            None,
                                            None,
                                        )
                                    })?;
                                let mut call = AsyncCall::default();
                                self.daemon
                                    .post_time(
                                        &mut call as &mut dyn Call_PostTime,
                                        args.project_id,
                                        args.issue_iid,
                                        args.duration,
                                        args.summary,
                                    )
                                    .await?;
                                if let Some(reply) = call.take_reply() {
                                    server.send_reply(reply)?;
                                }
                            } else {
                                server.send_reply(varlink::Reply::error(
                                    "org.varlink.service.InvalidParameter",
                                    Some(serde_json::json!({"parameter": "parameters"})),
                                ))?;
                            }
                        }
                        method => {
                            warn!(method, "unknown varlink method");
                            server.send_reply(varlink::Reply::error(
                                "org.varlink.service.MethodNotFound",
                                Some(serde_json::json!({"method": method})),
                            ))?;
                        }
                    }
                }
                ServerEvent::Upgrade { interface } => return Ok(Some(interface)),
            }
        }
        Ok(None)
    }
}
