//! API for interacting with gitlab-trackrd
#![allow(non_camel_case_types)]

include!(concat!(env!("OUT_DIR"), "/org.thehoster.gitlab.trackrd.rs"));

/// Raw varlink interface description, suitable for the daemon's
/// `org.varlink.service.GetInterfaceDescription` reply.
pub const VARLINK_INTERFACE_DESCRIPTION: &str =
    include_str!("../varlink/org.thehoster.gitlab.trackrd.varlink");
