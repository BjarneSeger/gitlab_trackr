//! OS-level credential storage.
//!
//! macOS: Keychain (data-protection / iCloud-synchronized item).
//! Linux: secret-service via `oo7`.
//!
//! A single entry holds both host and token as a JSON blob — one logged-in
//! account at a time, matching the daemon's single-host model.

use crate::error::{Error, Result};

const SERVICE: &str = "gitlab-trackrd";

#[derive(Debug, Clone)]
pub struct Credentials {
    pub host: String,
    pub token: String,
}

pub async fn load() -> Result<Option<Credentials>> {
    platform::load().await
}

pub async fn store(creds: &Credentials) -> Result<()> {
    platform::store(creds).await
}

pub async fn delete() -> Result<()> {
    platform::delete().await
}

fn encode(creds: &Credentials) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&serde_json::json!({
        "host": creds.host,
        "token": creds.token,
    }))?)
}

fn decode(bytes: &[u8]) -> Result<Credentials> {
    let v: serde_json::Value = serde_json::from_slice(bytes)?;
    let host = v["host"]
        .as_str()
        .ok_or_else(|| Error::Secrets("stored secret missing 'host'".to_string()))?
        .to_string();
    let token = v["token"]
        .as_str()
        .ok_or_else(|| Error::Secrets("stored secret missing 'token'".to_string()))?
        .to_string();
    Ok(Credentials { host, token })
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{Credentials, Error, Result, SERVICE, decode, encode};
    use security_framework::passwords::{
        PasswordOptions, delete_generic_password_options, generic_password,
        set_generic_password_options,
    };

    const ACCOUNT: &str = "default";

    fn options(synchronized: Option<bool>) -> PasswordOptions {
        let mut opts = PasswordOptions::new_generic_password(SERVICE, ACCOUNT);
        opts.set_access_synchronized(synchronized);
        opts
    }

    pub async fn load() -> Result<Option<Credentials>> {
        let blob = tokio::task::spawn_blocking(|| generic_password(options(None)))
            .await
            .map_err(|e| Error::Secrets(format!("join: {e}")))?;
        match blob {
            Ok(bytes) => Ok(Some(decode(&bytes)?)),
            Err(e) if e.code() == security_framework_sys::base::errSecItemNotFound => Ok(None),
            Err(e) => Err(Error::Secrets(e.to_string())),
        }
    }

    pub async fn store(creds: &Credentials) -> Result<()> {
        let payload = encode(creds)?;
        tokio::task::spawn_blocking(move || {
            set_generic_password_options(&payload, options(Some(true)))
        })
        .await
        .map_err(|e| Error::Secrets(format!("join: {e}")))?
        .map_err(|e| Error::Secrets(e.to_string()))
    }

    pub async fn delete() -> Result<()> {
        let r = tokio::task::spawn_blocking(|| delete_generic_password_options(options(None)))
            .await
            .map_err(|e| Error::Secrets(format!("join: {e}")))?;
        match r {
            Ok(()) => Ok(()),
            Err(e) if e.code() == security_framework_sys::base::errSecItemNotFound => Ok(()),
            Err(e) => Err(Error::Secrets(e.to_string())),
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use std::collections::HashMap;

    use super::{Credentials, Error, Result, SERVICE, decode, encode};
    use oo7::Keyring;

    fn attributes() -> HashMap<&'static str, &'static str> {
        HashMap::from([("service", SERVICE)])
    }

    async fn open() -> Result<Keyring> {
        let kr = Keyring::new()
            .await
            .map_err(|e| Error::Secrets(e.to_string()))?;
        kr.unlock()
            .await
            .map_err(|e| Error::Secrets(e.to_string()))?;
        Ok(kr)
    }

    pub async fn load() -> Result<Option<Credentials>> {
        let kr = open().await?;
        let items = kr
            .search_items(&attributes())
            .await
            .map_err(|e| Error::Secrets(e.to_string()))?;
        let Some(item) = items.first() else {
            return Ok(None);
        };
        let secret = item
            .secret()
            .await
            .map_err(|e| Error::Secrets(e.to_string()))?;
        Ok(Some(decode(&secret)?))
    }

    pub async fn store(creds: &Credentials) -> Result<()> {
        let kr = open().await?;
        let payload = encode(creds)?;
        kr.create_item("gitlab-trackrd credentials", &attributes(), payload, true)
            .await
            .map_err(|e| Error::Secrets(e.to_string()))
    }

    pub async fn delete() -> Result<()> {
        let kr = open().await?;
        kr.delete(&attributes())
            .await
            .map_err(|e| Error::Secrets(e.to_string()))
    }
}
