//! Socket-activated accept loop and varlink connection driver.

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::error::Result;

/// Build a [`UnixListener`] from the systemd-passed socket FD (socket activation)
/// or by binding a new socket at `path`.
pub fn make_listener(socket_path: &str) -> Result<UnixListener> {
    if is_socket_activated() {
        // SAFETY: systemd guarantees FD 3 is a valid, bound, listening Unix socket.
        let std_listener = unsafe {
            use std::os::unix::io::FromRawFd;
            std::os::unix::net::UnixListener::from_raw_fd(3)
        };
        std_listener.set_nonblocking(true)?;
        Ok(UnixListener::from_std(std_listener)?)
    } else {
        Ok(UnixListener::bind(socket_path)?)
    }
}

/// Returns `true` when the process was socket-activated by systemd.
pub fn is_socket_activated() -> bool {
    std::env::var("LISTEN_FDS").as_deref() == Ok("1")
}

/// The daemon's connection-level dispatcher. A daemon-owned sibling of
/// `varlink::AsyncConnectionHandler` whose `handle` also receives the
/// connection's write side, so a method that streams (`more: true` →
/// several replies with `continues`) can flush earlier replies to the
/// socket *before* it finishes computing later ones — the whole point of
/// streaming. Handlers that reply once can ignore `out`; the connection
/// driver drains any unflushed transmits after `handle` returns.
#[async_trait::async_trait]
pub trait ConnectionHandler: Send + Sync {
    async fn handle(
        &self,
        server: &mut varlink::sansio::Server,
        out: &mut (dyn AsyncWrite + Send + Unpin),
        upgraded: Option<String>,
    ) -> varlink::Result<Option<String>>;
}

/// Write every pending sans-IO transmit to `out` and flush it.
pub async fn flush_transmits(
    server: &mut varlink::sansio::Server,
    out: &mut (dyn AsyncWrite + Send + Unpin),
) -> varlink::Result<()> {
    let mut wrote = false;
    while let Some(transmit) = server.poll_transmit() {
        out.write_all(&transmit.payload)
            .await
            .map_err(|_| varlink::Error(varlink::ErrorKind::ConnectionClosed, None, None))?;
        wrote = true;
    }
    if wrote {
        out.flush()
            .await
            .map_err(|_| varlink::Error(varlink::ErrorKind::ConnectionClosed, None, None))?;
    }
    Ok(())
}

/// Accept loop — runs until the process receives a signal.
pub async fn serve<H: ConnectionHandler + 'static>(
    handler: Arc<H>,
    listener: UnixListener,
) -> Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let handler = Arc::clone(&handler);
        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, handler).await {
                match e.kind() {
                    varlink::ErrorKind::ConnectionClosed => {}
                    _ => tracing::warn!("connection error: {e:?}"),
                }
            }
        });
    }
}

/// Drive a single varlink connection to completion using the sans-IO state machine.
async fn handle_connection<H: ConnectionHandler>(
    mut stream: UnixStream,
    handler: Arc<H>,
) -> varlink::Result<()> {
    let mut server = varlink::sansio::Server::new();
    let mut buf = vec![0u8; 8192];
    let mut upgraded_iface: Option<String> = None;

    loop {
        let n = stream
            .read(&mut buf)
            .await
            .map_err(|_| varlink::Error(varlink::ErrorKind::ConnectionClosed, None, None))?;

        if n == 0 {
            return Ok(());
        }

        server.handle_input(&buf[..n])?;
        upgraded_iface = handler
            .handle(&mut server, &mut stream, upgraded_iface.clone())
            .await?;
        // Catch-all: whatever the handler didn't flush itself.
        flush_transmits(&mut server, &mut stream).await?;
    }
}
