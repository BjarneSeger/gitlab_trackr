//! Socket-activated accept loop and varlink connection driver.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;
use varlink::AsyncConnectionHandler;

use crate::Result;

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

/// Accept loop with idle timeout.
///
/// The idle timer resets when a new connection is accepted AND when the last
/// active connection finishes. This ensures the daemon doesn't exit while
/// requests are still in flight and gives a full `idle_timeout` window after
/// the last request completes before exiting.
pub async fn serve<H: AsyncConnectionHandler + 'static>(
    handler: Arc<H>,
    listener: UnixListener,
    idle_timeout: Duration,
) -> Result<()> {
    let idle = tokio::time::sleep(idle_timeout);
    tokio::pin!(idle);

    // Shared state between the accept loop and spawned connection tasks.
    let last_finished = Arc::new(Notify::new());
    let active = Arc::new(AtomicUsize::new(0));

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                // Reset idle timer — a new request is starting.
                idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);

                active.fetch_add(1, Ordering::Relaxed);
                let last_finished = Arc::clone(&last_finished);
                let active = Arc::clone(&active);
                let handler = Arc::clone(&handler);
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, handler).await {
                        match e.kind() {
                            varlink::ErrorKind::ConnectionClosed => {}
                            _ => tracing::warn!("connection error: {e:?}"),
                        }
                    }
                    // If this was the last active connection, signal the accept loop
                    // so it can reset the idle timer from now.
                    if active.fetch_sub(1, Ordering::AcqRel) == 1 {
                        last_finished.notify_one();
                    }
                });
            }
            _ = last_finished.notified() => {
                // Last in-flight connection finished — start the idle window from now.
                idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
            }
            _ = &mut idle => {
                tracing::info!(
                    idle_timeout_secs = idle_timeout.as_secs(),
                    "idle timeout reached, exiting"
                );
                return Ok(());
            }
        }
    }
}

/// Drive a single varlink connection to completion using the sans-IO state machine.
async fn handle_connection<H: AsyncConnectionHandler>(
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
        upgraded_iface = handler.handle(&mut server, upgraded_iface.clone()).await?;

        while let Some(transmit) = server.poll_transmit() {
            stream
                .write_all(&transmit.payload)
                .await
                .map_err(|_| varlink::Error(varlink::ErrorKind::ConnectionClosed, None, None))?;
            stream
                .flush()
                .await
                .map_err(|_| varlink::Error(varlink::ErrorKind::ConnectionClosed, None, None))?;
        }
    }
}
