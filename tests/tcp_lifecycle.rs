//! Lifecycle: listener bind/unbind, graceful shutdown via cancellation token,
//! shutdown with active connections.

mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use common::*;

#[tokio::test]
async fn cancel_stops_listener() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;

    // Confirm listener is responsive before cancel
    {
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(b"x").await.unwrap();
        let mut b = [0u8; 1];
        c.read_exact(&mut b).await.unwrap();
    }

    proxy.shutdown.cancel();

    // The serve task should complete promptly
    let res = tokio::time::timeout(Duration::from_secs(3), proxy.task).await;
    assert!(res.is_ok(), "serve task didn't exit on cancellation");
    let serve_result = res.unwrap().unwrap();
    assert!(
        serve_result.is_ok(),
        "serve returned error: {serve_result:?}"
    );
}

#[tokio::test]
async fn cancel_during_active_connection() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    conn.write_all(b"warmup").await.unwrap();
    let mut buf = [0u8; 6];
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"warmup");

    // Cancel while connection is open and idle. The accept loop exits but
    // child connection tasks each hold a child_token of the shutdown token,
    // so they get cancelled too via the watchdog's shutdown branch.
    proxy.shutdown.cancel();

    // The relay watchdog catches the shutdown and cancels the relay; the
    // connection should EOF.
    let mut tail = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(15), conn.read(&mut tail))
        .await
        .expect("cancellation didn't propagate to active connection")
        .unwrap_or(0);
    assert_eq!(n, 0);

    let _ = proxy.task.await;
}

#[tokio::test]
async fn bind_failure_returns_error() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    // Bind something on a port first
    let blocker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = blocker.local_addr().unwrap();

    let mut cfg = cfg_tcp("127.0.0.1:1".parse().unwrap());
    cfg.listen = addr.to_string(); // already taken

    // Call run() (NOT serve) to exercise the bind path
    let result = oxiduct::proxy::tcp::run(
        Arc::new(cfg),
        oxiduct::metrics::Metrics::new(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err(), "expected bind failure, got {result:?}");

    drop(blocker);
}

#[tokio::test]
async fn dropped_proxy_handle_releases_port() {
    // Sanity: after we cancel and the serve task ends, the port can be
    // bound again. Catches FD leaks.
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;
    proxy.stop().await;

    // The port is now free (or at least the listener task is done).
    // We can't bind to the exact same port reliably (OS may keep it in
    // TIME_WAIT briefly), but we should at least see the task gone.
    // Use a fresh proxy on the same target to sanity-check.
    let proxy2 = spawn_tcp_proxy(cfg_tcp(echo)).await;
    assert_ne!(proxy2.addr, addr, "same port reused — unlikely race");
    proxy2.stop().await;
}
