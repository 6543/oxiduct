//! L3 / L4 timeout behavior and connect timeout.
//!
//! These tests use real wall-clock waits (no `tokio::time::pause` since the
//! relay uses `SystemTime` for its idle clock). Values are kept small so the
//! suite stays under a few seconds.

mod common;

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use common::*;

#[tokio::test]
async fn connect_to_refused_target_fails_fast() {
    // Bind then drop → the addr likely has nothing listening (race-prone but
    // sufficient for tests; if rebound, connect succeeds and the test is a
    // weak no-op rather than a false failure).
    let bogus = unused_tcp_addr().await;
    let mut cfg = cfg_tcp(bogus);
    cfg.connect_timeout_secs = 2;
    let proxy = spawn_tcp_proxy(cfg).await;

    let start = Instant::now();
    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    // Write something so the handler runs the connect-to-target path
    let _ = conn.write_all(b"x").await;
    let mut buf = [0u8; 1];
    let _ = tokio::time::timeout(Duration::from_secs(3), conn.read(&mut buf)).await;
    let elapsed = start.elapsed();

    // ECONNREFUSED is returned synchronously by Linux loopback; should be
    // well under the connect_timeout.
    assert!(elapsed < Duration::from_secs(2), "elapsed {elapsed:?}");

    proxy.stop().await;
}

#[tokio::test]
async fn connect_timeout_caps_long_connect() {
    // TEST-NET-1 (RFC 5737) — guaranteed not to be reachable on the public
    // internet and not routable from a typical test host.
    let mut cfg = cfg_tcp("192.0.2.1:1".parse().unwrap());
    cfg.connect_timeout_secs = 1;
    let proxy = spawn_tcp_proxy(cfg).await;

    let start = Instant::now();
    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    let _ = conn.write_all(b"x").await;
    let mut buf = [0u8; 1];
    let _ = tokio::time::timeout(Duration::from_secs(5), conn.read(&mut buf)).await;
    let elapsed = start.elapsed();

    // Connect should be capped to ~1s (some slop for scheduling).
    assert!(
        elapsed < Duration::from_secs(3),
        "expected ≲1s, got {elapsed:?}"
    );

    proxy.stop().await;
}

#[tokio::test]
async fn idle_timeout_kills_silent_connection() {
    let blackhole = spawn_tcp_blackhole().await;
    let mut cfg = cfg_tcp(blackhole);
    cfg.idle_timeout_secs = 2;
    cfg.half_close_timeout_secs = 0;
    let proxy = spawn_tcp_proxy(cfg).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    // Send no data after connect → idle timer ticks → conn closed.
    let mut buf = [0u8; 1];
    let start = Instant::now();
    let n = tokio::time::timeout(Duration::from_secs(15), conn.read(&mut buf))
        .await
        .expect("idle_timeout never closed connection")
        .unwrap_or(0);
    let elapsed = start.elapsed();

    assert_eq!(n, 0, "expected EOF, got {n} bytes");
    // Watchdog ticks every 5s; expect to close within ~7s.
    assert!(
        elapsed < Duration::from_secs(10),
        "took too long: {elapsed:?}"
    );

    proxy.stop().await;
}

#[tokio::test]
async fn idle_timeout_resets_on_traffic() {
    let echo = spawn_tcp_echo().await;
    let mut cfg = cfg_tcp(echo);
    cfg.idle_timeout_secs = 3;
    cfg.half_close_timeout_secs = 0;
    let proxy = spawn_tcp_proxy(cfg).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();

    // Send a byte every 800ms for ~5s — well past the 3s idle window.
    // The activity should keep the connection alive.
    let start = Instant::now();
    let mut tick = 0u8;
    while start.elapsed() < Duration::from_secs(5) {
        conn.write_all(&[tick]).await.expect("write should succeed");
        let mut buf = [0u8; 1];
        conn.read_exact(&mut buf)
            .await
            .expect("read should succeed");
        assert_eq!(buf[0], tick);
        tick = tick.wrapping_add(1);
        tokio::time::sleep(Duration::from_millis(800)).await;
    }

    proxy.stop().await;
}

#[tokio::test]
async fn half_close_timeout_kills_stuck_server() {
    // Server sends a fixed message then refuses to close. After the client
    // EOFs, b→a is still receiving (nothing). L4 should fire.
    let server = spawn_tcp_send_then_hold(b"hi\n").await;
    let mut cfg = cfg_tcp(server);
    cfg.idle_timeout_secs = 0;
    cfg.half_close_timeout_secs = 2;
    let proxy = spawn_tcp_proxy(cfg).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();

    // Read the server's greeting
    let mut buf = [0u8; 3];
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hi\n");

    // Close our write side: client → upstream is now done.
    conn.shutdown().await.unwrap();

    // The server-side half stays open; L4 should close it within ~7s
    // (2s grace + up to 5s watchdog tick).
    let start = Instant::now();
    let mut rest = [0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(15), conn.read(&mut rest))
        .await
        .expect("half_close_timeout never fired")
        .unwrap_or(0);
    let elapsed = start.elapsed();

    assert_eq!(n, 0);
    assert!(
        elapsed < Duration::from_secs(10),
        "took too long: {elapsed:?}"
    );

    proxy.stop().await;
}

#[tokio::test]
async fn idle_zero_disables_timeout() {
    let blackhole = spawn_tcp_blackhole().await;
    let mut cfg = cfg_tcp(blackhole);
    cfg.idle_timeout_secs = 0; // disabled
    cfg.half_close_timeout_secs = 0;
    let proxy = spawn_tcp_proxy(cfg).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    let mut buf = [0u8; 1];
    // After 3s of silence, the connection should still be open.
    let res = tokio::time::timeout(Duration::from_secs(3), conn.read(&mut buf)).await;
    assert!(res.is_err(), "connection closed unexpectedly with idle=0");

    proxy.stop().await;
}
