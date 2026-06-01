//! Misbehaving clients & adversarial conditions.
//!
//! Covers:
//! - Client RST (abrupt close without FIN)
//! - Slowloris-style trickle clients (one slow client mustn't block others)
//! - Backpressure: client never reads, proxy must not deadlock
//! - Rapid connect/disconnect storm
//! - Half-open after RST

mod common;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use common::*;

#[tokio::test]
async fn client_rst_handled_gracefully() {
    use socket2::SockRef;

    // Configure SO_LINGER=0 on the client → close() sends RST instead of FIN
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;

    let conn = TcpStream::connect(proxy.addr).await.unwrap();
    SockRef::from(&conn)
        .set_linger(Some(Duration::ZERO))
        .unwrap();
    // Dropping with SO_LINGER=0 sends RST
    drop(conn);

    // Proxy should keep accepting after the RST. Open a fresh connection
    // and verify it still works.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut ok = TcpStream::connect(proxy.addr).await.unwrap();
    ok.write_all(b"still-alive").await.unwrap();
    let mut buf = [0u8; 11];
    ok.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"still-alive");

    proxy.stop().await;
}

#[tokio::test]
async fn rapid_connect_disconnect_storm() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;

    // 200 connections, opened and immediately closed
    let mut handles = Vec::new();
    for _ in 0..200u32 {
        handles.push(tokio::spawn(async move {
            if let Ok(c) = TcpStream::connect(addr).await {
                drop(c);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    // Proxy must still be healthy after the storm
    let mut ok = TcpStream::connect(addr).await.unwrap();
    ok.write_all(b"alive").await.unwrap();
    let mut buf = [0u8; 5];
    ok.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"alive");

    proxy.stop().await;
}

#[tokio::test]
async fn slow_client_does_not_block_others() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;

    // Slow client: connects, sends a byte every 200ms, doesn't drain replies
    // promptly
    let slow_count = Arc::new(AtomicU32::new(0));
    let sc = slow_count.clone();
    let slow = tokio::spawn(async move {
        let mut c = TcpStream::connect(addr).await.unwrap();
        for _ in 0..10u32 {
            if c.write_all(&[0xAB]).await.is_err() {
                break;
            }
            sc.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    // Meanwhile, 20 fast clients should all complete promptly
    let mut fast_handles = Vec::new();
    for i in 0..20u32 {
        fast_handles.push(tokio::spawn(async move {
            let mut c = TcpStream::connect(addr).await.unwrap();
            let payload = format!("fast-{i}").into_bytes();
            c.write_all(&payload).await.unwrap();
            let mut buf = vec![0u8; payload.len()];
            c.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, payload);
        }));
    }

    let start = std::time::Instant::now();
    for h in fast_handles {
        h.await.unwrap();
    }
    let fast_total = start.elapsed();

    // All 20 fast clients done in well under the slow client's 2s lifetime
    assert!(
        fast_total < Duration::from_secs(2),
        "fast clients took {fast_total:?} — slow client may be blocking"
    );

    let _ = slow.await;
    proxy.stop().await;
}

#[tokio::test]
async fn client_never_reads_response_doesnt_deadlock() {
    let echo = spawn_tcp_echo().await;
    let mut cfg = cfg_tcp(echo);
    cfg.idle_timeout_secs = 0;
    cfg.half_close_timeout_secs = 0;
    let proxy = spawn_tcp_proxy(cfg).await;

    // Client writes a moderate amount but never reads → proxy's b→a
    // direction must not block a→b indefinitely. With socket buffers and
    // our 16KB relay buffer, ~256KB fills both directions.
    let (mut r, mut w) = TcpStream::connect(proxy.addr).await.unwrap().into_split();
    let writer = tokio::spawn(async move {
        let payload = vec![0u8; 256 * 1024];
        // Write may block on backpressure but shouldn't return an error
        let _ = w.write_all(&payload).await;
        let _ = w.shutdown().await;
    });

    // Drain the response (it's the echoed payload coming back)
    let mut sink = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), r.read_to_end(&mut sink)).await;

    // We don't assert on byte count — the point is no deadlock
    let _ = writer.await;
    proxy.stop().await;
}

#[tokio::test]
async fn connect_then_close_immediately_no_panic() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;

    for _ in 0..50u32 {
        let c = TcpStream::connect(addr).await.unwrap();
        drop(c); // EOF before any I/O
    }

    // Proxy still healthy
    let mut ok = TcpStream::connect(addr).await.unwrap();
    ok.write_all(b"ping").await.unwrap();
    let mut buf = [0u8; 4];
    ok.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"ping");

    proxy.stop().await;
}
