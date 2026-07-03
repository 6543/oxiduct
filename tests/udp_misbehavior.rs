//! UDP edge cases: shutdown propagation, target unresolvable, bind clash.

mod common;

use std::time::Duration;

use tokio::net::UdpSocket;

use common::*;

#[tokio::test]
async fn cancel_stops_listener() {
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    // Confirm working before cancel
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();
    client.send(b"alive").await.unwrap();
    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"alive");

    proxy.shutdown.cancel();
    let res = tokio::time::timeout(Duration::from_secs(3), proxy.task).await;
    assert!(res.is_ok(), "UDP serve didn't exit on cancel");
}

#[tokio::test]
async fn target_dns_failure_doesnt_kill_listener() {
    // Target is an invalid host:port — DNS lookup fails when a session is
    // first opened. The listener should keep running.
    let mut cfg = cfg_udp("127.0.0.1:1".parse().unwrap());
    cfg.target = "this-host-cannot-possibly-exist.invalid:9999".into();
    let proxy = spawn_udp_proxy(cfg).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();
    // First send triggers session-open which will fail
    client.send(b"hi").await.unwrap();

    // Listener should still be alive — try a second send (which will trigger
    // another failed open, also logged but non-fatal)
    tokio::time::sleep(Duration::from_millis(200)).await;
    let send_again = client.send(b"hi again").await;
    assert!(send_again.is_ok(), "listener died after DNS failure");

    proxy.stop().await;
}

#[tokio::test]
async fn bind_already_in_use_returns_error() {
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

    let blocker = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = blocker.local_addr().unwrap();

    let mut cfg = cfg_udp("127.0.0.1:1".parse().unwrap());
    cfg.listen = addr.to_string();

    let result = oxiduct::proxy::udp::run(
        Arc::new(cfg),
        oxiduct::metrics::Metrics::new(),
        CancellationToken::new(),
    )
    .await;
    assert!(result.is_err(), "expected bind failure, got {result:?}");

    drop(blocker);
}

#[tokio::test]
async fn ipv6_loopback_target() {
    // Skip silently on systems without IPv6 loopback support
    let echo_sock = match UdpSocket::bind("[::1]:0").await {
        Ok(s) => s,
        Err(_) => return,
    };
    let echo_addr = echo_sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65535];
        loop {
            let (n, src) = match echo_sock.recv_from(&mut buf).await {
                Ok(x) => x,
                Err(_) => return,
            };
            let _ = echo_sock.send_to(&buf[..n], src).await;
        }
    });

    let proxy = spawn_udp_proxy(cfg_udp(echo_addr)).await;
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();
    client.send(b"v6").await.unwrap();

    let mut buf = [0u8; 8];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("v6 echo timed out")
        .unwrap();
    assert_eq!(&buf[..n], b"v6");

    proxy.stop().await;
}

/// Regression: a session whose relay task dies (e.g. ICMP port unreachable
/// surfacing as ECONNREFUSED on the connected upstream socket) must not
/// linger as a zombie. Its limits slot must be released and the next packet
/// from the same source must transparently get a fresh session, even with
/// idle_timeout disabled and max_connections = 1.
#[tokio::test]
async fn dead_session_evicted_and_recreated() {
    // Target: a UDP port that is bound, then dropped -> nothing listens.
    // On Linux, sends to it trigger ICMP unreachable and the connected
    // upstream socket errors out, killing the session's relay tasks.
    let dead = {
        let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = s.local_addr().unwrap();
        drop(s);
        addr
    };

    let mut cfg = cfg_udp(dead);
    cfg.name = "test".into();
    cfg.max_connections = 1; // any slot leak would reject the next session
    cfg.idle_timeout_secs = 0; // no idle eviction to hide the leak

    let metrics = oxiduct::metrics::Metrics::new();
    let proxy = spawn_udp_proxy_with_metrics(cfg, metrics.clone()).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    // Each send may hit a live-but-doomed or an already-dead session; either
    // way no send may ever be rejected by the limiter.
    for _ in 0..5 {
        client.send(b"ping").await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    let rejected: u64 = ["total", "per_ip"]
        .iter()
        .map(|r| {
            metrics
                .connections_rejected
                .with_label_values(&["test", r])
                .get()
        })
        .sum();
    assert_eq!(rejected, 0, "dead session leaked its limits slot");

    // At least one re-created session proves eviction happened.
    let opened = metrics
        .connections_total
        .with_label_values(&["test", "udp"])
        .get();
    assert!(opened >= 2, "expected session re-creation, got {opened}");

    proxy.stop().await;
}
