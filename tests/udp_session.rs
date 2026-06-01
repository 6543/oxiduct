//! UDP session lifecycle: per-source isolation, reuse, eviction.

mod common;

use std::time::Duration;

use tokio::net::UdpSocket;

use common::*;

#[tokio::test]
async fn different_sources_get_distinct_responses() {
    // The echo server replies to each source independently. Two clients
    // sending overlapping payloads must each get their own response back —
    // proxies that lose track of source addr would cross-talk.
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let c1 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let c2 = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    c1.connect(proxy.addr).await.unwrap();
    c2.connect(proxy.addr).await.unwrap();

    c1.send(b"client-one").await.unwrap();
    c2.send(b"client-two").await.unwrap();

    let mut b1 = [0u8; 32];
    let mut b2 = [0u8; 32];
    let n1 = tokio::time::timeout(Duration::from_secs(2), c1.recv(&mut b1))
        .await
        .unwrap()
        .unwrap();
    let n2 = tokio::time::timeout(Duration::from_secs(2), c2.recv(&mut b2))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(&b1[..n1], b"client-one");
    assert_eq!(&b2[..n2], b"client-two");

    proxy.stop().await;
}

#[tokio::test]
async fn same_client_keeps_session_across_packets() {
    // Verifies session reuse: 5 consecutive sends from the same client port
    // should all be relayed (and replies received) without packet loss
    // beyond what UDP normally tolerates on loopback (≈zero).
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    for i in 0..5u32 {
        let payload = format!("reuse-{i}");
        client.send(payload.as_bytes()).await.unwrap();
        let mut buf = [0u8; 32];
        let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..n], payload.as_bytes());
    }

    proxy.stop().await;
}

#[tokio::test]
async fn idle_session_evicted_then_recreated() {
    let echo = spawn_udp_echo().await;
    let mut cfg = cfg_udp(echo);
    cfg.idle_timeout_secs = 2; // short
    let proxy = spawn_udp_proxy(cfg).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    // First exchange
    client.send(b"first").await.unwrap();
    let mut buf = [0u8; 32];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"first");

    // Wait past idle: cleanup task runs every 5s, so allow ≥7s
    tokio::time::sleep(Duration::from_secs(8)).await;

    // Second exchange must still succeed — new session is created
    client.send(b"second").await.unwrap();
    let mut buf = [0u8; 32];
    let n = tokio::time::timeout(Duration::from_secs(3), client.recv(&mut buf))
        .await
        .expect("eviction broke post-eviction sending")
        .unwrap();
    assert_eq!(&buf[..n], b"second");

    proxy.stop().await;
}

#[tokio::test]
async fn many_distinct_sources_no_panic() {
    // Open 100 UDP sockets, each sends one packet, each gets a session.
    // Verifies the session map handles many entries without panicking.
    // Note: there's currently no cap → this is also a DoS vector documented
    // in the security review.
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let mut handles = Vec::new();
    for i in 0..100u32 {
        let target = proxy.addr;
        handles.push(tokio::spawn(async move {
            let c = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            c.connect(target).await.unwrap();
            let payload = format!("flood-{i:03}").into_bytes();
            c.send(&payload).await.unwrap();
            let mut buf = [0u8; 32];
            let _ = tokio::time::timeout(Duration::from_secs(3), c.recv(&mut buf)).await;
        }));
    }
    for h in handles {
        let _ = h.await;
    }

    // Proxy must still be responsive afterwards
    let c = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    c.connect(proxy.addr).await.unwrap();
    c.send(b"alive?").await.unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), c.recv(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&buf[..n], b"alive?");

    proxy.stop().await;
}
