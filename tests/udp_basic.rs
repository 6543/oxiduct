//! Basic UDP proxy functionality: datagram round-trip, byte preservation.

mod common;

use std::time::Duration;

use tokio::net::UdpSocket;

use common::*;

#[tokio::test]
async fn echo_single_packet() {
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    client.send(b"hello udp").await.unwrap();

    let mut buf = [0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("timed out waiting for echo")
        .unwrap();
    assert_eq!(&buf[..n], b"hello udp");

    proxy.stop().await;
}

#[tokio::test]
async fn multiple_packets_same_client() {
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    for i in 0..10u32 {
        let payload = format!("pkt-{i}").into_bytes();
        client.send(&payload).await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
            .await
            .expect("recv timeout")
            .unwrap();
        assert_eq!(&buf[..n], &payload[..]);
    }

    proxy.stop().await;
}

#[tokio::test]
async fn large_packet_near_mtu() {
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    // 1400 bytes — comfortably under typical link MTU
    let payload: Vec<u8> = (0..1400).map(|i| (i % 251) as u8).collect();
    client.send(&payload).await.unwrap();

    let mut buf = vec![0u8; 2048];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(n, payload.len());
    assert_eq!(&buf[..n], &payload[..]);

    proxy.stop().await;
}

#[tokio::test]
async fn empty_datagram_relayed() {
    let echo = spawn_udp_echo().await;
    let proxy = spawn_udp_proxy(cfg_udp(echo)).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();

    client.send(&[]).await.unwrap();
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(2), client.recv(&mut buf))
        .await
        .expect("recv timeout")
        .unwrap();
    assert_eq!(n, 0);

    proxy.stop().await;
}

#[tokio::test]
async fn target_no_response_eventually_silent() {
    let blackhole = spawn_udp_blackhole().await;
    let mut cfg = cfg_udp(blackhole);
    cfg.idle_timeout_secs = 2;
    let proxy = spawn_udp_proxy(cfg).await;

    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(proxy.addr).await.unwrap();
    client.send(b"hello?").await.unwrap();

    // Nothing should ever come back
    let mut buf = [0u8; 64];
    let res = tokio::time::timeout(Duration::from_millis(500), client.recv(&mut buf)).await;
    assert!(res.is_err(), "got unexpected response from blackhole");

    proxy.stop().await;
}
