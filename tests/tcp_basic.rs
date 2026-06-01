//! Basic TCP proxy functionality: connection setup, bidirectional bytes,
//! clean EOF propagation, and concurrency.

mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use common::*;

#[tokio::test]
async fn echo_small_payload() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    conn.write_all(b"hello").await.unwrap();

    let mut buf = [0u8; 5];
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    proxy.stop().await;
}

#[tokio::test]
async fn echo_multiple_sequential_writes() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();

    for msg in ["one", "two", "three", "four"] {
        conn.write_all(msg.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; msg.len()];
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, msg.as_bytes());
    }

    proxy.stop().await;
}

#[tokio::test]
async fn large_transfer_one_megabyte() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;

    let (mut r, mut w) = TcpStream::connect(proxy.addr).await.unwrap().into_split();
    let payload: Vec<u8> = (0..1_000_000).map(|i| (i % 251) as u8).collect();
    let payload_clone = payload.clone();

    let writer = tokio::spawn(async move {
        w.write_all(&payload_clone).await.unwrap();
        w.shutdown().await.unwrap();
    });

    let mut received = Vec::with_capacity(payload.len());
    r.read_to_end(&mut received).await.unwrap();
    writer.await.unwrap();

    assert_eq!(received.len(), payload.len());
    assert_eq!(received, payload);

    proxy.stop().await;
}

#[tokio::test]
async fn client_eof_propagates_to_server() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    conn.write_all(b"x").await.unwrap();
    let mut byte = [0u8; 1];
    conn.read_exact(&mut byte).await.unwrap();

    // Close the write half: should propagate as EOF through the proxy to
    // the echo server, which then closes its end, which the proxy reads
    // as EOF on the b→a direction and closes our read half.
    conn.shutdown().await.unwrap();

    // Reading should now hit EOF in finite time.
    let mut buf = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(3), conn.read(&mut buf))
        .await
        .expect("read didn't EOF in time")
        .unwrap();
    assert_eq!(n, 0);

    proxy.stop().await;
}

#[tokio::test]
async fn zero_byte_transfer_clean_close() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    conn.shutdown().await.unwrap();
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(3), conn.read(&mut buf))
        .await
        .expect("read didn't EOF in time")
        .unwrap();
    assert_eq!(n, 0);

    proxy.stop().await;
}

#[tokio::test]
async fn many_concurrent_connections() {
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;

    let mut handles = Vec::new();
    for i in 0..50u32 {
        handles.push(tokio::spawn(async move {
            let mut conn = TcpStream::connect(addr).await.unwrap();
            let payload = format!("conn-{i:04}").into_bytes();
            conn.write_all(&payload).await.unwrap();
            let mut buf = vec![0u8; payload.len()];
            conn.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, payload);
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    proxy.stop().await;
}

#[tokio::test]
async fn rapid_sequential_connections() {
    // Each connection is opened, used, and closed before the next.
    let echo = spawn_tcp_echo().await;
    let proxy = spawn_tcp_proxy(cfg_tcp(echo)).await;
    let addr = proxy.addr;

    for i in 0..30u32 {
        let mut conn = TcpStream::connect(addr).await.unwrap();
        let payload = format!("seq-{i}").into_bytes();
        conn.write_all(&payload).await.unwrap();
        let mut buf = vec![0u8; payload.len()];
        conn.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, payload);
    }

    proxy.stop().await;
}
