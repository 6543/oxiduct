//! PROXY protocol v2 emission: when `proxy_protocol` is enabled the proxy
//! must prepend a well-formed v2 header (announcing the real client address)
//! as the first bytes upstream, and only then relay client payload. When the
//! flag is off, no header is sent and bytes pass through untouched.

mod common;

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use common::*;

const SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// Upstream that reads exactly an IPv4 PROXY v2 header (28 bytes), hands it
/// back over a channel, then echoes whatever payload follows.
async fn spawn_header_capture() -> (SocketAddr, oneshot::Receiver<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut header = [0u8; 28];
        stream.read_exact(&mut header).await.unwrap();
        let _ = tx.send(header.to_vec());
        // Echo the payload that follows the header.
        let mut buf = vec![0u8; 4096];
        loop {
            match stream.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if stream.write_all(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    (addr, rx)
}

#[tokio::test]
async fn emits_v2_header_with_real_client_address() {
    let (target, header_rx) = spawn_header_capture().await;

    let mut cfg = cfg_tcp(target);
    cfg.proxy_protocol = true;
    let proxy = spawn_tcp_proxy(cfg).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    let client_addr = conn.local_addr().unwrap();

    // Payload after the header must still be relayed and echoed back.
    conn.write_all(b"ping").await.unwrap();
    let mut echo = [0u8; 4];
    conn.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"ping");

    let header = header_rx.await.unwrap();
    assert_eq!(&header[..12], &SIGNATURE, "signature");
    assert_eq!(header[12], 0x21, "version 2 + PROXY command");
    assert_eq!(header[13], 0x11, "AF_INET + STREAM");
    assert_eq!(
        &header[14..16],
        &12u16.to_be_bytes(),
        "address block length"
    );
    assert_eq!(&header[16..20], &[127, 0, 0, 1], "src addr = real client");
    assert_eq!(
        &header[20..24],
        &[127, 0, 0, 1],
        "dst addr = proxy listener"
    );
    assert_eq!(
        &header[24..26],
        &client_addr.port().to_be_bytes(),
        "src port = real client port"
    );
    assert_eq!(
        &header[26..28],
        &proxy.addr.port().to_be_bytes(),
        "dst port = proxy listener port"
    );

    proxy.stop().await;
}

#[tokio::test]
async fn no_header_when_disabled() {
    // With the flag off, the upstream must receive the raw payload first —
    // no PROXY header. Reading 4 bytes off a plain echo upstream yields the
    // payload, proving nothing was prepended.
    let echo = spawn_tcp_echo().await;
    let cfg = cfg_tcp(echo); // proxy_protocol defaults to false
    let proxy = spawn_tcp_proxy(cfg).await;

    let mut conn = TcpStream::connect(proxy.addr).await.unwrap();
    conn.write_all(b"raw!").await.unwrap();
    let mut buf = [0u8; 4];
    conn.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"raw!");

    proxy.stop().await;
}
