//! Process-level shutdown grace behavior.

#![cfg(unix)]

mod common;

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use common::spawn_tcp_echo;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}

async fn connect_with_retry(addr: std::net::SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match TcpStream::connect(addr).await {
            Ok(stream) => return stream,
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(e) => panic!("proxy did not start: {e}"),
        }
    }
}

#[tokio::test]
async fn shutdown_grace_keeps_active_connection_alive() {
    const SIGTERM: i32 = 15;

    let echo = spawn_tcp_echo().await;
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listen = reservation.local_addr().unwrap();
    drop(reservation);

    let child = Command::new(env!("CARGO_BIN_EXE_oxiduct"))
        .args([
            "--listen",
            &listen.to_string(),
            "--target",
            &echo.to_string(),
            "--shutdown-grace",
            "2",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);

    let mut connection = connect_with_retry(listen).await;
    connection.write_all(b"before").await.unwrap();
    let mut buf = [0u8; 6];
    connection.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"before");
    tokio::time::sleep(Duration::from_millis(50)).await;

    // SAFETY: child.id() is a live process created by this test; SIGTERM does
    // not violate memory safety. ChildGuard ensures cleanup on every exit path.
    let result = unsafe { kill(child.0.id() as i32, SIGTERM) };
    assert_eq!(result, 0, "failed to send SIGTERM to proxy");
    tokio::time::sleep(Duration::from_millis(200)).await;

    connection.write_all(b"during").await.unwrap();
    let echoed = tokio::time::timeout(Duration::from_millis(500), connection.read_exact(&mut buf))
        .await
        .expect("active connection stopped before shutdown grace elapsed")
        .expect("active connection was closed immediately on SIGTERM");
    assert_eq!(echoed, 6);
    assert_eq!(&buf, b"during");

    drop(connection);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if child.0.try_wait().unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "proxy did not finish shutdown");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
