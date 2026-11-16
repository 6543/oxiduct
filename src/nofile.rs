//! Open-file limit. Every TCP connection holds two descriptors, every UDP
//! session one, so the usual soft limit of 1024 is far below the default
//! connection caps. Past it accept() fails and clients hang unserved.

use std::io;

use crate::config::{Protocol, ProxyConfig};

/// Descriptors the connection caps of `proxies` allow in use at once, or
/// `None` when a proxy has no cap.
pub fn fds_needed(proxies: &[ProxyConfig]) -> Option<u64> {
    proxies
        .iter()
        .map(|p| {
            let per_conn = match p.protocol {
                Protocol::Tcp => 2,
                Protocol::Udp => 1,
            };
            // + 1 for the listening socket.
            (p.max_connections > 0).then(|| 1 + per_conn * u64::from(p.max_connections))
        })
        .sum()
}

/// Raise the soft open-file limit to the hard one. Returns the limit in
/// effect afterwards.
#[cfg(unix)]
#[allow(clippy::unnecessary_cast)] // rlim_t is not u64 everywhere
pub fn raise() -> io::Result<u64> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: both calls only read or write `lim`, which outlives them.
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) != 0 {
            return Err(io::Error::last_os_error());
        }
        let soft = lim.rlim_cur;
        lim.rlim_cur = lim.rlim_max;
        // macOS refuses its own hard limit of "infinity": keep the old one,
        // the caller then warns if that is too low.
        if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) != 0 {
            lim.rlim_cur = soft;
        }
    }
    Ok(lim.rlim_cur as u64)
}

#[cfg(not(unix))]
pub fn raise() -> io::Result<u64> {
    Ok(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(protocol: Protocol, max_connections: u32) -> ProxyConfig {
        ProxyConfig {
            name: "test".into(),
            listen: "127.0.0.1:0".into(),
            target: "127.0.0.1:0".into(),
            protocol,
            connect_timeout_secs: 3,
            keepalive_idle_secs: 0,
            keepalive_interval_secs: 0,
            keepalive_retries: 0,
            user_timeout_ms: 0,
            idle_timeout_secs: 0,
            half_close_timeout_secs: 0,
            max_connections,
            max_per_ip: 0,
            proxy_protocol: false,
        }
    }

    #[test]
    fn tcp_needs_two_per_connection_udp_one() {
        let proxies = [cfg(Protocol::Tcp, 100), cfg(Protocol::Udp, 100)];
        assert_eq!(fds_needed(&proxies), Some(201 + 101));
    }

    #[test]
    fn unknown_when_a_proxy_has_no_cap() {
        let proxies = [cfg(Protocol::Tcp, 100), cfg(Protocol::Udp, 0)];
        assert_eq!(fds_needed(&proxies), None);
    }

    #[cfg(unix)]
    #[test]
    fn raise_never_lowers_and_is_stable() {
        let first = raise().unwrap();
        assert!(first > 0);
        assert_eq!(raise().unwrap(), first);
    }
}
