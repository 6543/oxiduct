//! PROXY protocol v2 header construction.
//!
//! oxiduct is an L4 relay: without help, the backend only ever sees oxiduct's
//! own address as the source of every connection. That collapses every client
//! onto one IP, which breaks per-IP rate limiting, auto-banning and any
//! source-address policy on the backend (SPF/DMARC for mail, for example).
//!
//! Emitting a [PROXY protocol] v2 header as the very first bytes on the
//! upstream connection lets a PROXY-aware backend (HAProxy, NGINX, Traefik,
//! Stalwart, …) recover the real client address. The backend must be
//! configured to trust this proxy's address before it will honour the header.
//!
//! [PROXY protocol]: https://www.haproxy.org/download/2.9/doc/proxy-protocol.txt

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// The 12-byte v2 signature: `\r\n\r\n\0\r\nQUIT\n`.
const SIGNATURE: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// Version 2 + `PROXY` command (real addresses follow).
const VER_CMD_PROXY: u8 = 0x21;
/// Version 2 + `LOCAL` command (no addresses; receiver uses the real peer).
const VER_CMD_LOCAL: u8 = 0x20;
/// Address family `AF_INET`, transport `STREAM`.
const TP_TCP4: u8 = 0x11;
/// Address family `AF_INET6`, transport `STREAM`.
const TP_TCP6: u8 = 0x21;
/// Address family `AF_UNSPEC`, transport `UNSPEC`.
const TP_UNSPEC: u8 = 0x00;

/// Collapse an IPv4-mapped IPv6 address (`::ffff:a.b.c.d`) to its IPv4 form so
/// `src` and `dst` share an address family.
///
/// MSRV (1.71) predates `IpAddr::to_canonical` (stable 1.75), so do it by hand.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => {
            let o = v6.octets();
            let is_v4_mapped = o[..10].iter().all(|&b| b == 0) && o[10] == 0xff && o[11] == 0xff;
            if is_v4_mapped {
                IpAddr::V4(Ipv4Addr::new(o[12], o[13], o[14], o[15]))
            } else {
                IpAddr::V6(v6)
            }
        }
        v4 => v4,
    }
}

/// Build a PROXY protocol v2 header announcing `src` (the real downstream
/// client) and `dst` (the local address that client connected to on us).
///
/// On the rare address-family mismatch between `src` and `dst` (after
/// canonicalising IPv4-mapped IPv6), a `LOCAL` command with an empty address
/// block is emitted instead of a malformed `PROXY` header; a compliant
/// receiver then falls back to the real TCP peer.
pub fn v2_header(src: SocketAddr, dst: SocketAddr) -> Vec<u8> {
    let mut buf = Vec::with_capacity(28); // exact for the IPv4 case
    buf.extend_from_slice(&SIGNATURE);

    match (canonical(src.ip()), canonical(dst.ip())) {
        (IpAddr::V4(s), IpAddr::V4(d)) => {
            buf.push(VER_CMD_PROXY);
            buf.push(TP_TCP4);
            buf.extend_from_slice(&12u16.to_be_bytes()); // 4 + 4 + 2 + 2
            buf.extend_from_slice(&s.octets());
            buf.extend_from_slice(&d.octets());
            buf.extend_from_slice(&src.port().to_be_bytes());
            buf.extend_from_slice(&dst.port().to_be_bytes());
        }
        (IpAddr::V6(s), IpAddr::V6(d)) => {
            buf.push(VER_CMD_PROXY);
            buf.push(TP_TCP6);
            buf.extend_from_slice(&36u16.to_be_bytes()); // 16 + 16 + 2 + 2
            buf.extend_from_slice(&s.octets());
            buf.extend_from_slice(&d.octets());
            buf.extend_from_slice(&src.port().to_be_bytes());
            buf.extend_from_slice(&dst.port().to_be_bytes());
        }
        _ => {
            buf.push(VER_CMD_LOCAL);
            buf.push(TP_UNSPEC);
            buf.extend_from_slice(&0u16.to_be_bytes());
        }
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv6Addr, SocketAddrV4, SocketAddrV6};

    fn v4(a: u8, b: u8, c: u8, d: u8, port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(a, b, c, d), port))
    }

    fn v6(addr: Ipv6Addr, port: u16) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(addr, port, 0, 0))
    }

    #[test]
    fn signature_always_present() {
        let h = v2_header(v4(1, 2, 3, 4, 1), v4(5, 6, 7, 8, 2));
        assert_eq!(&h[..12], &SIGNATURE);
    }

    #[test]
    fn ipv4_layout_is_exact() {
        // src 192.0.2.1:51000 -> dst 198.51.100.2:25
        let h = v2_header(v4(192, 0, 2, 1, 51_000), v4(198, 51, 100, 2, 25));
        assert_eq!(h.len(), 28, "12 sig + 4 fixed + 12 addr block");
        assert_eq!(h[12], VER_CMD_PROXY);
        assert_eq!(h[13], TP_TCP4);
        assert_eq!(&h[14..16], &12u16.to_be_bytes()); // address block length
        assert_eq!(&h[16..20], &[192, 0, 2, 1]); // src addr
        assert_eq!(&h[20..24], &[198, 51, 100, 2]); // dst addr
        assert_eq!(&h[24..26], &51_000u16.to_be_bytes()); // src port
        assert_eq!(&h[26..28], &25u16.to_be_bytes()); // dst port
    }

    #[test]
    fn ipv6_layout_is_exact() {
        let s = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
        let d = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 2);
        let h = v2_header(v6(s, 40_000), v6(d, 587));
        assert_eq!(h.len(), 52, "12 sig + 4 fixed + 36 addr block");
        assert_eq!(h[12], VER_CMD_PROXY);
        assert_eq!(h[13], TP_TCP6);
        assert_eq!(&h[14..16], &36u16.to_be_bytes());
        assert_eq!(&h[16..32], &s.octets());
        assert_eq!(&h[32..48], &d.octets());
        assert_eq!(&h[48..50], &40_000u16.to_be_bytes());
        assert_eq!(&h[50..52], &587u16.to_be_bytes());
    }

    #[test]
    fn ipv4_mapped_v6_collapses_to_v4() {
        // ::ffff:192.0.2.1 must be treated as an IPv4 source.
        let mapped = Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0xc000, 0x0201);
        let h = v2_header(v6(mapped, 1234), v4(198, 51, 100, 2, 25));
        assert_eq!(h.len(), 28);
        assert_eq!(h[13], TP_TCP4);
        assert_eq!(&h[16..20], &[192, 0, 2, 1]);
    }

    #[test]
    fn family_mismatch_emits_local() {
        // genuine v6 src, v4 dst -> cannot describe in one family -> LOCAL.
        let s = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1);
        let h = v2_header(v6(s, 1234), v4(198, 51, 100, 2, 25));
        assert_eq!(h.len(), 16, "12 sig + 4 fixed, no address block");
        assert_eq!(h[12], VER_CMD_LOCAL);
        assert_eq!(h[13], TP_UNSPEC);
        assert_eq!(&h[14..16], &0u16.to_be_bytes());
    }
}
