//! Connection admission limits.
//!
//! Two layers, both per-proxy:
//!   * total cap        — protects the host (FDs, memory, scheduler).
//!   * per-source-IP cap — makes a basic flooding DOS less convenient.
//!     An IPv6 source counts by its /64, the smallest block a host gets.
//!
//! Behaviour:
//!   * On the first crossing of 90 % of either limit, emit a single `warn!`
//!     so operators see trouble coming before connections are dropped.
//!   * On every rejection at the hard limit, emit an `error!` with the
//!     source IP — formatted so log scrapers (fail2ban etc.) can match.
//!   * `try_acquire` returns a `Guard`; the slot is released when the guard
//!     drops, so RAII handles cleanup even on panic / cancellation.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::{Arc, Mutex};

use tracing::warn;

/// Why a connection was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reject {
    /// Global cap reached.
    Total,
    /// Per-source-IP cap reached.
    PerIp,
}

impl Reject {
    /// Stable label for logs and the `reason` metric label.
    pub fn label(self) -> &'static str {
        match self {
            Reject::Total => "total",
            Reject::PerIp => "per_ip",
        }
    }

    /// The configured cap that was hit.
    pub fn limit(self, limits: &ConnLimits) -> u32 {
        match self {
            Reject::Total => limits.max_total,
            Reject::PerIp => limits.max_per_ip,
        }
    }
}

/// RAII slot. Drop releases the slot back to the pool.
pub struct Guard {
    limits: Arc<ConnLimits>,
    src_ip: IpAddr,
}

#[derive(Default)]
struct State {
    total: u32,
    per_ip: HashMap<IpAddr, IpStat>,
    total_warned: bool,
}

#[derive(Default)]
struct IpStat {
    count: u32,
    warned: bool,
}

pub struct ConnLimits {
    pub max_total: u32,
    pub max_per_ip: u32,
    state: Mutex<State>,
}

impl ConnLimits {
    pub fn new(max_total: u32, max_per_ip: u32) -> Arc<Self> {
        Arc::new(Self {
            max_total,
            max_per_ip,
            state: Mutex::new(State::default()),
        })
    }

    /// Reserve a slot for a connection from `src_ip`. Either returns a
    /// `Guard` (which must be kept alive for the connection's lifetime) or
    /// the reason for rejection.
    ///
    /// `max_total == 0` or `max_per_ip == 0` disables that specific layer.
    pub fn try_acquire(self: &Arc<Self>, src_ip: IpAddr) -> Result<Guard, Reject> {
        let src_ip = per_ip_key(src_ip);
        let mut s = self.state.lock().expect("limits mutex poisoned");

        if self.max_total > 0 && s.total >= self.max_total {
            return Err(Reject::Total);
        }
        let current_ip = s.per_ip.get(&src_ip).map(|st| st.count).unwrap_or(0);
        if self.max_per_ip > 0 && current_ip >= self.max_per_ip {
            return Err(Reject::PerIp);
        }

        // Admit.
        s.total += 1;
        let total_now = s.total;
        let total_should_warn =
            !s.total_warned && self.max_total > 0 && reached_90pct(total_now, self.max_total);
        if total_should_warn {
            s.total_warned = true;
        }

        let stat = s.per_ip.entry(src_ip).or_default();
        stat.count += 1;
        let per_ip_count = stat.count;
        let per_ip_should_warn =
            !stat.warned && self.max_per_ip > 0 && reached_90pct(per_ip_count, self.max_per_ip);
        if per_ip_should_warn {
            stat.warned = true;
        }

        drop(s);

        if total_should_warn {
            warn!(
                used = total_now,
                limit = self.max_total,
                "approaching total connection limit (>=90%)"
            );
        }
        if per_ip_should_warn {
            warn!(
                %src_ip,
                used = per_ip_count,
                limit = self.max_per_ip,
                "approaching per-IP connection limit (>=90%)"
            );
        }

        Ok(Guard {
            limits: self.clone(),
            src_ip,
        })
    }

    /// Current total slot usage (for tests).
    #[cfg(test)]
    pub fn total(&self) -> u32 {
        self.state.lock().unwrap().total
    }

    /// Current per-IP usage (for tests).
    #[cfg(test)]
    pub fn per_ip(&self, ip: IpAddr) -> u32 {
        self.state
            .lock()
            .unwrap()
            .per_ip
            .get(&per_ip_key(ip))
            .map(|s| s.count)
            .unwrap_or(0)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let mut s = self.limits.state.lock().expect("limits mutex poisoned");
        s.total = s.total.saturating_sub(1);
        if let Some(stat) = s.per_ip.get_mut(&self.src_ip) {
            stat.count = stat.count.saturating_sub(1);
            if stat.count == 0 {
                s.per_ip.remove(&self.src_ip);
            }
        }
    }
}

/// What the per-IP cap counts a source as: an IPv6 address by its /64, since
/// one host can use every address in it. IPv4 (also IPv4-mapped) stays as is.
fn per_ip_key(ip: IpAddr) -> IpAddr {
    match ip.to_canonical() {
        IpAddr::V6(v6) => Ipv6Addr::from_bits(v6.to_bits() & !(u64::MAX as u128)).into(),
        v4 => v4,
    }
}

/// True once `used` has reached 90 % of `cap` (integer arithmetic; widened to
/// `u64` so it cannot overflow).
fn reached_90pct(used: u32, cap: u32) -> bool {
    (used as u64) * 10 >= (cap as u64) * 9
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn unlimited_when_zero() {
        let l = ConnLimits::new(0, 0);
        for _ in 0..1000 {
            l.try_acquire(ip("10.0.0.1")).unwrap();
        }
    }

    #[test]
    fn enforces_total_cap() {
        let l = ConnLimits::new(3, 0);
        let _g1 = l.try_acquire(ip("10.0.0.1")).unwrap();
        let _g2 = l.try_acquire(ip("10.0.0.2")).unwrap();
        let _g3 = l.try_acquire(ip("10.0.0.3")).unwrap();
        assert_eq!(l.try_acquire(ip("10.0.0.4")).err(), Some(Reject::Total));
    }

    #[test]
    fn enforces_per_ip_cap() {
        let l = ConnLimits::new(0, 2);
        let _g1 = l.try_acquire(ip("10.0.0.1")).unwrap();
        let _g2 = l.try_acquire(ip("10.0.0.1")).unwrap();
        assert_eq!(l.try_acquire(ip("10.0.0.1")).err(), Some(Reject::PerIp));
        // A different IP is still admitted.
        let _g3 = l.try_acquire(ip("10.0.0.2")).unwrap();
    }

    #[test]
    fn per_ip_cap_counts_an_ipv6_slash_64_as_one_source() {
        let l = ConnLimits::new(0, 2);
        let _g1 = l.try_acquire(ip("2001:db8:0:1::1")).unwrap();
        let _g2 = l.try_acquire(ip("2001:db8:0:1:ffff::2")).unwrap();
        assert_eq!(
            l.try_acquire(ip("2001:db8:0:1::3")).err(),
            Some(Reject::PerIp)
        );
        // The next /64 is a different source.
        let _g3 = l.try_acquire(ip("2001:db8:0:2::1")).unwrap();
    }

    #[test]
    fn per_ip_cap_counts_ipv4_mapped_as_ipv4() {
        let l = ConnLimits::new(0, 1);
        let _g1 = l.try_acquire(ip("::ffff:10.0.0.1")).unwrap();
        assert_eq!(l.try_acquire(ip("10.0.0.1")).err(), Some(Reject::PerIp));
        let _g2 = l.try_acquire(ip("::ffff:10.0.0.2")).unwrap();
    }

    #[test]
    fn slot_released_on_guard_drop() {
        let l = ConnLimits::new(1, 0);
        let g = l.try_acquire(ip("10.0.0.1")).unwrap();
        assert_eq!(l.try_acquire(ip("10.0.0.1")).err(), Some(Reject::Total));
        drop(g);
        // Now there's room again.
        let _g2 = l.try_acquire(ip("10.0.0.1")).unwrap();
    }

    #[test]
    fn per_ip_entry_cleaned_on_release() {
        let l = ConnLimits::new(0, 5);
        {
            let _g = l.try_acquire(ip("10.0.0.1")).unwrap();
            assert_eq!(l.per_ip(ip("10.0.0.1")), 1);
        }
        assert_eq!(l.per_ip(ip("10.0.0.1")), 0);
    }

    #[test]
    fn total_count_correct() {
        let l = ConnLimits::new(0, 0);
        let g1 = l.try_acquire(ip("10.0.0.1")).unwrap();
        let g2 = l.try_acquire(ip("10.0.0.2")).unwrap();
        let g3 = l.try_acquire(ip("10.0.0.2")).unwrap();
        assert_eq!(l.total(), 3);
        drop(g2);
        assert_eq!(l.total(), 2);
        drop(g1);
        drop(g3);
        assert_eq!(l.total(), 0);
    }

    #[test]
    fn reached_90pct_threshold() {
        assert!(!reached_90pct(89, 100));
        assert!(reached_90pct(90, 100));
        assert!(reached_90pct(100, 100));
        assert!(!reached_90pct(8, 10));
        assert!(reached_90pct(9, 10));
        // Edge: large numbers don't overflow.
        assert!(reached_90pct(u32::MAX / 10 * 9, u32::MAX / 10 * 10));
    }
}
