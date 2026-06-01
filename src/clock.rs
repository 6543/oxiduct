//! Monotonic millisecond clock for the proxy liveness timers.
//!
//! Uses `std::time::Instant` so the idle / half-close math is immune to
//! wall-clock jumps (NTP steps, suspend/resume). All durations in oxiduct are
//! relative, so an arbitrary fixed origin is all we need.

use std::sync::OnceLock;
use std::time::Instant;

/// Milliseconds since the first call to this function. Monotonic.
pub fn now_ms() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN.get_or_init(Instant::now).elapsed().as_millis() as u64
}
