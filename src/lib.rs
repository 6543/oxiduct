//! oxiduct — robust TCP/UDP proxy library.
//!
//! The `oxiduct` binary is a thin wrapper around the modules in this crate.
//! All real logic lives here so integration tests can exercise it directly.

pub mod cli;
pub mod config;
pub mod proxy;
pub mod socket_opts;
