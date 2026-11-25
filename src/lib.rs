//! A `RouterOS` native API client for Rust, over TCP and TLS.
//!
//! `connect` dials and logs in with one `/login` message, the modern `RouterOS`
//! API login used since 6.43. [`Client::command`] runs one command and returns
//! every record in wire order with the final `!done` attributes;
//! [`Client::stream`] receives the `!re` records of a long-running command
//! through a bounded queue with TCP backpressure.
//!
//! The client ships no command catalog: every `RouterOS` path and attribute is
//! caller input. Values are strings, exactly as they travel on the wire.
//!
//! # Example
//!
//! ```no_run
//! use ros_rs::{Config, Request, connect};
//!
//! # async fn run() -> Result<(), ros_rs::Error> {
//! let client = connect(Config::new("router.example.com", "api", b"secret".to_vec())).await?;
//! let result = client
//!     .command("/system/identity/print", &Request::new())
//!     .await?;
//! println!("{:?}", result.done.get("name"));
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod cancel;
mod client;
mod config;
mod error;
pub mod query;
mod record;
mod request;
mod socket;
#[cfg(feature = "tls")]
mod tls;
mod transport;
mod wire;

pub use cancel::Cancel;
pub use client::{Client, State, Stream};
pub use config::{Config, DEFAULT_PLAINTEXT_PORT, DEFAULT_TLS_PORT};
pub use error::{Error, Result};
pub use query::Expr;
pub use record::{CommandResult, Record};
pub use request::{Attribute, Request};
#[cfg(feature = "tls")]
pub use tls::Tls;

/// Dial, log in, and return a ready client.
///
/// # Errors
///
/// Returns [`Error::Config`] when the configuration is invalid,
/// [`Error::Connection`] when the dial fails, [`Error::Tls`] when the TLS
/// settings cannot be built, [`Error::Login`] when the router rejects the
/// credentials, and [`Error::Timeout`] when dialling or login exceeds the
/// configured deadline.
pub async fn connect(config: Config) -> Result<Client> {
    client::connect(config).await
}

/// Like [`connect`], with a caller-supplied cancellation token.
///
/// # Errors
///
/// The same failures as [`connect`], plus [`Error::Cancelled`] when `cancel`
/// fires before the login exchange finishes.
pub async fn connect_cancellable(config: Config, cancel: &Cancel) -> Result<Client> {
    client::connect_cancellable(config, cancel).await
}
