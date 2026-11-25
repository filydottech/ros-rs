//! The client's typed error hierarchy.
//!
//! Every failure the public API reports is an [`Error`]. Variants are stable
//! categories a caller can match on; messages carry detail, never credentials.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::record::Record;

/// The result type every fallible public operation returns.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A failure reported by the `RouterOS` client.
///
/// The enum is `#[non_exhaustive]`: a later release may add a category, so a
/// caller that matches exhaustively must carry a wildcard arm.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Error {
    /// The configuration was rejected before any socket was opened.
    Config {
        /// What is wrong with the configuration. Never contains a credential.
        detail: String,
    },
    /// The command path, attributes, proplist, or query words were invalid.
    /// Nothing was sent.
    InvalidRequest {
        /// Which field was rejected, and why.
        detail: String,
    },
    /// The TLS settings cannot be materialized, for example because no root
    /// certificate could be loaded from the platform store.
    Tls {
        /// What could not be built.
        detail: String,
    },
    /// A dial, read, write, or peer-close failure.
    Connection {
        /// The failed operation, for example `"connect"`, `"command"`, or
        /// `"read"`.
        op: &'static str,
        /// The underlying failure.
        source: Arc<dyn StdError + Send + Sync>,
    },
    /// The operation ran on a client that [`crate::Client::close`] ended.
    Closed,
    /// A framing, reply-parsing, or desynchronization failure. The connection
    /// generation ends when this error comes from the reader.
    Protocol {
        /// What the protocol violation was.
        detail: String,
    },
    /// The router rejected the login exchange with a `!trap`.
    Login {
        /// The trap message, or `"login rejected"` when it carried none.
        message: String,
    },
    /// The router rejected a command with a `!trap`.
    Trap {
        /// The trap category, empty when the router sent none.
        category: String,
        /// The trap message.
        message: String,
        /// The attributes of the terminating `!done` reply.
        done: Record,
    },
    /// The router sent `!fatal`; the connection generation ended.
    Fatal {
        /// The reason the router reported.
        reason: String,
    },
    /// One phase of an operation did not finish within its deadline.
    Timeout {
        /// The phase that timed out, for example `"connect"` or `"command"`.
        phase: &'static str,
        /// The deadline that elapsed.
        duration: Duration,
    },
    /// A [`crate::Cancel`] token or [`crate::Stream::close`] ended the
    /// operation before the router answered.
    Cancelled,
    /// A decoder or stream-queue limit was exceeded.
    ResourceLimit {
        /// The limit that was exceeded, for example `"word"`, `"sentence"`,
        /// or `"stream records"`.
        scope: &'static str,
        /// The configured bound.
        limit: u64,
        /// The size that exceeded it.
        observed: u64,
    },
    /// [`crate::Client::reconnect`] displaced the operation, or work was
    /// submitted while a reconnect was in progress. The client never replays
    /// a command: the caller decides whether repeating a mutation is safe.
    Reconnect,
}

impl Error {
    /// Whether this error reports a client that is closed for good.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        matches!(self, Error::Closed)
    }

    /// Whether this error reports work displaced by a reconnect.
    #[must_use]
    pub fn is_reconnect(&self) -> bool {
        matches!(self, Error::Reconnect)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Config { detail } => write!(f, "routeros: invalid config: {detail}"),
            Error::InvalidRequest { detail } => {
                write!(f, "routeros: invalid request: {detail}")
            }
            Error::Tls { detail } => write!(f, "routeros: TLS: {detail}"),
            Error::Connection { op, source } => {
                write!(f, "routeros: {op}: {source}")
            }
            Error::Closed => f.write_str("routeros: client is closed"),
            Error::Protocol { detail } => write!(f, "routeros: protocol error: {detail}"),
            Error::Login { message } => write!(f, "routeros: login failed: {message}"),
            Error::Trap {
                category, message, ..
            } => {
                if category.is_empty() {
                    write!(f, "routeros: trap: {message}")
                } else {
                    write!(f, "routeros: trap ({category}): {message}")
                }
            }
            Error::Fatal { reason } => write!(f, "routeros: fatal: {reason}"),
            Error::Timeout { phase, duration } => {
                write!(f, "routeros: {phase} timed out after {duration:?}")
            }
            Error::Cancelled => f.write_str("routeros: cancelled"),
            Error::ResourceLimit {
                scope,
                limit,
                observed,
            } => write!(
                f,
                "routeros: {scope} exceeds limit {limit} (observed {observed})"
            ),
            Error::Reconnect => f.write_str("routeros: operation interrupted by reconnect"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::Connection { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        connection("io", error)
    }
}

/// Build a [`Error::Connection`] from any sendable standard error.
pub(crate) fn connection(op: &'static str, source: impl StdError + Send + Sync + 'static) -> Error {
    Error::Connection {
        op,
        source: Arc::new(source),
    }
}
