//! How an internal failure ends a connection generation, request, or stream.

use std::time::Duration;

use crate::error::{Error, connection};
use crate::record::Record;
use crate::wire::WireError;

/// What ended a connection generation, a command, or a stream. It is cloneable
/// so every pending operation can report the same cause.
#[derive(Clone, Debug)]
pub(crate) enum Fault {
    /// The client closed the connection locally.
    Closed,
    /// The client replaced this connection generation with a new one.
    Reconnected,
    /// The caller cancelled the operation.
    Cancelled,
    /// The operation exceeded its phase deadline.
    Timeout {
        phase: &'static str,
        duration: Duration,
    },
    /// The router's bytes did not form a valid protocol exchange.
    Protocol { detail: String },
    /// A configured queue limit was exceeded.
    Limit {
        scope: &'static str,
        limit: u64,
        observed: u64,
    },
    /// The router sent `!fatal`.
    Fatal { reason: String },
    /// The router rejected a request with `!trap`.
    Trap {
        category: String,
        message: String,
        done: Record,
    },
    /// A socket read or write failed; `op` names the internal operation.
    Io { op: &'static str, message: String },
}

impl Fault {
    pub(crate) fn from_wire(error: WireError) -> Self {
        match error {
            WireError::Protocol { detail } => Fault::Protocol { detail },
            WireError::Limit {
                scope,
                limit,
                observed,
            } => Fault::Limit {
                scope,
                limit,
                observed,
            },
        }
    }

    pub(crate) fn io(op: &'static str, error: &std::io::Error) -> Self {
        Fault::Io {
            op,
            message: error.to_string(),
        }
    }

    /// Turn the cause into the public error category. `op` names the caller's
    /// operation, so a connection failure is reported against the work that
    /// observed it.
    pub(crate) fn to_error(&self, op: &'static str) -> Error {
        match self {
            Fault::Closed => Error::Closed,
            Fault::Reconnected => Error::Reconnect,
            Fault::Cancelled => Error::Cancelled,
            Fault::Timeout { phase, duration } => Error::Timeout {
                phase,
                duration: *duration,
            },
            Fault::Protocol { detail } => Error::Protocol {
                detail: detail.clone(),
            },
            Fault::Limit {
                scope,
                limit,
                observed,
            } => Error::ResourceLimit {
                scope,
                limit: *limit,
                observed: *observed,
            },
            Fault::Fatal { reason } => Error::Fatal {
                reason: reason.clone(),
            },
            Fault::Trap {
                category,
                message,
                done,
            } => Error::Trap {
                category: category.clone(),
                message: message.clone(),
                done: done.clone(),
            },
            Fault::Io { op: io_op, message } => {
                connection(op, std::io::Error::other(format!("{io_op}: {message}")))
            }
        }
    }
}
