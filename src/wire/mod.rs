//! Wire encoding, incremental decoding, and reply parsing.
//!
//! This layer knows the `RouterOS` sentence format and nothing about sockets,
//! timeouts, tags, or the client lifecycle. It is reached by the transport
//! layer only.

mod decoder;
mod encoder;
mod length;
mod limits;
mod reply;

pub(crate) use decoder::Decoder;
pub(crate) use encoder::Encoder;
pub(crate) use limits::Limits;
pub(crate) use reply::{Kind, Reply, parse_reply};

use std::error::Error as StdError;
use std::fmt;

/// A failure inside the codec. The transport maps it to a public
/// [`crate::Error`] category.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum WireError {
    /// The bytes cannot be framed or parsed.
    Protocol { detail: String },
    /// A configured limit was exceeded.
    Limit {
        scope: &'static str,
        limit: u64,
        observed: u64,
    },
}

impl WireError {
    pub(crate) fn protocol(detail: impl Into<String>) -> Self {
        WireError::Protocol {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Protocol { detail } => write!(f, "wire: {detail}"),
            WireError::Limit {
                scope,
                limit,
                observed,
            } => write!(
                f,
                "wire: {scope} exceeds limit {limit} (observed {observed})"
            ),
        }
    }
}

impl StdError for WireError {}
