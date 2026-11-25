//! The one-message `/login` exchange, read directly off the socket before the
//! reader task exists.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::config::Config;
use crate::error::{Error, Result, connection};
use crate::wire::{Decoder, Encoder, Kind, Limits, parse_reply};

/// Authenticate `reader`/`writer` and hand them back ready for the I/O tasks.
///
/// One linear exchange, kept in one place so the deadline, the cancellation,
/// and every reply kind are visible together.
#[allow(clippy::too_many_lines)]
pub(crate) async fn login(
    reader: &mut tokio::io::ReadHalf<crate::socket::IoStream>,
    writer: &mut tokio::io::WriteHalf<crate::socket::IoStream>,
    config: &Config,
    cancel: &crate::cancel::Cancel,
) -> Result<()> {
    let encoder = Encoder::new(Limits::new(
        config.max_word_bytes,
        config.max_sentence_bytes,
        config.max_words_per_sentence,
    ));
    let password = String::from_utf8_lossy(&config.password).into_owned();
    let sentence = encoder
        .sentence(&[
            "/login".to_owned(),
            format!("=name={}", config.username),
            format!("=password={password}"),
        ])
        .map_err(|error| match error {
            crate::wire::WireError::Protocol { detail } => Error::Protocol { detail },
            crate::wire::WireError::Limit {
                scope,
                limit,
                observed,
            } => Error::ResourceLimit {
                scope,
                limit,
                observed,
            },
        })?;

    let work = async {
        writer.write_all(&sentence).await?;
        writer.flush().await?;
        let mut decoder = Decoder::new(Limits::new(
            config.max_word_bytes,
            config.max_sentence_bytes,
            config.max_words_per_sentence,
        ));
        let mut buffer = vec![0u8; 32 << 10];
        loop {
            tokio::select! {
                () = cancel.cancelled() => return Err(Error::Cancelled),
                read = reader.read(&mut buffer) => {
                    match read {
                        Ok(0) => {
                            return Err(connection(
                                "login",
                                std::io::Error::new(
                                    std::io::ErrorKind::UnexpectedEof,
                                    "connection closed during login",
                                ),
                            ));
                        }
                        Ok(n) => {
                            let sentences = decoder.feed(&buffer[..n]).map_err(|error| match error {
                                crate::wire::WireError::Protocol { detail } => Error::Protocol { detail },
                                crate::wire::WireError::Limit { scope, limit, observed } => Error::ResourceLimit {
                                    scope,
                                    limit,
                                    observed,
                                },
                            })?;
                            if let Some(words) = sentences.into_iter().next() {
                                let reply = parse_reply(&words).map_err(|error| match error {
                                    crate::wire::WireError::Protocol { detail } => Error::Protocol { detail },
                                    crate::wire::WireError::Limit { scope, limit, observed } => Error::ResourceLimit {
                                        scope,
                                        limit,
                                        observed,
                                    },
                                })?;
                                match reply.kind {
                                    Kind::Done => return Ok(()),
                                    Kind::Trap => {
                                        let message = reply
                                            .attributes
                                            .get("message")
                                            .cloned()
                                            .unwrap_or_else(|| "login rejected".to_owned());
                                        return Err(Error::Login { message });
                                    }
                                    Kind::Fatal => {
                                        return Err(Error::Fatal {
                                            reason: reply.fatal_reason,
                                        });
                                    }
                                    other => {
                                        return Err(Error::Protocol {
                                            detail: format!(
                                                "unexpected {} during login",
                                                other.word()
                                            ),
                                        });
                                    }
                                }
                            }
                        }
                        Err(error) => return Err(connection("login", error)),
                    }
                }
            }
        }
    };

    let timeout = tokio::time::timeout(config.connect_timeout, work).await;
    drop(password);
    match timeout {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(_elapsed) => Err(Error::Timeout {
            phase: "login",
            duration: config.connect_timeout,
        }),
    }
}
