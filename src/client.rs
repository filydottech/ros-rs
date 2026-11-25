//! The public client: lifecycle, commands, streams, and reconnect.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::cancel::Cancel;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::record::{CommandResult, Record};
use crate::request::Request;
use crate::transport::{Conn, Fault};

/// The observable lifecycle state of a [`Client`].
///
/// It is informational: no method forces a state apart from the documented
/// lifecycle methods.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum State {
    /// While a dial is in progress.
    Connecting,
    /// While the login exchange is in progress.
    LoggingIn,
    /// While the connection accepts commands.
    Ready,
    /// While [`Client::reconnect`] replaces the connection.
    Reconnecting,
    /// While [`Client::close`] releases the connection.
    Closing,
    /// After the connection generation ended.
    Closed,
}

impl State {
    /// The lifecycle word.
    #[must_use]
    pub fn word(self) -> &'static str {
        match self {
            State::Connecting => "connecting",
            State::LoggingIn => "logging in",
            State::Ready => "ready",
            State::Reconnecting => "reconnecting",
            State::Closing => "closing",
            State::Closed => "closed",
        }
    }
}

/// One `RouterOS` API connection.
///
/// The client is cheap to share across tasks (`Arc<Client>`), and every
/// method takes `&self`. All work goes through the current connection
/// generation; [`Client::reconnect`] replaces it explicitly and never replays
/// a command or resubscribes a stream.
pub struct Client {
    inner: RwLock<ClientInner>,
}

struct ClientInner {
    conn: Option<Arc<Conn>>,
    state: State,
    closed: bool,
    reconnecting: bool,
    terminal: Option<Error>,
}

/// Dial, log in, and return a ready client.
///
/// TLS is the default: port [`crate::DEFAULT_TLS_PORT`], the certificate
/// chain is verified, and the hostname in [`Config::host`] is checked. Set
/// [`Config::plaintext`] to opt into an unencrypted connection, which then
/// defaults to [`crate::DEFAULT_PLAINTEXT_PORT`].
///
/// A failed dial returns [`Error::Connection`]; a login `!trap` returns
/// [`Error::Login`]. An already-cancelled [`Cancel`] sends nothing.
/// # Errors
///
/// Returns [`Error::Config`] for a rejected configuration,
/// [`Error::Connection`] for a failed dial or read, and [`Error::Login`] when
/// the router rejects the credentials.
pub async fn connect(config: Config) -> Result<Client> {
    connect_cancellable(config, &Cancel::new()).await
}

/// Like [`connect`], with a caller-supplied cancellation token.
/// # Errors
///
/// The categories are those of [`connect`], plus [`Error::Cancelled`] when the
/// token is already cancelled.
pub async fn connect_cancellable(config: Config, cancel: &Cancel) -> Result<Client> {
    let config = config.normalized()?;
    let dial_options = crate::socket::DialOptions::from_config(&config);
    let conn = crate::transport::open(dial_options, &config, cancel).await?;
    let generation = Arc::new(conn);
    let mut config = config;
    config.forget_password();
    drop(config);
    Ok(Client {
        inner: RwLock::new(ClientInner {
            conn: Some(generation),
            state: State::Ready,
            closed: false,
            reconnecting: false,
            terminal: None,
        }),
    })
}

impl Client {
    /// Run one command and return every `!re` record in arrival order with
    /// the final `!done` attributes.
    /// # Errors
    ///
    /// [`Error::InvalidRequest`] before anything is sent, [`Error::Trap`] when
    /// the router rejects the command, [`Error::Timeout`] or
    /// [`Error::Cancelled`] when the command does not finish in time, and the
    /// transport categories [`Error::Closed`], [`Error::Protocol`], and
    /// [`Error::Connection`].
    pub async fn command(&self, path: &str, request: &Request) -> Result<CommandResult> {
        self.command_cancellable(path, request, &Cancel::new())
            .await
    }

    /// Like [`Client::command`], but cancelling in flight asks the router to
    /// cancel the command and resolves with [`Error::Cancelled`].
    /// # Errors
    ///
    /// The categories are those of [`Client::command`].
    pub async fn command_cancellable(
        &self,
        path: &str,
        request: &Request,
        cancel: &Cancel,
    ) -> Result<CommandResult> {
        let words = request.sentence_words(path)?;
        let conn = self.conn_or_fail().await?;
        conn.command(&words, cancel, "command").await
    }

    /// Receive the `!re` records of a long-running command through a bounded
    /// queue with TCP backpressure.
    /// # Errors
    ///
    /// [`Error::InvalidRequest`] before anything is sent,
    /// [`Error::ResourceLimit`] when the connection already has its maximum
    /// number of streams, and the transport categories [`Error::Closed`],
    /// [`Error::Protocol`], and [`Error::Connection`]. A `!trap` terminates the
    /// returned stream rather than this call.
    pub async fn stream(&self, path: &str, request: &Request) -> Result<crate::Stream> {
        self.stream_cancellable(path, request, &Cancel::new()).await
    }

    /// Like [`Client::stream`], with a caller-supplied cancellation token.
    /// # Errors
    ///
    /// The categories are those of [`Client::stream`], plus
    /// [`Error::Cancelled`] when the token is already cancelled.
    pub async fn stream_cancellable(
        &self,
        path: &str,
        request: &Request,
        cancel: &Cancel,
    ) -> Result<crate::Stream> {
        let words = request.sentence_words(path)?;
        let conn = self.conn_or_fail().await?;
        let (pending_tag, queue) = conn.stream(&words, cancel, "stream").await?;
        Ok(crate::Stream {
            inner: Arc::new(queue),
            conn,
            pending_tag,
            cancel: cancel.clone(),
            closed: false,
        })
    }

    /// Close the client. It is final and idempotent: later calls return
    /// immediately and every operation fails with [`Error::Closed`].
    pub async fn close(&self) {
        let conn = {
            let mut inner = self.inner.write().await;
            if inner.closed {
                return;
            }
            inner.closed = true;
            inner.state = State::Closing;
            inner.conn.take()
        };
        if let Some(conn) = conn {
            conn.close().await;
        }
        let mut inner = self.inner.write().await;
        inner.state = State::Closed;
        inner.terminal = None;
    }

    /// Replace the connection using `config`.
    ///
    /// Pending work and work submitted while the reconnect is in progress
    /// fail with [`Error::Reconnect`]. A failed reconnect leaves the client
    /// closed but retryable.
    /// # Errors
    ///
    /// [`Error::Closed`] on a closed client, [`Error::Reconnect`] for a
    /// concurrent reconnect, and the dial and login categories of
    /// [`connect`].
    pub async fn reconnect(&self, config: Config) -> Result<()> {
        self.reconnect_cancellable(config, &Cancel::new()).await
    }

    /// Like [`Client::reconnect`], with a caller-supplied cancellation token.
    /// # Errors
    ///
    /// The categories are those of [`Client::reconnect`], plus
    /// [`Error::Cancelled`] when the token is already cancelled.
    pub async fn reconnect_cancellable(&self, config: Config, cancel: &Cancel) -> Result<()> {
        let old = {
            let mut inner = self.inner.write().await;
            if inner.closed {
                return Err(Error::Closed);
            }
            if inner.reconnecting {
                return Err(Error::Reconnect);
            }
            let config = config.normalized()?;
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            inner.reconnecting = true;
            inner.state = State::Reconnecting;
            (inner.conn.take(), config)
        };
        let (old, config) = old;

        // End the old generation before dialling so its pending work fails
        // with a reconnect error and its socket is released.
        if let Some(old) = old {
            old.shutdown(&Fault::Reconnected).await;
        }

        let dial_options = crate::socket::DialOptions::from_config(&config);
        let opened = crate::transport::open(dial_options, &config, cancel).await;

        let mut inner = self.inner.write().await;
        inner.reconnecting = false;
        if inner.closed {
            drop(inner);
            if let Ok(conn) = opened {
                conn.close().await;
            }
            return Err(Error::Closed);
        }
        match opened {
            Ok(conn) => {
                let mut config = config;
                config.forget_password();
                drop(config);
                inner.conn = Some(Arc::new(conn));
                inner.state = State::Ready;
                inner.terminal = None;
                Ok(())
            }
            Err(error) => {
                inner.state = State::Closed;
                inner.terminal = None;
                Err(error)
            }
        }
    }

    /// The current lifecycle state.
    pub async fn state(&self) -> State {
        let inner = self.inner.read().await;
        if inner.state == State::Ready
            && !inner.reconnecting
            && !inner.closed
            && inner
                .conn
                .as_ref()
                .is_some_and(|conn| conn.terminal().is_some())
        {
            return State::Closed;
        }
        inner.state
    }

    /// The terminal error of the current generation, when one has been
    /// recorded.
    pub async fn last_error(&self) -> Option<Error> {
        let inner = self.inner.read().await;
        if let Some(error) = &inner.terminal {
            return Some(error.clone());
        }
        inner
            .conn
            .as_ref()
            .and_then(|conn| conn.terminal())
            .map(|fault| fault.to_error("connection"))
    }

    async fn conn_or_fail(&self) -> Result<Arc<Conn>> {
        let inner = self.inner.read().await;
        if inner.closed {
            return Err(Error::Closed);
        }
        if inner.reconnecting {
            return Err(Error::Reconnect);
        }
        if let Some(terminal) = &inner.terminal {
            return Err(terminal.clone());
        }
        inner.conn.clone().ok_or(Error::Closed)
    }
}

/// The receiving end of [`Client::stream`].
pub struct Stream {
    inner: Arc<crate::transport::Queue>,
    conn: Arc<Conn>,
    pending_tag: String,
    cancel: Cancel,
    closed: bool,
}

impl Stream {
    /// Receive the next record, or `None` for a normal `!done`.
    /// # Errors
    ///
    /// [`Error::Cancelled`] after [`Stream::close`] or a cancelled token,
    /// [`Error::Trap`] when the router rejects the command, and the transport
    /// categories [`Error::Closed`], [`Error::Reconnect`],
    /// [`Error::ResourceLimit`], [`Error::Protocol`], and
    /// [`Error::Connection`].
    pub async fn recv(&mut self) -> Result<Option<Record>> {
        if self.closed {
            return self
                .inner
                .recv()
                .await
                .map_err(|fault| fault.to_error("recv"));
        }
        let outcome = tokio::select! {
            result = self.inner.recv() => Some(result),
            () = self.cancel.cancelled() => None,
        };
        match outcome {
            Some(Ok(Some(record))) => {
                self.conn.note_drained(&self.pending_tag);
                Ok(Some(record))
            }
            Some(Ok(None)) => Ok(None),
            Some(Err(fault)) => Err(fault.to_error("recv")),
            None => {
                self.closed = true;
                self.conn
                    .request_cancel(&self.pending_tag, Fault::Cancelled);
                Err(Error::Cancelled)
            }
        }
    }

    /// Cancel the command on the router and wait for its `!done` or the
    /// cancel grace period. Idempotent; wakes a waiting [`Stream::recv`].
    /// # Errors
    ///
    /// This method finishes with `Ok(())` once cancellation cleanup ends, even
    /// when the router never answered.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let waiter = self.conn.cancel_tag(&self.pending_tag, Fault::Cancelled);
        let _ = waiter.await;
        Ok(())
    }
}

/// A stream is identified by its tag; the queue and connection state are
/// intentionally opaque.
impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("tag", &self.pending_tag)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

/// A dropped stream is cancelled on the router, so a listener that was never
/// closed cannot leave the connection paused behind a full queue. This is a
/// deliberate improvement over cancellation that waits for the caller.
impl Drop for Stream {
    fn drop(&mut self) {
        if !self.closed {
            self.closed = true;
            self.conn
                .request_cancel(&self.pending_tag, Fault::Cancelled);
        }
    }
}
