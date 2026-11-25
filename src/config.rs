//! Connection configuration and its defaults.

use std::fmt;
use std::time::Duration;

use crate::error::Error;

pub(crate) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const DEFAULT_CANCEL_GRACE: Duration = Duration::from_secs(2);
pub(crate) const DEFAULT_MAX_WORD_BYTES: usize = 32 << 20;
pub(crate) const DEFAULT_MAX_SENTENCE_BYTES: usize = 64 << 20;
pub(crate) const DEFAULT_MAX_WORDS_PER_SENTENCE: usize = 4096;
pub(crate) const DEFAULT_STREAM_MAX_RECORDS: usize = 1024;
pub(crate) const DEFAULT_STREAM_MAX_BYTES: usize = 8 << 20;
pub(crate) const DEFAULT_MAX_BUFFERED_STREAM_BYTES: usize = 64 << 20;
pub(crate) const DEFAULT_MAX_OPEN_STREAMS: usize = 16;

/// The API-over-TLS port used when `Config::port` is zero and TLS is enabled.
pub const DEFAULT_TLS_PORT: u16 = 8729;
/// The API port used when `Config::port` is zero and `Config::plaintext` is
/// set.
pub const DEFAULT_PLAINTEXT_PORT: u16 = 8728;

/// Everything [`crate::connect`] and [`crate::Client::reconnect`] use.
///
/// The default is usable once `host` is set: TLS is enabled, the port is
/// [`DEFAULT_TLS_PORT`], and every timeout and limit takes its documented
/// default. Plaintext is never the default and must be opted into; a plaintext
/// config defaults to [`DEFAULT_PLAINTEXT_PORT`].
///
/// The configuration is validated before a socket is opened, so an invalid
/// config never reaches the network. The password is never printed by the
/// `Debug` implementation and is zeroed when the config is dropped.
#[derive(Clone)]
pub struct Config {
    /// The router hostname or IP address, without a port. Required.
    pub host: String,
    /// The API port. Zero selects [`DEFAULT_TLS_PORT`], or
    /// [`DEFAULT_PLAINTEXT_PORT`] when `plaintext` is set.
    pub port: u16,
    /// The API user name.
    pub username: String,
    /// The API password. Held as bytes so the client does not build a second
    /// `String` copy.
    pub password: Vec<u8>,
    /// Send credentials without TLS. Never the default.
    pub plaintext: bool,
    /// TLS settings. Ignored when `plaintext` is set.
    #[cfg(feature = "tls")]
    pub tls: crate::tls::Tls,
    /// TCP keepalive idle interval. `None` selects 15 seconds;
    /// `Some(Duration::ZERO)` disables keepalive; any other value is the idle
    /// interval.
    pub keepalive: Option<Duration>,
    /// Bounds dialling, the TLS handshake, and login. Zero selects 10 seconds.
    pub connect_timeout: Duration,
    /// Bounds one command end to end. Zero selects 10 seconds.
    pub command_timeout: Duration,
    /// Bounds remote cancellation cleanup after a cancel. Zero selects 2
    /// seconds.
    pub cancel_grace: Duration,
    /// The largest accepted or sent word. Zero selects 32 MiB.
    pub max_word_bytes: usize,
    /// Bounds the total size of one sentence. Zero selects 64 MiB.
    pub max_sentence_bytes: usize,
    /// Bounds a sentence made of many small words. Zero selects 4096.
    pub max_words_per_sentence: usize,
    /// Bounds one stream's record count. Zero selects 1024.
    pub stream_max_records: usize,
    /// Bounds one stream's queued payload bytes. Zero selects 8 MiB.
    pub stream_max_bytes: usize,
    /// Bounds every queued stream record on the connection together. It must
    /// be at least `stream_max_bytes`. Zero selects 64 MiB.
    pub max_buffered_stream_bytes: usize,
    /// Bounds concurrent streams on the connection. Zero selects 16.
    pub max_open_streams: usize,
}

impl Config {
    /// Build a config for `host`/`username`/`password` with every other field
    /// at its default.
    pub fn new(
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<Vec<u8>>,
    ) -> Self {
        let defaults = Config::default();
        Config {
            host: host.into(),
            port: defaults.port,
            username: username.into(),
            password: password.into(),
            plaintext: defaults.plaintext,
            #[cfg(feature = "tls")]
            tls: defaults.tls.clone(),
            keepalive: defaults.keepalive,
            connect_timeout: defaults.connect_timeout,
            command_timeout: defaults.command_timeout,
            cancel_grace: defaults.cancel_grace,
            max_word_bytes: defaults.max_word_bytes,
            max_sentence_bytes: defaults.max_sentence_bytes,
            max_words_per_sentence: defaults.max_words_per_sentence,
            stream_max_records: defaults.stream_max_records,
            stream_max_bytes: defaults.stream_max_bytes,
            max_buffered_stream_bytes: defaults.max_buffered_stream_bytes,
            max_open_streams: defaults.max_open_streams,
        }
    }

    /// Apply defaults and validate every field, returning an independent copy.
    ///
    /// The receiver's password is not touched.
    pub(crate) fn normalized(&self) -> Result<Config, Error> {
        if self.host.is_empty() {
            return Err(Error::Config {
                detail: "host is required".to_owned(),
            });
        }
        let mut config = self.clone();
        if config.port == 0 {
            config.port = if config.plaintext {
                DEFAULT_PLAINTEXT_PORT
            } else {
                DEFAULT_TLS_PORT
            };
        }
        if config.connect_timeout.is_zero() {
            config.connect_timeout = DEFAULT_CONNECT_TIMEOUT;
        }
        if config.command_timeout.is_zero() {
            config.command_timeout = DEFAULT_COMMAND_TIMEOUT;
        }
        if config.cancel_grace.is_zero() {
            config.cancel_grace = DEFAULT_CANCEL_GRACE;
        }

        config.max_word_bytes = limit_or(
            "max word bytes",
            config.max_word_bytes,
            DEFAULT_MAX_WORD_BYTES,
        )?;
        config.max_sentence_bytes = limit_or(
            "max sentence bytes",
            config.max_sentence_bytes,
            DEFAULT_MAX_SENTENCE_BYTES,
        )?;
        config.max_words_per_sentence = limit_or(
            "max words per sentence",
            config.max_words_per_sentence,
            DEFAULT_MAX_WORDS_PER_SENTENCE,
        )?;
        config.stream_max_records = limit_or(
            "stream max records",
            config.stream_max_records,
            DEFAULT_STREAM_MAX_RECORDS,
        )?;
        config.stream_max_bytes = limit_or(
            "stream max bytes",
            config.stream_max_bytes,
            DEFAULT_STREAM_MAX_BYTES,
        )?;
        config.max_buffered_stream_bytes = limit_or(
            "max buffered stream bytes",
            config.max_buffered_stream_bytes,
            DEFAULT_MAX_BUFFERED_STREAM_BYTES,
        )?;
        config.max_open_streams = limit_or(
            "max open streams",
            config.max_open_streams,
            DEFAULT_MAX_OPEN_STREAMS,
        )?;

        if config.max_word_bytes > u32::MAX as usize {
            return Err(Error::Config {
                detail: "max word bytes must not exceed 4294967295".to_owned(),
            });
        }
        if config.stream_max_bytes > config.max_buffered_stream_bytes {
            return Err(Error::Config {
                detail: format!(
                    "stream max bytes {} exceeds max buffered stream bytes {}",
                    config.stream_max_bytes, config.max_buffered_stream_bytes
                ),
            });
        }
        #[cfg(not(feature = "tls"))]
        if !config.plaintext {
            return Err(Error::Config {
                detail: "TLS support is disabled at compile time; enable the tls feature or set plaintext = true"
                    .to_owned(),
            });
        }
        Ok(config)
    }

    /// Erase the password copy this config owns.
    pub(crate) fn forget_password(&mut self) {
        self.password.fill(0);
        self.password.clear();
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: String::new(),
            port: 0,
            username: String::new(),
            password: Vec::new(),
            plaintext: false,
            #[cfg(feature = "tls")]
            tls: crate::tls::Tls::System,
            keepalive: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            command_timeout: DEFAULT_COMMAND_TIMEOUT,
            cancel_grace: DEFAULT_CANCEL_GRACE,
            max_word_bytes: DEFAULT_MAX_WORD_BYTES,
            max_sentence_bytes: DEFAULT_MAX_SENTENCE_BYTES,
            max_words_per_sentence: DEFAULT_MAX_WORDS_PER_SENTENCE,
            stream_max_records: DEFAULT_STREAM_MAX_RECORDS,
            stream_max_bytes: DEFAULT_STREAM_MAX_BYTES,
            max_buffered_stream_bytes: DEFAULT_MAX_BUFFERED_STREAM_BYTES,
            max_open_streams: DEFAULT_MAX_OPEN_STREAMS,
        }
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("plaintext", &self.plaintext)
            .field("keepalive", &self.keepalive)
            .field("connect_timeout", &self.connect_timeout)
            .field("command_timeout", &self.command_timeout)
            .field("cancel_grace", &self.cancel_grace)
            .field("max_word_bytes", &self.max_word_bytes)
            .field("max_sentence_bytes", &self.max_sentence_bytes)
            .field("max_words_per_sentence", &self.max_words_per_sentence)
            .field("stream_max_records", &self.stream_max_records)
            .field("stream_max_bytes", &self.stream_max_bytes)
            .field("max_buffered_stream_bytes", &self.max_buffered_stream_bytes)
            .field("max_open_streams", &self.max_open_streams)
            .finish_non_exhaustive()
    }
}

impl Drop for Config {
    fn drop(&mut self) {
        self.password.fill(0);
    }
}

fn limit_or(name: &str, value: usize, fallback: usize) -> Result<usize, Error> {
    if value == 0 {
        Ok(fallback)
    } else if value > isize::MAX as usize {
        Err(Error::Config {
            detail: format!("{name} {value} is too large"),
        })
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "tls")]
    fn applies_tls_defaults() {
        let config = Config::new("router.example.com", "api", b"secret".to_vec())
            .normalized()
            .unwrap();
        assert_eq!(config.port, DEFAULT_TLS_PORT);
        assert_eq!(config.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(config.command_timeout, DEFAULT_COMMAND_TIMEOUT);
        assert_eq!(config.cancel_grace, DEFAULT_CANCEL_GRACE);
        assert_eq!(config.max_word_bytes, DEFAULT_MAX_WORD_BYTES);
        assert_eq!(config.max_sentence_bytes, DEFAULT_MAX_SENTENCE_BYTES);
        assert_eq!(
            config.max_words_per_sentence,
            DEFAULT_MAX_WORDS_PER_SENTENCE
        );
        assert_eq!(config.stream_max_records, DEFAULT_STREAM_MAX_RECORDS);
        assert_eq!(config.stream_max_bytes, DEFAULT_STREAM_MAX_BYTES);
        assert_eq!(
            config.max_buffered_stream_bytes,
            DEFAULT_MAX_BUFFERED_STREAM_BYTES
        );
        assert_eq!(config.max_open_streams, DEFAULT_MAX_OPEN_STREAMS);
    }

    #[test]
    fn plaintext_defaults_to_the_plaintext_port() {
        let mut config = Config::new("router", "api", b"secret".to_vec());
        config.plaintext = true;
        let config = config.normalized().unwrap();
        assert_eq!(config.port, DEFAULT_PLAINTEXT_PORT);
    }

    #[test]
    fn keeps_explicit_values() {
        let mut config = Config::new("router", "api", b"secret".to_vec());
        config.plaintext = true;
        config.port = 9999;
        config.connect_timeout = Duration::from_secs(1);
        config.max_word_bytes = 128;
        let normalized = config.normalized().unwrap();
        assert_eq!(normalized.port, 9999);
        assert_eq!(normalized.connect_timeout, Duration::from_secs(1));
        assert_eq!(normalized.max_word_bytes, 128);
    }

    #[test]
    fn keepalive_is_unset_by_default_and_survives_normalizing() {
        let mut config = Config::new("router", "api", b"secret".to_vec());
        config.plaintext = true;
        assert_eq!(config.keepalive, None);
        config.keepalive = Some(Duration::from_secs(42));
        let normalized = config.normalized().unwrap();
        assert_eq!(normalized.keepalive, Some(Duration::from_secs(42)));
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn rejects_a_tls_config_when_tls_is_compiled_out() {
        let error = Config::new("router", "api", b"secret".to_vec())
            .normalized()
            .unwrap_err();
        assert!(matches!(error, Error::Config { .. }));
    }

    #[test]
    fn rejects_a_missing_host() {
        let error = Config::default().normalized().unwrap_err();
        assert!(matches!(error, Error::Config { .. }));
    }

    #[test]
    fn rejects_stream_bytes_above_the_connection_budget() {
        let mut config = Config::new("router", "api", b"secret".to_vec());
        config.stream_max_bytes = 64;
        config.max_buffered_stream_bytes = 32;
        let error = config.normalized().unwrap_err();
        assert!(matches!(error, Error::Config { .. }));
    }

    #[test]
    fn debug_never_prints_the_password() {
        let config = Config::new("router", "api", b"hunter2".to_vec());
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("hunter2"));
        assert!(rendered.contains("<redacted>"));
    }
}
