//! Dialling and the TLS handshake.

use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::error::{Error, Result, connection};

/// The single TLS type the crate talks about.
#[cfg(feature = "tls")]
pub(crate) type SecureStream = tokio_rustls::client::TlsStream<TcpStream>;

/// A connected byte stream.
pub(crate) enum IoStream {
    /// Plaintext TCP.
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    /// TLS over TCP.
    Tls(Box<SecureStream>),
}

impl IoStream {
    pub(crate) fn split(
        self,
    ) -> (
        tokio::io::ReadHalf<IoStream>,
        tokio::io::WriteHalf<IoStream>,
    ) {
        tokio::io::split(self)
    }
}

impl AsyncRead for IoStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            IoStream::Plain(stream) => std::pin::Pin::new(stream).poll_read(cx, buffer),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_read(cx, buffer),
        }
    }
}

impl AsyncWrite for IoStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            IoStream::Plain(stream) => std::pin::Pin::new(stream).poll_write(cx, buffer),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_write(cx, buffer),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            IoStream::Plain(stream) => std::pin::Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            IoStream::Plain(stream) => std::pin::Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            IoStream::Tls(stream) => std::pin::Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Unpin for IoStream {}

pub(crate) struct DialOptions {
    pub(crate) address: String,
    #[cfg(feature = "tls")]
    pub(crate) host: String,
    #[cfg(feature = "tls")]
    pub(crate) plaintext: bool,
    pub(crate) keepalive: Option<Duration>,
    pub(crate) connect_timeout: Duration,
    #[cfg(feature = "tls")]
    pub(crate) tls: crate::tls::Tls,
}

impl DialOptions {
    pub(crate) fn from_config(config: &crate::config::Config) -> Self {
        DialOptions {
            address: format!("{}:{}", config.host, config.port),
            #[cfg(feature = "tls")]
            host: config.host.clone(),
            #[cfg(feature = "tls")]
            plaintext: config.plaintext,
            keepalive: config.keepalive,
            connect_timeout: config.connect_timeout,
            #[cfg(feature = "tls")]
            tls: config.tls.clone(),
        }
    }
}

/// Dial the router and complete the TLS handshake when TLS is enabled.
pub(crate) async fn dial(options: &DialOptions) -> Result<(IoStream, Duration)> {
    let started = std::time::Instant::now();

    // DNS resolution runs on the blocking pool inside TcpStream::connect.
    let connect = TcpStream::connect(options.address.clone());
    let stream = tokio::time::timeout(options.connect_timeout, connect)
        .await
        .map_err(|_elapsed| Error::Timeout {
            phase: "connect",
            duration: options.connect_timeout,
        })?
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                Error::Timeout {
                    phase: "connect",
                    duration: options.connect_timeout,
                }
            } else {
                connection("connect", error)
            }
        })?;
    let _ = stream.set_nodelay(true);
    apply_keepalive(&stream, options.keepalive)?;

    #[cfg(feature = "tls")]
    {
        if !options.plaintext {
            let secure = tls_handshake(stream, options).await?;
            return Ok((IoStream::Tls(Box::new(secure)), started.elapsed()));
        }
    }
    Ok((IoStream::Plain(stream), started.elapsed()))
}

/// The idle interval used when [`crate::Config::keepalive`] is `None`.
const DEFAULT_KEEPALIVE_IDLE: Duration = Duration::from_secs(15);

/// Apply the configured TCP keepalive to a connected socket.
fn apply_keepalive(stream: &TcpStream, keepalive: Option<Duration>) -> Result<()> {
    let socket = SockRef::from(stream);
    let applied = match keepalive {
        Some(Duration::ZERO) => socket.set_keepalive(false),
        Some(idle) => socket.set_tcp_keepalive(&keepalive_params(idle)),
        None => socket.set_tcp_keepalive(&keepalive_params(DEFAULT_KEEPALIVE_IDLE)),
    };
    applied.map_err(|error| connection("keepalive", error))
}

/// Keepalive parameters with `idle` as both the idle time and, where the
/// platform exposes it, the probe interval.
fn keepalive_params(idle: Duration) -> TcpKeepalive {
    let params = TcpKeepalive::new().with_time(idle);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "visionos",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
        target_os = "cygwin",
        target_os = "nuttx",
        all(target_os = "wasi", not(target_env = "p1")),
    ))]
    let params = params.with_interval(idle);
    params
}

#[cfg(feature = "tls")]
async fn tls_handshake(stream: TcpStream, options: &DialOptions) -> Result<SecureStream> {
    use rustls::pki_types::ServerName;

    let config = options.tls.client_config()?;
    let server_name =
        ServerName::try_from(options.host.clone()).map_err(|error| Error::Config {
            detail: format!("host is not a valid TLS server name: {error}"),
        })?;
    let connector = tokio_rustls::TlsConnector::from(config);
    let handshake = connector.connect(server_name, stream);
    tokio::time::timeout(options.connect_timeout, handshake)
        .await
        .map_err(|_elapsed| Error::Timeout {
            phase: "TLS handshake",
            duration: options.connect_timeout,
        })?
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::TimedOut {
                Error::Timeout {
                    phase: "TLS handshake",
                    duration: options.connect_timeout,
                }
            } else {
                connection("TLS handshake", error)
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn a_plain_stream_can_be_shut_down() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(address))
            .await
            .unwrap()
            .unwrap();
        let mut stream = IoStream::Plain(client);
        stream.shutdown().await.unwrap();
        let (mut accepted, _) = listener.accept().await.unwrap();
        let mut buffer = [0u8; 1];
        let read = accepted.read(&mut buffer).await.unwrap();
        assert_eq!(read, 0);
    }

    #[tokio::test]
    async fn a_plain_stream_can_be_split() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address).await.unwrap();
        let (mut reader, mut writer) = IoStream::Plain(client).split();
        let (mut accepted, _) = listener.accept().await.unwrap();
        writer.write_all(b"ping").await.unwrap();
        let mut buffer = [0u8; 4];
        accepted.read_exact(&mut buffer).await.unwrap();
        assert_eq!(&buffer, b"ping");
        accepted.write_all(b"pong").await.unwrap();
        let mut reply = [0u8; 4];
        reader.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"pong");
    }

    /// A connected client stream and the local peer it dialled.
    async fn connect_pair() -> (TcpStream, TcpStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        (client, server)
    }

    #[tokio::test]
    async fn a_missing_keepalive_setting_enables_keepalive_with_a_15_second_idle() {
        let (client, _server) = connect_pair().await;
        apply_keepalive(&client, None).unwrap();
        let socket = SockRef::from(&client);
        assert!(socket.keepalive().unwrap());
        #[cfg(not(any(
            windows,
            target_os = "haiku",
            target_os = "openbsd",
            target_os = "vita"
        )))]
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(15)
        );
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "emscripten",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "windows",
            target_os = "cygwin",
            target_os = "nuttx",
            all(target_os = "wasi", not(target_env = "p1")),
        ))]
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            Duration::from_secs(15)
        );
    }

    #[tokio::test]
    async fn a_zero_keepalive_setting_disables_keepalive() {
        let (client, _server) = connect_pair().await;
        apply_keepalive(&client, Some(Duration::ZERO)).unwrap();
        assert!(!SockRef::from(&client).keepalive().unwrap());
    }

    #[tokio::test]
    async fn an_explicit_keepalive_setting_becomes_the_idle_interval() {
        let (client, _server) = connect_pair().await;
        apply_keepalive(&client, Some(Duration::from_secs(42))).unwrap();
        let socket = SockRef::from(&client);
        assert!(socket.keepalive().unwrap());
        #[cfg(not(any(
            windows,
            target_os = "haiku",
            target_os = "openbsd",
            target_os = "vita"
        )))]
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            Duration::from_secs(42)
        );
    }
}
