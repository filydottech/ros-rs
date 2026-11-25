//! Dialling and the TLS handshake.

use std::time::Duration;

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

    #[cfg(feature = "tls")]
    {
        if !options.plaintext {
            let secure = tls_handshake(stream, options).await?;
            return Ok((IoStream::Tls(Box::new(secure)), started.elapsed()));
        }
    }
    Ok((IoStream::Plain(stream), started.elapsed()))
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
}
