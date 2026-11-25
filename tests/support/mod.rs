//! An in-process `RouterOS` API server for the integration tests.
//!
//! The framing is implemented here independently of `src/wire`: the tests
//! pin the wire format a second time, so a change that only agrees with
//! itself cannot pass.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use ros_rs::Config;

/// How long a test may wait for one client step.
pub const STEP_TIMEOUT: Duration = Duration::from_secs(5);

/// One length prefix in the `RouterOS` encoding.
///
/// Every branch masks the value to the bits its prefix carries.
#[allow(clippy::cast_possible_truncation)]
fn encode_length(out: &mut Vec<u8>, n: u32) {
    if n < 0x80 {
        out.push(n as u8);
    } else if n < 0x4000 {
        out.push((n >> 8) as u8 | 0x80);
        out.push(n as u8);
    } else if n < 0x20_0000 {
        out.push((n >> 16) as u8 | 0xc0);
        out.push((n >> 8) as u8);
        out.push(n as u8);
    } else {
        out.push((n >> 24) as u8 | 0xe0);
        out.push((n >> 16) as u8);
        out.push((n >> 8) as u8);
        out.push(n as u8);
    }
}

/// Encode one sentence: length-prefixed words and a zero-length terminator.
pub fn encode_sentence(words: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for word in words {
        let length = u32::try_from(word.len()).expect("test words fit a length prefix");
        encode_length(&mut out, length);
        out.extend_from_slice(word.as_bytes());
    }
    out.push(0);
    out
}

/// Read one length prefix. `None` means the buffer holds only part of one.
fn peek_length(bytes: &[u8]) -> Option<(u32, usize)> {
    let first = *bytes.first()?;
    if first < 0x80 {
        return Some((u32::from(first), 1));
    }
    if first & 0xc0 == 0x80 {
        if bytes.len() < 2 {
            return None;
        }
        return Some(((u32::from(first & 0x3f) << 8) | u32::from(bytes[1]), 2));
    }
    if first & 0xe0 == 0xc0 {
        if bytes.len() < 3 {
            return None;
        }
        return Some((
            (u32::from(first & 0x1f) << 16) | (u32::from(bytes[1]) << 8) | u32::from(bytes[2]),
            3,
        ));
    }
    if bytes.len() < 4 {
        return None;
    }
    Some((
        (u32::from(first & 0x0f) << 24)
            | (u32::from(bytes[1]) << 16)
            | (u32::from(bytes[2]) << 8)
            | u32::from(bytes[3]),
        4,
    ))
}

/// Take one complete sentence off `buffer`, if it holds one.
fn take_sentence(buffer: &mut Vec<u8>) -> Option<Vec<String>> {
    let mut cursor = 0;
    let mut words = Vec::new();
    loop {
        let (length, size) = peek_length(&buffer[cursor..])?;
        cursor += size;
        if length == 0 {
            buffer.drain(..cursor);
            return Some(words);
        }
        let end = cursor + length as usize;
        if buffer.len() < end {
            return None;
        }
        words.push(String::from_utf8_lossy(&buffer[cursor..end]).into_owned());
        cursor = end;
    }
}

/// The server end of one connection.
pub struct Session {
    stream: TcpStream,
    buffer: Vec<u8>,
}

impl Session {
    /// The next sentence the client sent, or `None` at end of stream.
    pub async fn next(&mut self) -> Option<Vec<String>> {
        loop {
            if let Some(words) = take_sentence(&mut self.buffer) {
                return Some(words);
            }
            let mut chunk = [0u8; 8192];
            match self.stream.read(&mut chunk).await {
                Ok(0) | Err(_) => return None,
                Ok(n) => self.buffer.extend_from_slice(&chunk[..n]),
            }
        }
    }

    /// Send one reply sentence.
    pub async fn send(&mut self, words: &[&str]) {
        self.stream
            .write_all(&encode_sentence(words))
            .await
            .expect("server write");
        self.stream.flush().await.expect("server flush");
    }

    /// Answer the login exchange.
    pub async fn login(&mut self) {
        let words = self.next().await.expect("login sentence");
        assert_eq!(words.first().map(String::as_str), Some("/login"));
        self.send(&["!done"]).await;
    }

    /// Consume everything until the client closes the connection.
    pub async fn drain(&mut self) {
        while self.next().await.is_some() {}
    }
}

/// The `.tag=` value of a sentence, with the prefix removed.
pub fn tag_of(words: &[String]) -> String {
    words
        .iter()
        .find_map(|word| word.strip_prefix(".tag="))
        .unwrap_or_else(|| panic!("sentence {words:?} has no tag"))
        .to_owned()
}

/// Bind a server on a loopback port and run `script` for its one connection.
pub async fn serve<F>(script: F) -> String
where
    F: FnOnce(Session) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        let _ = stream.set_nodelay(true);
        script(Session {
            stream,
            buffer: Vec::new(),
        })
        .await;
    });
    address.to_string()
}

/// A plaintext config pointing at `address`, with one second timeouts.
pub fn config(address: &str) -> Config {
    let port: u16 = address
        .rsplit(':')
        .next()
        .expect("address has a port")
        .parse()
        .expect("valid port");
    let mut config = Config::new("127.0.0.1", "alice", "secret");
    config.port = port;
    config.plaintext = true;
    config.command_timeout = Duration::from_secs(1);
    config
}

/// Await `work`, failing the test rather than hanging it.
pub async fn within<T>(work: impl Future<Output = T>) -> T {
    tokio::time::timeout(STEP_TIMEOUT, work)
        .await
        .expect("test step timed out")
}
