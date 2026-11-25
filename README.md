# ros-rs

A RouterOS native API client for Rust, over TCP and TLS.

One authenticated connection carries every command and stream. Replies are
routed by tag, so independent tasks can share one `Client`; each request
returns its own records or its own error. Long-running commands deliver `!re`
records through a bounded queue with real TCP backpressure. The crate ships no
command catalogue: every menu path, attribute, projection, and query word is
caller input, and every value stays a string, exactly as it travels on the wire.

Requires Rust 1.100 (edition 2024). TLS is compiled in by default.

## Install

```toml
[dependencies]
ros-rs = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

The examples use Tokio as the async runtime. `tokio` is not re-exported by the
crate; your binary supplies it.

## Connect

TLS is the default: port 8729, certificate-chain verification, and hostname
verification against `Config::host`. The one-message `/login` exchange requires
RouterOS 6.43 or later.

```rust
use ros_rs::{Config, Request, connect};

#[tokio::main]
async fn main() -> Result<(), ros_rs::Error> {
    let client = connect(Config::new(
        "router.example.com",
        "api",
        std::env::var("ROS_PASSWORD")
            .unwrap_or_default()
            .into_bytes(),
    ))
    .await?;

    let identity = client
        .command("/system/identity/print", &Request::new())
        .await?;
    println!("{:?}", identity.done.get("name"));

    client.close().await;
    Ok(())
}
```

A dial, TLS, or peer failure returns `Error::Connection`. A login `!trap`
returns `Error::Login`. An already-cancelled `Cancel` sends nothing.

## Commands

`command` returns `CommandResult { records, done }`. `records` holds every `!re`
reply in arrival order; `done` holds the attributes of the terminating `!done`.
A `!trap` becomes `Error::Trap { category, message, done }`, and the connection
stays usable.

```rust
use ros_rs::{Request, Result};

async fn add_address(client: &ros_rs::Client) -> Result<()> {
    let added = client
        .command(
            "/ip/address/add",
            &Request::new()
                .attribute("address", "10.0.0.1/24")
                .attribute("interface", "ether1"),
        )
        .await?;

    println!("{:?}", added.done.get("ret"));
    Ok(())
}
```

Attributes are sent in insertion order. `Request::proplist` sends one
`.proplist=` word, and `Request::raw_query` accepts a raw word that starts with
`?`.

## Queries

Build the RouterOS postfix query with `ros_rs::query`, or pass raw words when a
field or operator is not represented.

```rust
use ros_rs::Request;
use ros_rs::query::{absent, and, equal};

fn ethernet_request() -> Request {
    Request::new()
        .proplist(["name", "type"])
        .query(and(equal("type", "ether"), [absent("comment")]))
}
```

`equal`, `less_than`, `greater_than`, `exists`, `absent`, `not`, `and`, and `or`
produce query words; `words()` returns a fresh copy. The builder validates
neither names nor values.

## Streams

`stream` is for long-running commands such as `/interface/listen`. `recv`
returns `Ok(Some(record))` for an `!re`, `Ok(None)` after a normal `!done`, and
an error otherwise.

```rust
use ros_rs::{Request, Result};

async fn listen(client: &ros_rs::Client) -> Result<()> {
    let mut stream = client.stream("/interface/listen", &Request::new()).await?;
    while let Some(record) = stream.recv().await? {
        println!("{:?}", record.get("name"));
    }
    stream.close().await?;
    Ok(())
}
```

Each stream has a queue bounded by record count and payload bytes. When a
queue fills, or when the connection's total queued stream payload reaches its
budget, the reader stops reading from the socket until the consumer drains
below the low-water mark. A slow consumer therefore delays every command
multiplexed on the same connection; use a dedicated `Client` for a high-volume
listener. A record larger than the stream's byte cap fails that stream with
`Error::ResourceLimit`, and the rest of its replies are absorbed.

`Stream::close` asks the router to cancel the command, wakes a blocked `recv`,
and waits for the tagged `!done` or the cancel grace period. It is idempotent.
Dropping a `Stream` cancels it synchronously, so an error path does not leave a
paused reader behind.

## Cancellation and timeouts

`Cancel` is a cheap, cloneable token. `command_with` and `stream_with` accept
one through their options struct. Cancelling a command in flight sends
`/cancel =tag=<n>`, resolves the command with `Error::Cancelled`, and leaves
the connection usable. Cancelling while a stream is being submitted fails that
submission with `Error::Cancelled`; a cancellation that wins before the writer
starts the sentence means the command is never sent at all. To end a stream
that has already been returned, use `Stream::close`, or drop it.

```rust
use std::time::Duration;

use ros_rs::{CallOptions, Cancel, Client, Error, Request, Result};

async fn ping(client: &Client) -> Result<()> {
    let cancel = Cancel::new();
    let token = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        token.cancel();
    });

    match client
        .command_with(
            "/tool/ping",
            &Request::new(),
            &CallOptions::new().cancel(cancel),
        )
        .await
    {
        Err(Error::Cancelled) => {}
        Err(error) => return Err(error),
        Ok(_) => {}
    }
    Ok(())
}
```

`Config::command_timeout` bounds one command end to end; zero selects 10
seconds, and `CallOptions::timeout` overrides it for one call. On expiry the
client sends the same cancel and returns `Error::Timeout { phase: "command",
.. }`. `Config::cancel_grace` bounds how long a cancelled tag is remembered
before its late replies are absorbed as a zombie; zero selects 2 seconds.

## Per-call options

`command_with`, `stream_with`, `connect_with`, and `reconnect_with` take an
options struct instead of extra parameters. `CallOptions::timeout` overrides
`Config::command_timeout` for one call, `CallOptions::cancel` and
`StreamOptions::cancel` attach a `Cancel`, and `StreamOptions::records` and
`StreamOptions::bytes` override the queue bounds for one stream, which still
counts against the connection budget. A deadline or queue bound of zero, or a
byte bound above `max_buffered_stream_bytes`, is `Error::InvalidRequest`
before anything is sent.

```rust
use std::time::Duration;

use ros_rs::{CallOptions, Request};

async fn short_command(client: &ros_rs::Client) -> ros_rs::Result<()> {
    let result = client
        .command_with(
            "/interface/print",
            &Request::new(),
            &CallOptions::new().timeout(Duration::from_secs(2)),
        )
        .await?;
    println!("{}", result.records.len());
    Ok(())
}
```

## TLS

TLS is the default and needs no configuration: the certificate chain is
verified against the platform root store and the hostname in `Config::host` is
checked. Port 8729 is used when `config.port` is zero. `connect` returns
`Error::Tls` when the platform store cannot be loaded or a certificate cannot
be parsed.

A private certificate authority, or a complete `rustls::ClientConfig` for client
certificates or non-default ciphers, is supplied through `Tls` and assigned to
`Config::tls`:

```rust
use ros_rs::{Config, Tls};

let mut config = Config::new("router.example.com", "api", password);
config.tls = Tls::root_ca_pem(&std::fs::read("router-ca.pem")?)?;
```

`Tls::System` is the default.

Plaintext is an explicit opt-in and defaults to port 8728. The login and every
command then travel unencrypted; use it only on a management network you trust.

```rust
use ros_rs::Config;

fn plaintext_config() -> Config {
    let mut config = Config::new(
        "router.example.com",
        "api",
        std::env::var("ROS_PASSWORD")
            .unwrap_or_default()
            .into_bytes(),
    );
    config.plaintext = true;
    config
}
```

Building the crate with `default-features = false` removes all TLS crates. Such
a build is plaintext-only and rejects a configuration that does not set
`plaintext`.

## Lifecycle

`State` has six values: `connecting`, `logging in`, `ready`, `reconnecting`,
`closing`, and `closed`; `state()` returns the current one. `last_error()`
returns the terminal error of the current connection generation, if one was
recorded.

`reconnect(config)` replaces the connection generation: it closes the old
connection and installs a new one only after dial and login succeed. It does
not replay commands or resubscribe streams. Pending work and work issued while
the reconnect is in progress fail with `Error::Reconnect`. A failed reconnect
leaves the client closed but retryable.

`close()` is final and idempotent. Every operation after it fails with
`Error::Closed`.

`done()` resolves when the connection generation that was current when it was
called ends: a `close()`, a fault, or a `reconnect` that replaced it. A later
call awaits the next generation. Use it in a `select!` arm to stop unrelated
work when the connection dies; `last_error()` then names the cause.

## Errors

`Error` is `#[non_exhaustive]`; match the categories you handle and keep a
wildcard arm.

| Variant | Meaning |
| --- | --- |
| `Config`, `InvalidRequest`, `Tls` | rejected before or during connection setup |
| `Connection` | dial, read, write, or peer-close failure; `op` names the operation |
| `Closed` | the client was closed for good |
| `Protocol` | framing or routing failure; the generation ends |
| `Login`, `Trap`, `Fatal` | router rejection; `Trap` carries the `!done` attributes, `Fatal` ends the generation |
| `Timeout`, `Cancelled` | the operation exceeded its bound, or was cancelled |
| `ResourceLimit` | a codec, stream, or open-stream limit was exceeded |
| `Reconnect` | the operation was displaced by a reconnect |

Error messages never contain credentials. `Config`'s `Debug` implementation
prints `<redacted>` for the password, and the password buffer is zeroed when
the config is dropped. `Error::is_closed` and `Error::is_reconnect` are
convenience predicates.

## Limits

Every timeout and limit in the table takes its documented default when the
field is zero, except `Config::keepalive`: `None` selects the default
15-second TCP keepalive interval and `Some(Duration::ZERO)` turns keepalive
off. An out-of-range limit, or a `stream_max_bytes` above
`max_buffered_stream_bytes`, is rejected with `Error::Config` before any socket
is opened.

| Setting | Default |
| --- | ---: |
| `keepalive` | 15 s, on |
| `connect_timeout` | 10 s |
| `command_timeout` | 10 s |
| `cancel_grace` | 2 s |
| `max_word_bytes` | 32 MiB |
| `max_sentence_bytes` | 64 MiB |
| `max_words_per_sentence` | 4096 |
| `stream_max_records` | 1024 |
| `stream_max_bytes` | 8 MiB |
| `max_buffered_stream_bytes` | 64 MiB |
| `max_open_streams` | 16 |

`max_open_streams` counts pending and cancelling streams; exceeding it returns
`Error::ResourceLimit { scope: "open streams", .. }`. Codec limits are enforced
in both directions, so a hostile or broken peer cannot make the client allocate
an unbounded word or sentence.

## Scope

The client is a transport, not a command catalogue. It does not know RouterOS
menus or field names, does not reconnect automatically, does not replay a
command, and does not pool connections. Retry, backoff, and stream re-issue
belong to the application, which is the only layer that knows whether repeating
a mutation is safe.
