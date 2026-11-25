# Changelog

All notable changes to this crate are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-25

### Added

- `connect` and `connect_with`: one authenticated RouterOS API connection over
  TCP or TLS, with tag-routed multiplexed commands.
- `Client::command` and `Client::command_with`: one command, every `!re` record
  in arrival order, the terminating `!done` attributes, and `Error::Trap` with
  those attributes; the `_with` form takes a per-call deadline and cancellation
  token.
- `Client::stream` and `Client::stream_with`: a long-running command delivered
  through a bounded queue that pauses the reader when the caller falls behind;
  the `_with` form overrides the stream queue bounds and takes a cancellation
  token.
- `CallOptions` and `StreamOptions`, which carry a per-call deadline,
  cancellation token, or stream queue override in one struct.
- Cancellation through `Cancel`, `/cancel =tag=` on the wire, and zombie tags
  that absorb late replies instead of desynchronizing the connection.
- TCP keepalive, on by default with a 15-second idle interval; set
  `Config::keepalive` to `Some(Duration::ZERO)` to turn it off.
- `Client::reconnect`, which ends the old generation and never replays work.
- `Client::done`, which resolves when the current generation ends, so a caller
  can abandon its work in a `select!` instead of polling `state`.
- TLS by default, verifying against the platform root store, a supplied root
  CA, or a complete `rustls::ClientConfig`.
- `Query` expressions, `.proplist`, and raw query words as caller input.
