# Changelog

All notable changes to this crate are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-25

### Added

- `connect` and `connect_cancellable`: one authenticated RouterOS API
  connection over TCP or TLS, with tag-routed multiplexed commands.
- `Client::command`: one command, every `!re` record in arrival order, the
  terminating `!done` attributes, and `Error::Trap` with those attributes.
- `Client::stream`: a long-running command delivered through a bounded queue
  that pauses the reader when the caller falls behind.
- Cancellation through `Cancel`, `/cancel =tag=` on the wire, and zombie tags
  that absorb late replies instead of desynchronizing the connection.
- `Client::reconnect`, which ends the old generation and never replays work.
- `Client::done`, which resolves when the current generation ends, so a caller
  can abandon its work in a `select!` instead of polling `state`.
- TLS by default, verifying against the platform root store, a supplied root
  CA, or a complete `rustls::ClientConfig`.
- `Query` expressions, `.proplist`, and raw query words as caller input.
