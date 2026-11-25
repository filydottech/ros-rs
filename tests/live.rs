//! Live checks against a router, run explicitly:
//!
//! ```text
//! ROS_HOST=127.0.0.1 ROS_PORT=8703 ROS_PASSWORD= \
//!     cargo test --test live -- --ignored --test-threads=1
//! ```
//!
//! Every test is `#[ignore]`d so an ordinary `cargo test` neither runs them nor
//! counts them as a pass. Credentials come only from `ROS_*` environment
//! variables; nothing here prints a password or a certificate.

use std::time::Duration;

use ros_rs::{Cancel, Config, Error, Request, StreamOptions, connect};

/// Build a config from `ROS_*`. Panics when the required variables are absent,
/// which only happens if the ignored tests were requested without them.
fn config() -> Config {
    let host = dotenv("ROS_HOST").expect("ROS_HOST must name the router");
    let mut config = Config::new(
        host,
        dotenv("ROS_USERNAME").unwrap_or_else(|| "admin".to_owned()),
        dotenv("ROS_PASSWORD").unwrap_or_default(),
    );
    config.plaintext = dotenv("ROS_PLAINTEXT").unwrap_or_else(|| "true".to_owned()) == "true";
    if let Some(port) = dotenv("ROS_PORT") {
        config.port = port.parse().expect("ROS_PORT must be a port number");
    }
    if let Some(pem) = dotenv("ROS_CA_PEM") {
        apply_ca(&mut config, &pem);
    }
    config.command_timeout = Duration::from_secs(10);
    config.connect_timeout = Duration::from_secs(10);
    config
}

#[cfg(feature = "tls")]
fn apply_ca(config: &mut Config, pem: &str) {
    let bytes = std::fs::read(pem).expect("ROS_CA_PEM must be readable");
    config.tls = ros_rs::Tls::root_ca_pem(&bytes).expect("ROS_CA_PEM must hold a certificate");
}

#[cfg(not(feature = "tls"))]
fn apply_ca(_config: &mut Config, _pem: &str) {
    panic!("ROS_CA_PEM requires the tls feature");
}

fn dotenv(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

#[tokio::test]
#[ignore = "requires ROS_HOST; run with --ignored"]
async fn logs_in_and_prints_the_identity() {
    let client = connect(config()).await.expect("connect");
    let result = client
        .command("/system/identity/print", &Request::new())
        .await
        .expect("identity");
    let row = result.records.first().expect("one identity record");
    let name = row.get("name").expect("identity name");
    assert_ne!(name, "");
}

#[tokio::test]
#[ignore = "requires ROS_HOST; run with --ignored"]
async fn reports_the_router_version() {
    let client = connect(config()).await.expect("connect");
    let result = client
        .command("/system/resource/print", &Request::new())
        .await
        .expect("resource");
    let row = result.records.first().expect("one resource record");
    let version = row.get("version").expect("version");
    assert!(
        version.starts_with("6.") || version.starts_with("7."),
        "unexpected version {version}"
    );
    assert!(row.contains_key("uptime"));
}

#[tokio::test]
#[ignore = "requires ROS_HOST; run with --ignored"]
async fn a_proplist_and_query_round_trip() {
    let client = connect(config()).await.expect("connect");
    let request = Request::new()
        .proplist(["name"])
        .query(ros_rs::query::absent("comment"));
    let result = client
        .command("/interface/print", &request)
        .await
        .expect("interfaces");
    for record in &result.records {
        assert!(record.contains_key("name"));
        assert!(!record.contains_key("comment") || record["comment"].is_empty());
    }
}

#[tokio::test]
#[ignore = "requires ROS_HOST; run with --ignored"]
async fn an_unknown_path_is_a_trap_and_the_connection_survives() {
    let client = connect(config()).await.expect("connect");
    let error = client
        .command("/no/such/command", &Request::new())
        .await
        .expect_err("an unknown path must trap");
    match error {
        Error::Trap { message, .. } => assert_ne!(message, ""),
        other => panic!("expected a trap, got {other:?}"),
    }
    client
        .command("/system/identity/print", &Request::new())
        .await
        .expect("usable after a trap");
}

#[tokio::test]
#[ignore = "requires ROS_HOST; run with --ignored"]
async fn a_stream_cancels_cleanly_and_releases_its_tag() {
    let client = connect(config()).await.expect("connect");
    let cancel = Cancel::new();
    let mut stream = client
        .stream_with(
            "/interface/listen",
            &Request::new(),
            &StreamOptions::new().cancel(cancel.clone()),
        )
        .await
        .expect("stream");

    cancel.cancel();
    let error = stream.recv().await.expect_err("a cancelled stream ends");
    assert!(matches!(error, Error::Cancelled), "got {error:?}");
    drop(stream);

    // The router's untagged `!done` acknowledgement must be absorbed, so the
    // next command still resolves instead of desynchronizing the connection.
    tokio::time::sleep(Duration::from_millis(200)).await;
    client
        .command("/system/identity/print", &Request::new())
        .await
        .expect("usable after a stream cancellation");
}

#[tokio::test]
#[ignore = "requires ROS_HOST; run with --ignored"]
async fn a_reconnect_replaces_the_generation() {
    let client = connect(config()).await.expect("connect");
    client
        .command("/system/identity/print", &Request::new())
        .await
        .expect("before reconnect");
    client.reconnect(config()).await.expect("reconnect");
    client
        .command("/system/identity/print", &Request::new())
        .await
        .expect("after reconnect");
    client.close().await;
}
