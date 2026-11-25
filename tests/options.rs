//! Per-call and per-stream options: deadline overrides, cancellation tokens,
//! and the bounds that are rejected before anything reaches the wire.

mod support;

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "tls")]
use tokio::net::TcpListener;
use tokio::sync::mpsc;
#[cfg(feature = "tls")]
use tokio::sync::oneshot;

#[cfg(feature = "tls")]
use ros_rs::Config;
use ros_rs::{CallOptions, Cancel, Error, Request, StreamOptions, connect, connect_with};
use support::{config, serve, tag_of, within};

#[tokio::test]
async fn a_per_call_timeout_sends_one_cancel_and_never_retries() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            tx.send(words.clone()).ok();
            let tag = tag_of(&words);
            let cancel = session.next().await.expect("cancel");
            tx.send(cancel).ok();
            session.send(&["!done"]).await;
            session.send(&["!done", &format!(".tag={tag}")]).await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let error = within(client.command_with(
        "/ip/address/add",
        &Request::new(),
        &CallOptions::new().timeout(Duration::from_millis(50)),
    ))
    .await
    .expect_err("the command must time out");

    match error {
        Error::Timeout { phase, duration } => {
            assert_eq!(phase, "command");
            assert_eq!(duration, Duration::from_millis(50));
        }
        other => panic!("expected a timeout, got {other:?}"),
    }

    // Let the cancel reach the server and the server's acknowledgements flow.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut seen = Vec::new();
    while let Ok(words) = transcript.try_recv() {
        seen.push(words[0].clone());
    }
    assert_eq!(
        seen,
        vec!["/ip/address/add".to_owned(), "/cancel".to_owned()],
        "a timed-out command is cancelled once and never replayed"
    );
}

#[tokio::test]
async fn a_per_call_timeout_bounds_a_blocked_submission() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            std::future::pending::<()>().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let request = Request::new().attribute("comment", "x".repeat(16 << 20));
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        client.command_with(
            "/ip/address/add",
            &request,
            &CallOptions::new().timeout(Duration::from_millis(100)),
        ),
    )
    .await
    .expect("the per-call deadline must bound submission");
    let Err(error) = result else {
        panic!("the peer never completes the command");
    };

    match error {
        Error::Timeout { phase, duration } => {
            assert_eq!(phase, "command");
            assert_eq!(duration, Duration::from_millis(100));
        }
        other => panic!("expected a timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn a_per_call_connection_timeout_overrides_the_configured_timeout() {
    let address = serve(|mut session| {
        Box::pin(async move {
            let _ = session.next().await.expect("login");
            session.drain().await;
        })
    })
    .await;

    let mut settings = config(&address);
    settings.connect_timeout = Duration::from_millis(20);
    let Err(error) = connect_with(
        settings,
        &CallOptions::new().timeout(Duration::from_millis(100)),
    )
    .await
    else {
        panic!("the server never completes login");
    };

    match error {
        Error::Timeout { duration, .. } => {
            assert_eq!(duration, Duration::from_millis(100));
        }
        other => panic!("expected a timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn a_per_call_reconnect_timeout_overrides_the_configured_timeout() {
    let first = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            session.drain().await;
        })
    })
    .await;
    let second = serve(|mut session| {
        Box::pin(async move {
            let _ = session.next().await.expect("login");
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&first)).await.expect("connect");
    let mut settings = config(&second);
    settings.connect_timeout = Duration::from_millis(20);
    let error = client
        .reconnect_with(
            settings,
            &CallOptions::new().timeout(Duration::from_millis(100)),
        )
        .await
        .expect_err("the call option must replace the configured deadline");

    match error {
        Error::Timeout { duration, .. } => {
            assert_eq!(duration, Duration::from_millis(100));
        }
        other => panic!("expected a timeout, got {other:?}"),
    }
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn a_per_call_cancel_ends_a_stalled_tls_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("local address");
    let (accepted_tx, accepted_rx) = oneshot::channel();
    tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.expect("accept");
        accepted_tx.send(()).expect("tell the test");
        std::future::pending::<()>().await;
    });

    let mut settings = Config::new("127.0.0.1", "alice", "secret");
    settings.port = address.port();
    settings.connect_timeout = Duration::from_secs(1);
    let cancel = Cancel::new();
    let token = cancel.clone();
    let connecting =
        tokio::spawn(
            async move { connect_with(settings, &CallOptions::new().cancel(token)).await },
        );

    accepted_rx.await.expect("the client reaches TCP");
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_millis(200), connecting)
        .await
        .expect("cancellation must interrupt the handshake")
        .expect("connection task");
    let Err(error) = result else {
        panic!("the server never completes a TLS handshake");
    };
    assert!(matches!(error, Error::Cancelled), "got {error:?}");
}

#[tokio::test]
async fn a_per_call_cancel_token_ends_the_command() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            tx.send(words.clone()).ok();
            let tag = tag_of(&words);
            let cancel = session.next().await.expect("cancel");
            tx.send(cancel).ok();
            session.send(&["!done"]).await;
            session.send(&["!done", &format!(".tag={tag}")]).await;
            session.drain().await;
        })
    })
    .await;

    let client = Arc::new(connect(config(&address)).await.expect("connect"));
    let cancel = Cancel::new();
    let options = CallOptions::new().cancel(cancel.clone());
    let running = tokio::spawn({
        let client = Arc::clone(&client);
        async move {
            client
                .command_with("/tool/ping", &Request::new(), &options)
                .await
        }
    });

    let command = transcript.recv().await.expect("command sentence");
    assert_eq!(command[0], "/tool/ping");
    cancel.cancel();

    let error = within(running)
        .await
        .expect("task")
        .expect_err("must be cancelled");
    assert!(matches!(error, Error::Cancelled), "got {error:?}");

    let cancel_sentence = transcript.recv().await.expect("cancel sentence");
    assert_eq!(
        cancel_sentence,
        vec!["/cancel".to_owned(), format!("=tag={}", tag_of(&command))]
    );
}

#[tokio::test]
async fn a_small_record_bound_throttles_but_never_truncates() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let tagged = format!(".tag={}", tag_of(&words));
            for name in ["ether1", "ether2", "ether3"] {
                session
                    .send(&["!re", &format!("=name={name}"), &tagged])
                    .await;
            }
            session.send(&["!done", &tagged]).await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let mut stream = within(client.stream_with(
        "/interface/listen",
        &Request::new(),
        &StreamOptions::new().records(1),
    ))
    .await
    .expect("stream");

    let mut names = Vec::new();
    while let Some(record) = within(stream.recv()).await.expect("recv") {
        names.push(record["name"].clone());
    }
    assert_eq!(
        names,
        vec![
            "ether1".to_owned(),
            "ether2".to_owned(),
            "ether3".to_owned()
        ],
        "a small record bound pauses the reader without dropping records"
    );
}

#[tokio::test]
async fn option_bounds_are_rejected_before_anything_reaches_the_wire() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            if let Some(words) = session.next().await {
                tx.send(words).ok();
            }
            session.drain().await;
        })
    })
    .await;

    let mut settings = config(&address);
    settings.stream_max_bytes = 32;
    settings.max_buffered_stream_bytes = 64;
    let client = connect(settings).await.expect("connect");

    let zero_timeout = client
        .command_with(
            "/interface/print",
            &Request::new(),
            &CallOptions::new().timeout(Duration::ZERO),
        )
        .await
        .expect_err("a zero timeout must be rejected");
    assert!(
        matches!(zero_timeout, Error::InvalidRequest { .. }),
        "got {zero_timeout:?}"
    );

    let zero_records = client
        .stream_with(
            "/interface/listen",
            &Request::new(),
            &StreamOptions::new().records(0),
        )
        .await
        .expect_err("a zero record bound must be rejected");
    assert!(
        matches!(zero_records, Error::InvalidRequest { .. }),
        "got {zero_records:?}"
    );

    let zero_bytes = client
        .stream_with(
            "/interface/listen",
            &Request::new(),
            &StreamOptions::new().bytes(0),
        )
        .await
        .expect_err("a zero byte bound must be rejected");
    assert!(
        matches!(zero_bytes, Error::InvalidRequest { .. }),
        "got {zero_bytes:?}"
    );

    let oversized = client
        .stream_with(
            "/interface/listen",
            &Request::new(),
            &StreamOptions::new().bytes(65),
        )
        .await
        .expect_err("a byte bound above the connection budget must be rejected");
    assert!(
        matches!(oversized, Error::InvalidRequest { .. }),
        "got {oversized:?}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        transcript.try_recv().is_err(),
        "a rejected option must not be written"
    );
}
