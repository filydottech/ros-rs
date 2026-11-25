//! Command behaviour: multiplexing, reply routing, traps, cancellation,
//! timeouts, and connection failures.

mod support;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;

use ros_rs::{Cancel, Error, Request, connect};
use support::{config, serve, tag_of, within};

#[tokio::test]
async fn a_command_returns_records_in_wire_order_with_done_attributes() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            tx.send(words.clone()).ok();
            let tag = tag_of(&words);
            let tagged = format!(".tag={tag}");
            session.send(&["!re", "=name=ether1", &tagged]).await;
            session.send(&["!empty", &tagged]).await;
            session.send(&["!re", "=name=ether2", &tagged]).await;
            session.send(&["!done", "=ret=*1", &tagged]).await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let result = within(client.command("/interface/print", &Request::new()))
        .await
        .expect("command");

    let words = transcript.recv().await.expect("command sentence");
    assert_eq!(words[0], "/interface/print");
    assert_eq!(tag_of(&words), "1");

    assert_eq!(result.records.len(), 2);
    assert_eq!(result.records[0]["name"], "ether1");
    assert_eq!(result.records[1]["name"], "ether2");
    assert_eq!(result.done["ret"], "*1");
}

#[tokio::test]
async fn a_trap_keeps_the_done_attributes_and_the_connection() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            tx.send(words.clone()).ok();
            let tag = tag_of(&words);
            let tagged = format!(".tag={tag}");
            session
                .send(&["!trap", "=category=0", "=message=no such item", &tagged])
                .await;
            session.send(&["!done", "=ret=*2", &tagged]).await;
            let next = session.next().await.expect("second command");
            tx.send(next.clone()).ok();
            session
                .send(&["!done", &format!(".tag={}", tag_of(&next))])
                .await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let error = within(client.command("/interface/remove", &Request::new()))
        .await
        .expect_err("a trap must fail the command");

    match error {
        Error::Trap {
            category,
            message,
            done,
        } => {
            assert_eq!(category, "0");
            assert_eq!(message, "no such item");
            assert_eq!(done["ret"], "*2");
        }
        other => panic!("expected a trap, got {other:?}"),
    }

    // A trap resolves the request, so the tag is retired and a later command
    // still works.
    within(client.command("/system/identity/print", &Request::new()))
        .await
        .expect("the connection stays usable after a trap");
    assert_eq!(
        transcript.recv().await.expect("second sentence")[0],
        "/interface/remove"
    );
    assert_eq!(
        transcript.recv().await.expect("third sentence")[0],
        "/system/identity/print"
    );
}

#[tokio::test]
async fn cancelling_sends_one_cancel_and_leaves_the_connection_usable() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            tx.send(words.clone()).ok();
            let tag = tag_of(&words);
            let cancel = session.next().await.expect("cancel");
            tx.send(cancel.clone()).ok();
            session.send(&["!done"]).await;
            session.send(&["!done", &format!(".tag={tag}")]).await;
            let next = session.next().await.expect("second command");
            tx.send(next.clone()).ok();
            session
                .send(&["!done", &format!(".tag={}", tag_of(&next))])
                .await;
            session.drain().await;
        })
    })
    .await;

    let client = Arc::new(connect(config(&address)).await.expect("connect"));
    let cancel = Cancel::new();
    let running = tokio::spawn({
        let client = Arc::clone(&client);
        let cancel = cancel.clone();
        async move {
            client
                .command_cancellable("/tool/ping", &Request::new(), &cancel)
                .await
        }
    });

    let command = transcript.recv().await.expect("command sentence");
    assert_eq!(command[0], "/tool/ping");
    let tag = tag_of(&command);

    cancel.cancel();
    let error = within(running)
        .await
        .expect("task")
        .expect_err("must be cancelled");
    assert!(matches!(error, Error::Cancelled), "got {error:?}");

    let cancel_sentence = transcript.recv().await.expect("cancel sentence");
    assert_eq!(
        cancel_sentence,
        vec!["/cancel".to_owned(), format!("=tag={tag}")]
    );

    within(client.command("/system/resource/print", &Request::new()))
        .await
        .expect("the connection stays usable after a cancellation");
}

#[tokio::test]
async fn a_timeout_sends_one_cancel_and_never_retries() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            tx.send(words.clone()).ok();
            let tag = tag_of(&words);
            let cancel = session.next().await.expect("cancel");
            tx.send(cancel.clone()).ok();
            session.send(&["!done"]).await;
            session.send(&["!done", &format!(".tag={tag}")]).await;
            let next = session.next().await.expect("second command");
            tx.send(next.clone()).ok();
            session
                .send(&["!done", &format!(".tag={}", tag_of(&next))])
                .await;
            session.drain().await;
        })
    })
    .await;

    let mut settings = config(&address);
    settings.command_timeout = Duration::from_millis(100);
    let client = connect(settings).await.expect("connect");

    let error = within(client.command("/ip/address/add", &Request::new()))
        .await
        .expect_err("the command must time out");
    match error {
        Error::Timeout { phase, duration } => {
            assert_eq!(phase, "command");
            assert_eq!(duration, Duration::from_millis(100));
        }
        other => panic!("expected a timeout, got {other:?}"),
    }

    within(client.command("/system/resource/print", &Request::new()))
        .await
        .expect("the connection stays usable after a timeout");

    let seen: Vec<String> = {
        let mut names = Vec::new();
        while let Ok(words) = transcript.try_recv() {
            names.push(words[0].clone());
        }
        names
    };
    assert_eq!(
        seen,
        vec![
            "/ip/address/add".to_owned(),
            "/cancel".to_owned(),
            "/system/resource/print".to_owned()
        ],
        "a timed-out command is cancelled once and never replayed"
    );
}

#[tokio::test]
async fn an_invalid_request_never_reaches_the_wire() {
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

    let client = connect(config(&address)).await.expect("connect");
    let error = within(client.command("", &Request::new()))
        .await
        .expect_err("an empty path must be rejected");
    assert!(
        matches!(error, Error::InvalidRequest { .. }),
        "got {error:?}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        transcript.try_recv().is_err(),
        "an invalid request must not be written"
    );
}

#[tokio::test]
async fn a_peer_close_fails_the_command() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let _ = tag_of(&words);
            // Return without answering: the session, and its socket, drop here.
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let error = within(client.command("/interface/print", &Request::new()))
        .await
        .expect_err("a closed peer must fail the command");
    assert!(matches!(error, Error::Connection { .. }), "got {error:?}");
}

#[tokio::test]
async fn a_reply_for_an_unknown_tag_is_a_protocol_error() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let _ = tag_of(&words);
            session.send(&["!done", ".tag=999"]).await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let error = within(client.command("/interface/print", &Request::new()))
        .await
        .expect_err("an unknown tag must fail the command");
    match error {
        Error::Protocol { detail } => assert!(detail.contains("999"), "got {detail:?}"),
        other => panic!("expected a protocol error, got {other:?}"),
    }
}
