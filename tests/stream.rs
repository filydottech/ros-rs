//! Stream behaviour: ordered delivery, bounded queues, cancellation, and
//! the connection staying usable after a stream ends badly.

mod support;

use std::time::Duration;

use tokio::sync::mpsc;

use ros_rs::{Error, Request, connect};
use support::{config, serve, tag_of, within};

#[tokio::test]
async fn a_stream_yields_records_then_a_normal_end() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let tag = tag_of(&words);
            let tagged = format!(".tag={tag}");
            session.send(&["!re", "=name=ether1", &tagged]).await;
            session.send(&["!re", "=name=ether2", &tagged]).await;
            session.send(&["!done", &tagged]).await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let mut stream = within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect("stream");

    let first = within(stream.recv())
        .await
        .expect("first")
        .expect("a record");
    assert_eq!(first["name"], "ether1");
    let second = within(stream.recv())
        .await
        .expect("second")
        .expect("a record");
    assert_eq!(second["name"], "ether2");
    assert!(within(stream.recv()).await.expect("end").is_none());
}

#[tokio::test]
async fn closing_a_stream_cancels_it_once_and_the_connection_survives() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
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

    let client = connect(config(&address)).await.expect("connect");
    let mut stream = within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect("stream");
    within(stream.close()).await.expect("close");
    within(stream.close())
        .await
        .expect("a second close is harmless");

    let cancel = transcript.recv().await.expect("cancel sentence");
    assert_eq!(cancel, vec!["/cancel".to_owned(), format!("=tag={}", "1")]);

    let error = within(stream.recv())
        .await
        .expect_err("a closed stream has no records");
    assert!(matches!(error, Error::Cancelled), "got {error:?}");

    within(client.command("/system/identity/print", &Request::new()))
        .await
        .expect("the connection stays usable after a stream close");
}

#[tokio::test]
async fn dropping_a_stream_cancels_it_on_the_router() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let tag = tag_of(&words);
            if let Some(cancel) = session.next().await {
                tx.send(cancel.clone()).ok();
                session.send(&["!done"]).await;
                session.send(&["!done", &format!(".tag={tag}")]).await;
            }
            if let Some(next) = session.next().await {
                session
                    .send(&["!done", &format!(".tag={}", tag_of(&next))])
                    .await;
            }
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let stream = within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect("stream");
    drop(stream);

    let cancel = transcript.recv().await.expect("a dropped stream cancels");
    assert_eq!(cancel[0], "/cancel");

    within(client.command("/system/identity/print", &Request::new()))
        .await
        .expect("the connection stays usable after a dropped stream");
}

#[tokio::test]
async fn an_oversized_record_fails_the_stream_not_the_connection() {
    let (tx, mut transcript) = mpsc::unbounded_channel::<Vec<String>>();
    let address = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let tag = tag_of(&words);
            let tagged = format!(".tag={tag}");
            session.send(&["!re", "=v=1234", &tagged]).await;
            let cancel = session.next().await.expect("cancel");
            tx.send(cancel.clone()).ok();
            session.send(&["!done"]).await;
            session.send(&["!done", &tagged]).await;
            let next = session.next().await.expect("second command");
            session
                .send(&["!done", &format!(".tag={}", tag_of(&next))])
                .await;
            session.drain().await;
        })
    })
    .await;

    let mut settings = config(&address);
    settings.stream_max_bytes = 2;
    let client = connect(settings).await.expect("connect");
    let mut stream = within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect("stream");

    let error = within(stream.recv())
        .await
        .expect_err("an oversized record must fail the stream");
    match error {
        Error::ResourceLimit {
            scope,
            limit,
            observed,
        } => {
            assert_eq!(scope, "stream bytes");
            assert_eq!(limit, 2);
            assert_eq!(observed, 5);
        }
        other => panic!("expected a resource limit, got {other:?}"),
    }

    let cancel = transcript.recv().await.expect("cancel sentence");
    assert_eq!(cancel[0], "/cancel");

    within(client.command("/system/identity/print", &Request::new()))
        .await
        .expect("the connection stays usable after an oversized record");
}

#[tokio::test]
async fn a_trap_ends_the_stream_with_the_trap_error() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            let words = session.next().await.expect("command");
            let tagged = format!(".tag={}", tag_of(&words));
            session
                .send(&["!trap", "=category=0", "=message=no such command", &tagged])
                .await;
            session.send(&["!done", "=ret=*9", &tagged]).await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    let mut stream = within(client.stream("/nope/listen", &Request::new()))
        .await
        .expect("stream");

    let error = within(stream.recv())
        .await
        .expect_err("a trap must end the stream");
    match error {
        Error::Trap { message, done, .. } => {
            assert_eq!(message, "no such command");
            assert_eq!(done["ret"], "*9");
        }
        other => panic!("expected a trap, got {other:?}"),
    }
}

#[tokio::test]
async fn the_open_stream_limit_is_enforced() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            session.drain().await;
        })
    })
    .await;

    let mut settings = config(&address);
    settings.max_open_streams = 1;
    let client = connect(settings).await.expect("connect");

    let first = within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect("first stream");
    let error = within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect_err("the second stream must be refused");
    match error {
        Error::ResourceLimit { scope, limit, .. } => {
            assert_eq!(scope, "open streams");
            assert_eq!(limit, 1);
        }
        other => panic!("expected an open-stream limit, got {other:?}"),
    }
    drop(first);

    tokio::time::sleep(Duration::from_millis(50)).await;
    within(client.stream("/interface/listen", &Request::new()))
        .await
        .expect("dropping the first stream frees the slot");
}
