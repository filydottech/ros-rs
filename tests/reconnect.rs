//! Reconnect semantics: the old generation ends, pending work fails, and the
//! new generation is usable.

mod support;

use std::sync::Arc;

use tokio::sync::mpsc;

use ros_rs::{Error, Request, connect};
use support::{config, serve, tag_of, within};

#[tokio::test]
async fn reconnect_fails_pending_work_and_replaces_the_generation() {
    let (tx, mut first_seen) = mpsc::unbounded_channel::<Vec<String>>();
    let first = serve(move |mut session| {
        Box::pin(async move {
            session.login().await;
            if let Some(words) = session.next().await {
                tx.send(words).ok();
            }
            session.drain().await;
        })
    })
    .await;

    let second = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            if let Some(words) = session.next().await {
                let tagged = format!(".tag={}", tag_of(&words));
                session.send(&["!done", &tagged]).await;
            }
            session.drain().await;
        })
    })
    .await;

    let client = Arc::new(connect(config(&first)).await.expect("connect"));
    let running = tokio::spawn({
        let client = Arc::clone(&client);
        async move { client.command("/tool/ping", &Request::new()).await }
    });

    first_seen
        .recv()
        .await
        .expect("the pending command reaches the first connection");

    within(client.reconnect(config(&second)))
        .await
        .expect("reconnect");

    let error = within(running)
        .await
        .expect("task")
        .expect_err("pending work must not survive a reconnect");
    assert!(matches!(error, Error::Reconnect), "got {error:?}");

    within(client.command("/system/identity/print", &Request::new()))
        .await
        .expect("the new generation answers");
}

#[tokio::test]
async fn work_submitted_after_close_fails_with_closed() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    client.close().await;
    client.close().await;

    let error = within(client.command("/system/identity/print", &Request::new()))
        .await
        .expect_err("a closed client refuses commands");
    assert!(matches!(error, Error::Closed), "got {error:?}");
}
