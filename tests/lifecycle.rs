//! Lifecycle: `done()` resolves when the connection generation ends, and
//! `last_error()` names the cause.

mod support;

use std::time::Duration;

use ros_rs::{Error, connect};
use support::{config, serve, within};

#[tokio::test]
async fn done_resolves_when_the_peer_closes_the_connection() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            // Return without answering: the socket closes here.
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");
    within(client.done()).await;

    let error = client
        .last_error()
        .await
        .expect("a closed peer is an error");
    assert!(matches!(error, Error::Connection { .. }), "got {error:?}");
}

#[tokio::test]
async fn done_resolves_on_close_and_not_before() {
    let address = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&address)).await.expect("connect");

    let waiting = client.done();
    tokio::pin!(waiting);
    tokio::select! {
        () = &mut waiting => panic!("done resolved while the connection was alive"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }

    client.close().await;
    within(waiting).await;
    assert!(
        client.last_error().await.is_none(),
        "a close is not a fault"
    );
}

#[tokio::test]
async fn done_resolves_when_reconnect_replaces_the_generation() {
    let first = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            session.drain().await;
        })
    })
    .await;
    let second = serve(|mut session| {
        Box::pin(async move {
            session.login().await;
            session.drain().await;
        })
    })
    .await;

    let client = connect(config(&first)).await.expect("connect");

    let waiting = client.done();
    tokio::pin!(waiting);
    tokio::select! {
        () = &mut waiting => panic!("done resolved before the reconnect"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }

    client.reconnect(config(&second)).await.expect("reconnect");
    within(waiting).await;

    // The replacement generation is alive, so a fresh call waits again.
    let again = client.done();
    tokio::pin!(again);
    tokio::select! {
        () = &mut again => panic!("done resolved on a live generation"),
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
}
