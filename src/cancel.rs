//! Cooperative cancellation.
//!
//! A `Cancel` token is shared with one operation, or with a whole
//! [`crate::Client`] generation. Cancelling it resolves the waiting operation
//! with [`Error::Cancelled`](crate::Error::Cancelled); the client asks the
//! router to cancel the command with `/cancel` and leaves the connection
//! usable.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// A cheap, cloneable cancellation token.
#[derive(Clone, Debug, Default)]
pub struct Cancel {
    inner: Arc<CancelInner>,
}

#[derive(Debug)]
struct CancelInner {
    flag: AtomicBool,
    notify: Notify,
}

impl Cancel {
    /// A token that has not been cancelled.
    #[must_use]
    pub fn new() -> Self {
        Cancel {
            inner: Arc::new(CancelInner {
                flag: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// Cancel every operation waiting on this token.
    pub fn cancel(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Whether the token has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// Complete when the token is cancelled (or immediately, if it already
    /// is).
    pub async fn cancelled(&self) {
        loop {
            let notification = self.inner.notify.notified();
            tokio::pin!(notification);
            notification.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notification.await;
        }
    }
}

impl Default for CancelInner {
    fn default() -> Self {
        CancelInner {
            flag: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_uncancelled_token_never_fires() {
        let cancel = Cancel::new();
        assert!(!cancel.is_cancelled());
        let fired =
            tokio::time::timeout(std::time::Duration::from_millis(50), cancel.cancelled()).await;
        assert!(fired.is_err());
    }

    #[tokio::test]
    async fn cancelling_wakes_the_waiter() {
        let cancel = Cancel::new();
        let waiter = {
            let cancel = cancel.clone();
            tokio::spawn(async move { cancel.cancelled().await })
        };
        cancel.cancel();
        waiter.await.unwrap();
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn a_late_waiter_observes_the_cancellation() {
        let cancel = Cancel::new();
        cancel.cancel();
        cancel.cancelled().await;
    }

    #[tokio::test]
    async fn cancelling_twice_is_harmless() {
        let cancel = Cancel::new();
        cancel.cancel();
        cancel.cancel();
        assert!(cancel.is_cancelled());
    }
}
