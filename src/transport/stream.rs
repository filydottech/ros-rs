//! The bounded record queue one stream consumes.
//!
//! The queue is shared between the connection's reader task (which fills it)
//! and the caller (which drains it through `Stream::recv`). When it is full
//! of records or bytes, the reader task stops consuming the socket until the
//! queue drains, which is the TCP backpressure a slow listener relies on.
//!
//! Wakeups use a `watch` version counter rather than a bare notification. A
//! receiver is subscribed before the queue is inspected, so a push or close
//! that lands between the inspection and the await is still observed.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

use crate::record::Record;
use crate::wire::Reply;

use super::fault::Fault;

/// One record, with its wire size for the byte budget.
#[derive(Clone)]
struct Entry {
    record: Record,
    bytes: usize,
}

/// The receiving side of one stream.
#[derive(Clone)]
pub(crate) struct Queue {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    records: VecDeque<Entry>,
    bytes: usize,
    max_records: usize,
    max_bytes: usize,
    closed: bool,
    error: Option<Fault>,
    changed: watch::Sender<u64>,
}

impl Inner {
    fn bump(&self) {
        self.changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }
}

impl Queue {
    pub(crate) fn new(max_records: usize, max_bytes: usize) -> Self {
        let (changed, _receiver) = watch::channel(0u64);
        Queue {
            inner: Arc::new(Mutex::new(Inner {
                records: VecDeque::new(),
                bytes: 0,
                max_records,
                max_bytes,
                closed: false,
                error: None,
                changed,
            })),
        }
    }

    pub(crate) fn bytes_of_record(reply: &Reply) -> usize {
        reply
            .attributes
            .iter()
            .map(|(name, value)| name.len() + value.len())
            .sum()
    }

    pub(crate) fn max_bytes(&self) -> usize {
        self.inner.lock().unwrap().max_bytes
    }

    pub(crate) fn is_full(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.records.len() >= inner.max_records || inner.bytes >= inner.max_bytes
    }

    #[cfg(test)]
    pub(crate) fn is_drained(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.records.len() <= inner.max_records / 2 && inner.bytes <= inner.max_bytes / 2
    }

    /// Append one record. Returns `false` when the queue is closed or the
    /// record alone exceeds the byte cap, in which case the queue fails with
    /// [`Fault::Limit`].
    pub(crate) fn push(&self, reply: Reply, bytes: usize) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return false;
        }
        if bytes > inner.max_bytes {
            inner.closed = true;
            inner.error = Some(Fault::Limit {
                scope: "stream bytes",
                limit: inner.max_bytes as u64,
                observed: bytes as u64,
            });
            inner.bump();
            return false;
        }
        inner.bytes += bytes;
        inner.records.push_back(Entry {
            record: reply.attributes,
            bytes,
        });
        inner.bump();
        true
    }

    /// Mark a normal end after every queued record has been delivered.
    pub(crate) fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return;
        }
        inner.closed = true;
        inner.bump();
    }

    /// End the queue with `fault`, dropping anything still queued.
    pub(crate) fn fail(&self, fault: Fault) {
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            return;
        }
        inner.closed = true;
        inner.error = Some(fault);
        inner.records.clear();
        inner.bytes = 0;
        inner.bump();
    }

    pub(crate) async fn recv(&self) -> Result<Option<Record>, Fault> {
        loop {
            let mut changed = {
                let mut inner = self.inner.lock().unwrap();
                if let Some(entry) = inner.records.pop_front() {
                    inner.bytes -= entry.bytes;
                    inner.bump();
                    return Ok(Some(entry.record));
                }
                if let Some(error) = &inner.error {
                    return Err(error.clone());
                }
                if inner.closed {
                    return Ok(None);
                }
                inner.changed.subscribe()
            };
            if changed.changed().await.is_err() {
                return Err(Fault::Closed);
            }
        }
    }

    pub(crate) fn queued_bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Kind, Reply};

    fn reply(words: &[(&str, &str)]) -> Reply {
        let mut attributes = std::collections::BTreeMap::new();
        for (name, value) in words {
            attributes.insert((*name).to_owned(), (*value).to_owned());
        }
        Reply {
            kind: Kind::Reply,
            tag: None,
            attributes,
            api_attributes: std::collections::BTreeMap::new(),
            plain: Vec::new(),
            fatal_reason: String::new(),
        }
    }

    #[tokio::test]
    async fn records_arrive_in_push_order_then_a_normal_end() {
        let queue = Queue::new(4, 1 << 20);
        queue.push(reply(&[("n", "1")]), 10);
        queue.push(reply(&[("n", "2")]), 10);
        queue.close();
        assert_eq!(queue.recv().await.unwrap().unwrap()["n"], "1");
        assert_eq!(queue.recv().await.unwrap().unwrap()["n"], "2");
        assert_eq!(queue.recv().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_failed_queue_reports_the_failure_not_an_end() {
        let queue = Queue::new(4, 1 << 20);
        queue.push(reply(&[("n", "1")]), 10);
        queue.fail(Fault::Closed);
        assert!(matches!(queue.recv().await, Err(Fault::Closed)));
    }

    #[tokio::test]
    async fn an_oversized_record_fails_the_queue() {
        let queue = Queue::new(4, 16);
        let oversized = reply(&[("name", "a-very-long-value")]);
        assert!(!queue.push(oversized, 64));
        assert!(matches!(
            queue.recv().await,
            Err(Fault::Limit {
                scope: "stream bytes",
                ..
            })
        ));
    }

    #[tokio::test]
    async fn fullness_and_draining_are_reported() {
        let queue = Queue::new(2, 100);
        queue.push(reply(&[("a", "1")]), 60);
        queue.push(reply(&[("a", "2")]), 60);
        assert!(queue.is_full());
        assert!(!queue.is_drained());
        queue.recv().await.unwrap();
        assert!(!queue.is_drained());
        queue.recv().await.unwrap();
        assert!(queue.is_drained());
    }

    #[tokio::test]
    async fn a_push_after_the_receiver_is_already_waiting_wakes_it() {
        let queue = Queue::new(4, 1 << 20);
        let receiver = queue.clone();
        let waiter = tokio::spawn(async move { receiver.recv().await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        queue.push(reply(&[("n", "late")]), 4);
        let record = waiter.await.unwrap().unwrap().unwrap();
        assert_eq!(record["n"], "late");
    }
}
