//! One authenticated connection: dialling, login, tag routing, cancellation,
//! and bounded stream delivery.
//!
//! The concurrency model mirrors the Go client: one reader task owns the
//! decoder and routes every reply by tag; one writer task owns every write
//! from a single queue; each submitted sentence gets a decimal tag and a
//! pending entry in the shared state.
//!
//! Everything mutable lives behind one `std::sync::Mutex`; the critical
//! sections are short and never await. Wakeups use `watch` channels, never
//! bare notifications: `settled` tells a submitter waiting for its write that
//! the generation ended, `gate` tells the reader whether it may consume the
//! socket, and [`Queue`] counts its own versions so a drain between inspecting
//! a queue and awaiting it cannot be lost.

mod fault;
mod login;
mod stream;

pub(crate) use fault::Fault;
pub(crate) use stream::Queue;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot, watch};

use crate::cancel::Cancel;
use crate::error::{Error, Result};
use crate::record::{CommandResult, Record};
use crate::wire::{Decoder, Encoder, Kind, Limits, Reply, parse_reply};

use super::Config;
use super::socket::{DialOptions, IoStream, dial};

/// How a command ended.
pub(crate) enum Outcome {
    /// A `!done` resolved every record.
    Done { records: Vec<Record>, done: Record },
    /// A `!trap` preceded the terminating `!done`.
    Trap {
        category: String,
        message: String,
        done: Record,
    },
    /// The operation ended without a router reply.
    Fault(Fault),
}

/// One submitted sentence awaiting its replies.
struct Pending {
    /// The decimal routing tag sent on the wire.
    tag: String,
    /// Completion signal for a command; the router task takes it when the tag
    /// resolves.
    completion: Option<oneshot::Sender<Outcome>>,
    /// Every `!re` received for a command, in arrival order.
    records: Vec<Record>,
    /// The last `!trap` seen before the terminating `!done`.
    trap: Option<Reply>,
    /// The bounded queue of a stream request; `None` for a command.
    queue: Option<Queue>,
    /// Whether the writer committed the sentence to the wire. Guarded by the
    /// connection state.
    written: bool,
    /// Signals that cancellation cleanup finished for this tag.
    cancel_done: Option<oneshot::Sender<()>>,
}

/// What [`Core::submit`] hands back to its caller.
struct Submission {
    tag: String,
    completion: oneshot::Receiver<Outcome>,
    queue: Option<Queue>,
}

/// A queued write.
enum WriteJob {
    /// One tagged request, acknowledged once it reaches the socket.
    Sentence {
        tag: String,
        sentence: Vec<u8>,
        ack: oneshot::Sender<std::result::Result<(), WriteFailure>>,
    },
    /// The tagless `/cancel` sentence; nobody awaits it.
    Cancel { tag: String, sentence: Vec<u8> },
    /// Flush and half-close the write side.
    Shutdown,
}

/// Why the writer refused or failed a job.
enum WriteFailure {
    /// The request was cancelled before the writer committed it.
    Cancelled,
    /// The generation ended or the socket failed.
    Fault(Fault),
}

/// The part of a connection the reader task and the callers share.
struct Core {
    state: Arc<Mutex<State>>,
    encoder: Encoder,
    writes: mpsc::UnboundedSender<WriteJob>,
}

/// One `RouterOS` connection generation.
pub(crate) struct Conn {
    core: Arc<Core>,
    tasks: Mutex<Option<Tasks>>,
}

struct Tasks {
    reader: tokio::task::JoinHandle<()>,
    writer: tokio::task::JoinHandle<()>,
}

struct State {
    pending: HashMap<String, Pending>,
    cancelling: HashMap<String, Pending>,
    zombies: HashSet<String>,
    grace: Duration,
    counter: u64,
    cancel_acks: u64,
    command_timeout: Duration,
    stream_max_records: usize,
    stream_max_bytes: usize,
    max_buffered_stream_bytes: usize,
    max_open_streams: usize,
    pause_count: usize,
    aggregate_paused: bool,
    terminal: Option<Fault>,
    gate: watch::Sender<bool>,
    settled: watch::Sender<bool>,
}

impl State {
    /// The number of streams occupying a slot, counting cancelled ones still
    /// awaiting cleanup.
    fn open_streams(&self) -> usize {
        self.pending
            .values()
            .chain(self.cancelling.values())
            .filter(|pending| pending.queue.is_some())
            .count()
    }
}

// ---------------------------------------------------------------------------

/// Open a connection and start its reader and writer tasks.
///
/// `config` must already be normalized.
pub(crate) async fn open(
    dial_options: DialOptions,
    config: &Config,
    cancel: &Cancel,
) -> Result<Conn> {
    if cancel.is_cancelled() {
        return Err(Error::Cancelled);
    }
    let limits = Limits::new(
        config.max_word_bytes,
        config.max_sentence_bytes,
        config.max_words_per_sentence,
    );
    let (stream, _elapsed) = tokio::time::timeout(
        config.connect_timeout + Duration::from_millis(2000),
        dial(&dial_options),
    )
    .await
    .map_err(|_elapsed| Error::Timeout {
        phase: "connect",
        duration: config.connect_timeout,
    })??;
    let (mut reader, mut writer) = stream.split();

    login::login(&mut reader, &mut writer, config, cancel).await?;

    let (gate, _gate_receiver) = watch::channel(true);
    let (settled, _settled_receiver) = watch::channel(false);
    let state = Arc::new(Mutex::new(State {
        pending: HashMap::new(),
        cancelling: HashMap::new(),
        zombies: HashSet::new(),
        grace: config.cancel_grace,
        counter: 0,
        cancel_acks: 0,
        command_timeout: config.command_timeout,
        stream_max_records: config.stream_max_records,
        stream_max_bytes: config.stream_max_bytes,
        max_buffered_stream_bytes: config.max_buffered_stream_bytes,
        max_open_streams: config.max_open_streams,
        pause_count: 0,
        aggregate_paused: false,
        terminal: None,
        gate,
        settled,
    }));
    let (write_tx, write_rx) = mpsc::unbounded_channel();
    let core = Arc::new(Core {
        state,
        encoder: Encoder::new(limits),
        writes: write_tx,
    });

    let reader_task = spawn_reader(reader, Arc::clone(&core), limits);
    let writer_task = spawn_writer(writer, write_rx, Arc::clone(&core));
    Ok(Conn {
        core,
        tasks: Mutex::new(Some(Tasks {
            reader: reader_task,
            writer: writer_task,
        })),
    })
}

fn spawn_reader(
    mut reader: ReadHalf<IoStream>,
    core: Arc<Core>,
    limits: Limits,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut gate = core.state.lock().unwrap().gate.subscribe();
        let mut decoder = Decoder::new(limits);
        let mut buffer = vec![0u8; 32 << 10];
        loop {
            if !wait_readable(&core, &mut gate).await {
                break;
            }
            match reader.read(&mut buffer).await {
                Ok(0) => {
                    core.terminate(&Fault::Io {
                        op: "read",
                        message: "connection closed by peer".to_owned(),
                    });
                    break;
                }
                Ok(n) => {
                    if !route_chunk(&core, &mut decoder, &buffer[..n]) {
                        break;
                    }
                }
                Err(error) => {
                    core.terminate(&Fault::io("read", &error));
                    break;
                }
            }
        }
    })
}

fn route_chunk(core: &Core, decoder: &mut Decoder, chunk: &[u8]) -> bool {
    let sentences = match decoder.feed(chunk) {
        Ok(sentences) => sentences,
        Err(error) => {
            core.terminate(&Fault::from_wire(error));
            return false;
        }
    };
    for words in sentences {
        let reply = match parse_reply(&words) {
            Ok(reply) => reply,
            Err(error) => {
                core.terminate(&Fault::from_wire(error));
                return false;
            }
        };
        if !route(core, reply) {
            return false;
        }
    }
    true
}

fn spawn_writer(
    mut writer: WriteHalf<IoStream>,
    mut write_rx: mpsc::UnboundedReceiver<WriteJob>,
    core: Arc<Core>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(job) = write_rx.recv().await {
            match job {
                WriteJob::Shutdown => {
                    let _ = writer.shutdown().await;
                    break;
                }
                WriteJob::Sentence { tag, sentence, ack } => {
                    if let Err(failure) = commit_sentence(&core, &tag) {
                        let _ = ack.send(Err(failure));
                        continue;
                    }
                    if let Err(error) = writer.write_all(&sentence).await {
                        let fault = Fault::io("write", &error);
                        core.terminate(&fault);
                        let _ = ack.send(Err(WriteFailure::Fault(fault)));
                        break;
                    }
                    if let Err(error) = writer.flush().await {
                        let fault = Fault::io("write", &error);
                        core.terminate(&fault);
                        let _ = ack.send(Err(WriteFailure::Fault(fault)));
                        break;
                    }
                    let _ = ack.send(Ok(()));
                }
                WriteJob::Cancel { tag, sentence } => {
                    if !begin_cancel_write(&core, &tag) {
                        continue;
                    }
                    if let Err(error) = writer.write_all(&sentence).await {
                        core.terminate(&Fault::io("write", &error));
                        break;
                    }
                    if let Err(error) = writer.flush().await {
                        core.terminate(&Fault::io("write", &error));
                        break;
                    }
                }
            }
        }
    })
}

fn commit_sentence(core: &Core, tag: &str) -> std::result::Result<(), WriteFailure> {
    let mut state = core.state.lock().unwrap();
    if let Some(fault) = state.terminal.clone() {
        return Err(WriteFailure::Fault(fault));
    }
    if let Some(pending) = state.pending.get_mut(tag) {
        pending.written = true;
        Ok(())
    } else {
        // The caller cancelled before the write was committed, so the mutation
        // never reaches the router.
        Err(WriteFailure::Cancelled)
    }
}

fn begin_cancel_write(core: &Core, tag: &str) -> bool {
    let mut state = core.state.lock().unwrap();
    if state.terminal.is_some() {
        return false;
    }
    if state
        .cancelling
        .get(tag)
        .is_some_and(|pending| pending.written)
    {
        // Its untagged `!done` is counted separately from replies for the
        // command being cancelled.
        state.cancel_acks += 1;
        true
    } else {
        false
    }
}

// ---------------------------------------------------------------------------

impl Conn {
    /// Run one command end to end.
    pub(crate) async fn command(
        &self,
        words: &[String],
        cancel: &Cancel,
        timeout: Option<Duration>,
        op: &'static str,
    ) -> Result<CommandResult> {
        let submission = self.core.submit(words, false, cancel, None, op).await?;
        let Submission {
            tag, completion, ..
        } = submission;
        let timeout = timeout.unwrap_or_else(|| self.core.command_timeout());
        let outcome = tokio::select! {
            outcome = completion => outcome,
            () = cancel.cancelled() => {
                self.core.cancel_tag(&tag, Fault::Cancelled);
                return Err(Error::Cancelled);
            }
            () = tokio::time::sleep(timeout) => {
                let fault = Fault::Timeout {
                    phase: "command",
                    duration: timeout,
                };
                self.core.cancel_tag(&tag, fault.clone());
                return Err(fault.to_error(op));
            }
        };
        match outcome {
            Ok(Outcome::Done { records, done }) => Ok(CommandResult { records, done }),
            Ok(Outcome::Trap {
                category,
                message,
                done,
            }) => Err(Error::Trap {
                category,
                message,
                done,
            }),
            Ok(Outcome::Fault(fault)) => Err(fault.to_error(op)),
            Err(_receiver_error) => Err(Fault::Closed.to_error(op)),
        }
    }

    /// Submit one long-running command and return its tag and queue.
    pub(crate) async fn stream(
        &self,
        words: &[String],
        cancel: &Cancel,
        overrides: (Option<usize>, Option<usize>),
        op: &'static str,
    ) -> Result<(String, Queue)> {
        let bounds = self.core.stream_bounds(overrides)?;
        let submission = self.core.submit(words, true, cancel, bounds, op).await?;
        match submission.queue {
            Some(queue) => Ok((submission.tag, queue)),
            None => Err(Error::Protocol {
                detail: "stream submission has no queue".to_owned(),
            }),
        }
    }

    /// The terminal cause of this generation, if it has ended.
    pub(crate) fn terminal(&self) -> Option<Fault> {
        self.core.terminal()
    }

    /// Resolve when this generation ends: a fault, a close, or a reconnect.
    ///
    /// The receiver subscribes before it reads `terminal`, so an end that
    /// happens between the two is observed rather than slept through.
    pub(crate) async fn wait_ended(&self) {
        let mut settled = {
            let state = self.core.state.lock().unwrap();
            state.settled.subscribe()
        };
        if self.core.terminal().is_some() {
            return;
        }
        let _ = settled.changed().await;
    }

    /// Ask the router to cancel `tag` and report when cleanup finishes.
    pub(crate) fn cancel_tag(&self, tag: &str, fault: Fault) -> oneshot::Receiver<()> {
        self.core.cancel_tag(tag, fault)
    }

    /// Cancel without waiting. Used by [`crate::Stream`]'s drop path.
    pub(crate) fn request_cancel(&self, tag: &str, fault: Fault) {
        let _waiter = self.core.cancel_tag(tag, fault);
    }

    /// Tell the connection one record left `tag`'s queue.
    pub(crate) fn note_drained(&self, tag: &str) {
        self.core.note_drained(tag);
    }

    /// Fail every operation and release the socket. Idempotent.
    pub(crate) async fn close(&self) {
        self.shutdown(&Fault::Closed).await;
    }

    /// End this generation with `fault` and wait briefly for its tasks.
    pub(crate) async fn shutdown(&self, fault: &Fault) {
        self.core.terminate(fault);
        let _ = self.core.writes.send(WriteJob::Shutdown);
        let tasks = self.tasks.lock().unwrap().take();
        if let Some(Tasks {
            mut reader,
            mut writer,
        }) = tasks
        {
            let short = Duration::from_millis(500);
            let _ = tokio::time::timeout(short, std::pin::Pin::new(&mut writer)).await;
            writer.abort();
            let _ = tokio::time::timeout(short, std::pin::Pin::new(&mut reader)).await;
            reader.abort();
        }
    }
}

impl Core {
    async fn submit(
        &self,
        words: &[String],
        streaming: bool,
        cancel: &Cancel,
        bounds: Option<(usize, usize)>,
        op: &'static str,
    ) -> Result<Submission> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }

        let (tag, completion, queue, mut settled, sentence) = {
            let mut state = self.state.lock().unwrap();
            if let Some(fault) = state.terminal.clone() {
                return Err(fault.to_error(op));
            }
            if streaming {
                let open = state.open_streams();
                if open >= state.max_open_streams {
                    return Err(Error::ResourceLimit {
                        scope: "open streams",
                        limit: state.max_open_streams as u64,
                        observed: (open + 1) as u64,
                    });
                }
            }

            state.counter = state.counter.wrapping_add(1);
            let tag = state.counter.to_string();
            let queue = streaming.then(|| {
                let (max_records, max_bytes) =
                    bounds.unwrap_or((state.stream_max_records, state.stream_max_bytes));
                Queue::new(max_records, max_bytes)
            });
            let mut tagged = Vec::with_capacity(words.len() + 1);
            tagged.extend_from_slice(words);
            tagged.push(format!(".tag={tag}"));
            let sentence = match self.encoder.sentence(&tagged) {
                Ok(sentence) => sentence,
                Err(error) => return Err(Fault::from_wire(error).to_error(op)),
            };

            let (completion_tx, completion_rx) = oneshot::channel();
            state.pending.insert(
                tag.clone(),
                Pending {
                    tag: tag.clone(),
                    completion: Some(completion_tx),
                    records: Vec::new(),
                    trap: None,
                    queue: queue.clone(),
                    written: false,
                    cancel_done: None,
                },
            );
            let settled = state.settled.subscribe();
            (tag, completion_rx, queue, settled, sentence)
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        let job = WriteJob::Sentence {
            tag: tag.clone(),
            sentence,
            ack: ack_tx,
        };
        if self.writes.send(job).is_err() {
            self.forget_pending(&tag);
            return Err(self.terminal_or_closed().to_error(op));
        }

        let ack = tokio::select! {
            ack = ack_rx => ack,
            _ = settled.changed() => {
                self.forget_pending(&tag);
                return Err(self.terminal_or_closed().to_error(op));
            }
            () = cancel.cancelled() => {
                // The writer skips a request cancelled before it is committed.
                self.cancel_tag(&tag, Fault::Cancelled);
                return Err(Error::Cancelled);
            }
        };

        match ack {
            Ok(Ok(())) => Ok(Submission {
                tag,
                completion,
                queue,
            }),
            Ok(Err(WriteFailure::Cancelled)) => Err(Error::Cancelled),
            Ok(Err(WriteFailure::Fault(fault))) => Err(fault.to_error(op)),
            Err(_receiver_error) => Err(self.terminal_or_closed().to_error(op)),
        }
    }

    /// Resolve per-stream queue overrides against the connection defaults.
    ///
    /// The byte bound cannot exceed the connection's aggregate budget: one
    /// queue larger than what the pause accounting tracks would defeat it.
    fn stream_bounds(
        &self,
        (records, bytes): (Option<usize>, Option<usize>),
    ) -> Result<Option<(usize, usize)>> {
        if records.is_none() && bytes.is_none() {
            return Ok(None);
        }
        let state = self.state.lock().unwrap();
        let records = records.unwrap_or(state.stream_max_records);
        let bytes = bytes.unwrap_or(state.stream_max_bytes);
        if bytes > state.max_buffered_stream_bytes {
            return Err(Error::InvalidRequest {
                detail: format!(
                    "stream byte bound {bytes} exceeds the connection budget {}",
                    state.max_buffered_stream_bytes
                ),
            });
        }
        Ok(Some((records, bytes)))
    }

    fn command_timeout(&self) -> Duration {
        self.state.lock().unwrap().command_timeout
    }

    fn terminal(&self) -> Option<Fault> {
        self.state.lock().unwrap().terminal.clone()
    }

    fn terminal_or_closed(&self) -> Fault {
        self.terminal().unwrap_or(Fault::Closed)
    }

    fn forget_pending(&self, tag: &str) {
        let mut state = self.state.lock().unwrap();
        if state.pending.remove(tag).is_some() && state.terminal.is_none() {
            recompute_pause(&mut state);
        }
    }

    fn note_drained(&self, tag: &str) {
        let mut state = self.state.lock().unwrap();
        if state.terminal.is_some() {
            return;
        }
        if state.pending.contains_key(tag) || state.cancelling.contains_key(tag) {
            recompute_pause(&mut state);
        }
    }

    /// Remove `tag` from circulation, fail its queue, and ask the router to
    /// cancel it. Returns a receiver that resolves when the router answered
    /// `!done`, the grace period expired, or the generation ended.
    fn cancel_tag(&self, tag: &str, fault: Fault) -> oneshot::Receiver<()> {
        let (done_tx, done_rx) = oneshot::channel();
        let mut state = self.state.lock().unwrap();
        if state.cancelling.contains_key(tag) {
            drop(state);
            let _ = done_tx.send(());
            return done_rx;
        }
        let Some(mut pending) = state.pending.remove(tag) else {
            drop(state);
            let _ = done_tx.send(());
            return done_rx;
        };
        let tag = pending.tag.clone();

        if let Some(queue) = pending.queue.take() {
            queue.fail(fault.clone());
        }
        let completion = pending.completion.take();
        if !pending.written {
            // The writer will notice the tag is gone and skip the sentence, so
            // no `/cancel` is owed and cleanup is already finished.
            if state.terminal.is_none() {
                recompute_pause(&mut state);
            }
            drop(state);
            if let Some(completion) = completion {
                let _ = completion.send(Outcome::Fault(fault));
            }
            let _ = done_tx.send(());
            return done_rx;
        }

        pending.cancel_done = Some(done_tx);
        state.cancelling.insert(tag.clone(), pending);
        let grace = state.grace;
        if state.terminal.is_none() {
            recompute_pause(&mut state);
        }
        drop(state);

        // Resolve the caller before the cancel goes out, so it returns
        // immediately.
        if let Some(completion) = completion {
            let _ = completion.send(Outcome::Fault(fault));
        }
        if let Ok(sentence) = self
            .encoder
            .sentence(&["/cancel".to_owned(), format!("=tag={tag}")])
        {
            let _ = self.writes.send(WriteJob::Cancel {
                tag: tag.clone(),
                sentence,
            });
        }
        spawn_grace_timer(Arc::clone(&self.state), tag, grace);
        done_rx
    }

    fn terminate(&self, fault: &Fault) {
        let mut state = self.state.lock().unwrap();
        if state.terminal.is_some() {
            return;
        }
        state.terminal = Some(fault.clone());
        let pending: Vec<Pending> = state.pending.drain().map(|(_, pending)| pending).collect();
        let cancelling: Vec<Pending> = state
            .cancelling
            .drain()
            .map(|(_, pending)| pending)
            .collect();
        state.zombies.clear();
        state.cancel_acks = 0;
        state.pause_count = 0;
        state.aggregate_paused = false;
        state.gate.send_replace(true);
        state.settled.send_replace(true);
        drop(state);

        for mut pending in pending {
            if let Some(queue) = pending.queue.take() {
                queue.fail(fault.clone());
            }
            if let Some(completion) = pending.completion.take() {
                let _ = completion.send(Outcome::Fault(fault.clone()));
            }
        }
        for mut pending in cancelling {
            if let Some(queue) = pending.queue.take() {
                queue.fail(fault.clone());
            }
            if let Some(completion) = pending.completion.take() {
                let _ = completion.send(Outcome::Fault(fault.clone()));
            }
            if let Some(done) = pending.cancel_done.take() {
                let _ = done.send(());
            }
        }
    }
}

// ---------------------------------------------------------------------------

async fn wait_readable(core: &Core, gate: &mut watch::Receiver<bool>) -> bool {
    loop {
        {
            let state = core.state.lock().unwrap();
            if state.terminal.is_some() {
                return false;
            }
            if state.pause_count == 0 {
                return true;
            }
        }
        if gate.changed().await.is_err() {
            return false;
        }
    }
}

fn route(core: &Core, reply: Reply) -> bool {
    if reply.kind == Kind::Fatal {
        core.terminate(&Fault::Fatal {
            reason: reply.fatal_reason.clone(),
        });
        return false;
    }

    let mut state = core.state.lock().unwrap();

    let Some(tag) = reply.tag().map(str::to_owned) else {
        if reply.kind == Kind::Done && state.cancel_acks > 0 {
            state.cancel_acks -= 1;
            return true;
        }
        let detail = format!("untagged {} reply", reply.kind.word());
        drop(state);
        core.terminate(&Fault::Protocol { detail });
        return false;
    };

    if !state.pending.contains_key(&tag) {
        let absorbed = absorb_cancelled(&mut state, &tag, reply.kind == Kind::Done);
        drop(state);
        if absorbed {
            return true;
        }
        core.terminate(&Fault::Protocol {
            detail: format!("reply for unknown tag {tag:?}"),
        });
        return false;
    }

    match reply.kind {
        Kind::Reply => match deliver_reply(&mut state, &tag, reply) {
            ReplyDelivery::Queued => {
                recompute_pause(&mut state);
                true
            }
            ReplyDelivery::Oversized(fault) => {
                drop(state);
                core.cancel_tag(&tag, fault);
                true
            }
        },
        Kind::Trap => {
            if let Some(pending) = state.pending.get_mut(&tag) {
                pending.trap = Some(reply);
            }
            true
        }
        Kind::Done => {
            deliver_done(&mut state, &tag, reply);
            recompute_pause(&mut state);
            true
        }
        Kind::Empty => true,
        Kind::Fatal => false,
    }
}

/// A reply for a tag that is no longer pending is absorbed while its
/// cancellation is outstanding, or after the grace period as a zombie.
fn absorb_cancelled(state: &mut State, tag: &str, done: bool) -> bool {
    let absorbed = state.cancelling.contains_key(tag) || state.zombies.contains(tag);
    if done {
        if let Some(waiter) = state
            .cancelling
            .remove(tag)
            .and_then(|pending| pending.cancel_done)
        {
            let _ = waiter.send(());
        }
        state.zombies.remove(tag);
    }
    absorbed
}

enum ReplyDelivery {
    Queued,
    Oversized(Fault),
}

fn deliver_reply(state: &mut State, tag: &str, reply: Reply) -> ReplyDelivery {
    let Some(pending) = state.pending.get_mut(tag) else {
        return ReplyDelivery::Queued;
    };
    let Some(queue) = &pending.queue else {
        pending.records.push(reply.attributes);
        return ReplyDelivery::Queued;
    };
    let bytes = Queue::bytes_of_record(&reply);
    let max_bytes = queue.max_bytes();
    if bytes > max_bytes {
        return ReplyDelivery::Oversized(Fault::Limit {
            scope: "stream bytes",
            limit: max_bytes as u64,
            observed: bytes as u64,
        });
    }
    let _pushed = queue.push(reply, bytes);
    ReplyDelivery::Queued
}

fn deliver_done(state: &mut State, tag: &str, reply: Reply) {
    if let Some(waiter) = state
        .cancelling
        .remove(tag)
        .and_then(|pending| pending.cancel_done)
    {
        let _ = waiter.send(());
    }
    state.zombies.remove(tag);
    let Some(mut pending) = state.pending.remove(tag) else {
        return;
    };
    let done = reply.attributes;
    if let Some(queue) = pending.queue.take() {
        if let Some(trap) = pending.trap.take() {
            queue.fail(Fault::Trap {
                category: trap.attributes.get("category").cloned().unwrap_or_default(),
                message: trap.attributes.get("message").cloned().unwrap_or_default(),
                done,
            });
        } else {
            queue.close();
        }
        return;
    }
    if let Some(sender) = pending.completion.take() {
        let outcome = match pending.trap.take() {
            Some(trap) => Outcome::Trap {
                category: trap.attributes.get("category").cloned().unwrap_or_default(),
                message: trap.attributes.get("message").cloned().unwrap_or_default(),
                done,
            },
            None => Outcome::Done {
                records: pending.records,
                done,
            },
        };
        let _ = sender.send(outcome);
    }
}

/// Recompute the reader gate from every stream queue.
///
/// The caller holds the state lock. Each queue takes its own lock only after
/// the state lock, never before, so this order cannot deadlock.
fn recompute_pause(state: &mut State) {
    let mut pauses = 0;
    let mut buffered = 0usize;
    for pending in state.pending.values().chain(state.cancelling.values()) {
        if let Some(queue) = &pending.queue {
            buffered = buffered.saturating_add(queue.queued_bytes());
            if queue.is_full() {
                pauses += 1;
            }
        }
    }
    if state.aggregate_paused {
        if buffered <= state.max_buffered_stream_bytes / 2 {
            state.aggregate_paused = false;
        }
    } else if buffered >= state.max_buffered_stream_bytes {
        state.aggregate_paused = true;
    }
    if state.aggregate_paused {
        pauses += 1;
    }
    state.pause_count = pauses;
    state.gate.send_replace(pauses == 0);
}

fn spawn_grace_timer(state: Arc<Mutex<State>>, tag: String, grace: Duration) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        tokio::time::sleep(grace).await;
        let mut state = state.lock().unwrap();
        if let Some(pending) = state.cancelling.remove(&tag) {
            state.zombies.insert(tag);
            if let Some(done) = pending.cancel_done {
                let _ = done.send(());
            }
            if state.terminal.is_none() {
                recompute_pause(&mut state);
            }
        }
    });
}
