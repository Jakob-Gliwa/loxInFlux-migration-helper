//! Bounded producer/consumer pipeline: readers fill line protocol buffers,
//! writer threads compress and send them.
//!
//! Memory is bounded by construction: there is a fixed pool of
//! `(readers + writers) * 2` buffers of about `--batch-bytes` each. A reader
//! must take a free buffer from the pool before it can produce more output,
//! so a slow InfluxDB slows the readers down instead of filling RAM.

use crate::error::{Kind, classify};
use crate::guard::{InfluxGate, RateLimit, Shared, aborted_error};
use crate::influx::Client;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, bounded, unbounded};
use flate2::{Compression, write::GzEncoder};
use std::io::Write as _;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use tracing::error;

/// Where converters put their output.
pub trait LineSink {
    fn buf(&mut self) -> &mut Vec<u8>;
    /// Called after `values` field values were appended to `buf()`.
    fn commit(&mut self, values: u64) -> anyhow::Result<()>;
}

/// Collects everything in memory (tests).
#[cfg(test)]
#[derive(Default)]
pub struct VecSink {
    pub data: Vec<u8>,
    pub values: u64,
}

#[cfg(test)]
impl LineSink for VecSink {
    fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
    fn commit(&mut self, values: u64) -> anyhow::Result<()> {
        self.values += values;
        Ok(())
    }
}

pub struct Batch {
    pub job: usize,
    pub data: Vec<u8>,
    pub values: u64,
}

pub enum Event {
    Acked { job: usize, values: u64 },
    Rejected { job: usize, message: String },
}

pub struct Config {
    pub bucket: String,
    pub dry_run: bool,
    pub gzip: bool,
    pub skip_rejected: bool,
    pub batch_values: u64,
    pub batch_bytes: usize,
}

pub struct Pipeline {
    work_tx: Option<Sender<Batch>>,
    pool_tx: Sender<Vec<u8>>,
    pool_rx: Receiver<Vec<u8>>,
    pub events: Receiver<Event>,
    handles: Vec<JoinHandle<()>>,
    shared: Arc<Shared>,
    cfg: Arc<Config>,
}

impl Pipeline {
    pub fn start(
        client: Arc<Client>,
        cfg: Config,
        writers: usize,
        readers: usize,
        shared: Arc<Shared>,
        gate: Arc<InfluxGate>,
        rate: Arc<RateLimit>,
    ) -> Self {
        let cfg = Arc::new(cfg);
        let pool_size = (writers + readers) * 2;
        let (pool_tx, pool_rx) = bounded(pool_size);
        for _ in 0..pool_size {
            // Room for one more line after reaching batch_bytes.
            let _ = pool_tx.send(Vec::with_capacity(cfg.batch_bytes + (64 << 10)));
        }
        let (work_tx, work_rx) = bounded::<Batch>(writers);
        let (ev_tx, ev_rx) = unbounded();
        let handles = (0..writers)
            .map(|i| {
                let (client, cfg, work_rx, pool_tx, ev_tx, shared, gate, rate) = (
                    client.clone(),
                    cfg.clone(),
                    work_rx.clone(),
                    pool_tx.clone(),
                    ev_tx.clone(),
                    shared.clone(),
                    gate.clone(),
                    rate.clone(),
                );
                std::thread::Builder::new()
                    .name(format!("writer-{i}"))
                    .spawn(move || writer(client, cfg, work_rx, pool_tx, ev_tx, shared, gate, rate))
                    .expect("spawning writer")
            })
            .collect();
        Self {
            work_tx: Some(work_tx),
            pool_tx,
            pool_rx,
            events: ev_rx,
            handles,
            shared,
            cfg,
        }
    }

    pub fn sink(&self, job: usize) -> anyhow::Result<BatchWriter> {
        let mut w = BatchWriter {
            job,
            work_tx: self.work_tx.clone().expect("pipeline running"),
            pool_tx: self.pool_tx.clone(),
            pool_rx: self.pool_rx.clone(),
            shared: self.shared.clone(),
            buf: Vec::new(),
            values: 0,
            batches: 0,
            max_values: self.cfg.batch_values,
            max_bytes: self.cfg.batch_bytes,
        };
        w.take_buffer()?;
        Ok(w)
    }

    /// Stop accepting work and wait for the writers to drain. All sinks must
    /// have been finished or dropped before.
    pub fn close(mut self) {
        self.work_tx.take();
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn writer(
    client: Arc<Client>,
    cfg: Arc<Config>,
    work: Receiver<Batch>,
    pool: Sender<Vec<u8>>,
    events: Sender<Event>,
    shared: Arc<Shared>,
    gate: Arc<InfluxGate>,
    rate: Arc<RateLimit>,
) {
    for mut b in work.iter() {
        if !shared.aborted() {
            let res = if cfg.dry_run {
                Ok(())
            } else {
                gate.wait(&shared).and_then(|()| {
                    rate.acquire(b.values);
                    let body = if cfg.gzip {
                        let mut gz = GzEncoder::new(
                            Vec::with_capacity(b.data.len() / 6),
                            Compression::fast(),
                        );
                        gz.write_all(&b.data)?;
                        gz.finish()?
                    } else {
                        b.data.clone()
                    };
                    client.write_body(&cfg.bucket, bytes::Bytes::from(body), cfg.gzip)
                })
            };
            match res {
                Ok(()) => {
                    let _ = events.send(Event::Acked {
                        job: b.job,
                        values: b.values,
                    });
                }
                Err(e) if cfg.skip_rejected && classify(&e).0 == Kind::WriteRejected => {
                    error!(error = %e, values = b.values, "batch rejected, continuing (--on-reject skip)");
                    let _ = events.send(Event::Rejected {
                        job: b.job,
                        message: format!("{e:#}"),
                    });
                }
                Err(e) => shared.fail(e),
            }
        }
        b.data.clear();
        let _ = pool.send(b.data);
    }
}

/// A reader's handle: fills pool buffers and hands full ones to the writers.
pub struct BatchWriter {
    job: usize,
    work_tx: Sender<Batch>,
    pool_tx: Sender<Vec<u8>>,
    pool_rx: Receiver<Vec<u8>>,
    shared: Arc<Shared>,
    buf: Vec<u8>,
    values: u64,
    pub batches: u64,
    max_values: u64,
    max_bytes: usize,
}

impl BatchWriter {
    fn take_buffer(&mut self) -> anyhow::Result<()> {
        loop {
            match self.pool_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(b) => {
                    self.buf = b;
                    return Ok(());
                }
                Err(RecvTimeoutError::Timeout) if !self.shared.aborted() => continue,
                Err(_) => return Err(aborted_error()),
            }
        }
    }

    fn send(&mut self) -> anyhow::Result<()> {
        let mut batch = Batch {
            job: self.job,
            data: std::mem::take(&mut self.buf),
            values: self.values,
        };
        loop {
            match self.work_tx.send_timeout(batch, Duration::from_millis(200)) {
                Ok(()) => break,
                Err(crossbeam_channel::SendTimeoutError::Timeout(b)) if !self.shared.aborted() => {
                    batch = b
                }
                Err(_) => return Err(aborted_error()),
            }
        }
        self.batches += 1;
        self.values = 0;
        Ok(())
    }

    /// Send what is left; returns the number of batches this sink produced.
    pub fn finish(mut self) -> anyhow::Result<u64> {
        if self.values > 0 {
            self.send()?;
        } else {
            self.buf.clear();
            let _ = self.pool_tx.send(std::mem::take(&mut self.buf));
        }
        Ok(self.batches)
    }
}

impl LineSink for BatchWriter {
    fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
    fn commit(&mut self, values: u64) -> anyhow::Result<()> {
        self.values += values;
        if self.values >= self.max_values || self.buf.len() >= self.max_bytes {
            self.send()?;
            self.take_buffer()?;
        }
        Ok(())
    }
}
