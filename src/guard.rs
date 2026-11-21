//! Resource guards.
//!
//! * [`MemWatch`]: this process. Samples the resident set size; above
//!   `--memory-limit-mb` it aborts the run cleanly (exit code 9) instead of
//!   letting the kernel or a container limit kill it — or the host swap.
//! * [`InfluxGate`]: the InfluxDB server. Polls `/metrics` and pauses all
//!   writes while the server's Go heap is more than `--influx-heap-headroom-mb`
//!   above where it was when the run started, so a production InfluxDB is
//!   never pushed into its memory limit by the migration.

use crate::error::{Kind, fail};
use crate::influx::Client;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Shared abort flag plus the first error that caused it.
#[derive(Default)]
pub struct Shared {
    abort: AtomicBool,
    reason: Mutex<Option<anyhow::Error>>,
}

impl Shared {
    pub fn fail(&self, e: anyhow::Error) {
        let mut r = self.reason.lock().unwrap_or_else(|p| p.into_inner());
        if r.is_none() {
            *r = Some(e);
        }
        self.abort.store(true, Ordering::SeqCst);
    }
    pub fn aborted(&self) -> bool {
        self.abort.load(Ordering::Relaxed)
    }
    pub fn flag(&self) -> &AtomicBool {
        &self.abort
    }
    pub fn take_reason(&self) -> Option<anyhow::Error> {
        self.reason.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

static CURRENT: Mutex<Option<Arc<Shared>>> = Mutex::new(None);
static SIGNALS: AtomicU64 = AtomicU64::new(0);

/// Route SIGINT/SIGTERM to the running command: the first signal stops it
/// cleanly (finished work stays recorded, exit code 10), a second one exits
/// immediately.
pub fn install_signal_handler() {
    let _ = ctrlc::set_handler(|| {
        if SIGNALS.fetch_add(1, Ordering::SeqCst) > 0 {
            eprintln!("second interrupt, exiting immediately");
            std::process::exit(130);
        }
        warn!("interrupted, stopping after in-flight requests (press again to exit immediately)");
        if let Some(s) = CURRENT.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
            s.fail(
                fail(Kind::Interrupted, "interrupted by signal")
                    .hint("rerun the same command to resume; finished windows are recorded")
                    .into(),
            );
        } else {
            std::process::exit(130);
        }
    });
}

impl Shared {
    /// Create and register as the target of signal handling.
    pub fn new_registered() -> Arc<Self> {
        let s = Arc::new(Self::default());
        *CURRENT.lock().unwrap_or_else(|p| p.into_inner()) = Some(s.clone());
        s
    }
}

pub fn aborted_error() -> anyhow::Error {
    fail(
        Kind::Internal,
        "aborted because another part of the run failed",
    )
    .into()
}

/// Resident set size in bytes (Linux); `None` elsewhere.
pub fn rss_bytes() -> Option<u64> {
    status_kb("VmRSS:").map(|kb| kb * 1024)
}

/// Kernel-tracked peak resident set size in bytes (Linux).
pub fn peak_rss_bytes() -> Option<u64> {
    status_kb("VmHWM:").map(|kb| kb * 1024)
}

fn status_kb(key: &str) -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    s.lines()
        .find(|l| l.starts_with(key))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

pub struct MemWatch {
    stop: AtomicBool,
    peak: AtomicU64,
    pub limit_bytes: u64,
}

impl MemWatch {
    pub fn start(limit_mb: u64, shared: Arc<Shared>) -> Arc<Self> {
        let w = Arc::new(Self {
            stop: AtomicBool::new(false),
            peak: AtomicU64::new(0),
            limit_bytes: limit_mb * 1024 * 1024,
        });
        if rss_bytes().is_none() {
            tracing::debug!("memory watchdog unavailable on this platform");
            return w;
        }
        w.sample(&shared);
        let me = w.clone();
        std::thread::Builder::new()
            .name("memwatch".into())
            .spawn(move || {
                while !me.stop.load(Ordering::Relaxed) {
                    me.sample(&shared);
                    std::thread::sleep(Duration::from_millis(100));
                }
            })
            .expect("spawning memwatch");
        w
    }

    fn sample(&self, shared: &Shared) {
        let Some(rss) = rss_bytes() else { return };
        self.peak.fetch_max(rss, Ordering::Relaxed);
        if self.limit_bytes > 0 && rss > self.limit_bytes && !shared.aborted() {
            shared.fail(
                fail(
                    Kind::ResourceLimit,
                    format!(
                        "memory use {} MiB exceeded --memory-limit-mb {}",
                        rss >> 20,
                        self.limit_bytes >> 20
                    ),
                )
                .hint("lower --parallel, --writers or --batch-mb, or raise --memory-limit-mb")
                .into(),
            );
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn peak_mib(&self) -> Option<u64> {
        let sampled = self.peak.load(Ordering::Relaxed);
        let kernel = peak_rss_bytes().unwrap_or(0);
        let p = sampled.max(kernel);
        (p > 0).then_some(p >> 20)
    }
}

pub struct InfluxGate {
    paused: AtomicBool,
    stop: AtomicBool,
    pub enabled: AtomicBool,
    pub baseline: AtomicU64,
    pub peak: AtomicU64,
    pub paused_total_ms: AtomicU64,
    max_pause: Duration,
}

impl InfluxGate {
    /// `headroom_mb == 0` disables the gate.
    pub fn start(client: Arc<Client>, headroom_mb: u64, max_pause: Duration) -> Arc<Self> {
        let g = Arc::new(Self {
            paused: AtomicBool::new(false),
            stop: AtomicBool::new(false),
            enabled: AtomicBool::new(false),
            baseline: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            paused_total_ms: AtomicU64::new(0),
            max_pause,
        });
        if headroom_mb == 0 {
            return g;
        }
        let Some(base) = client.heap_inuse_bytes() else {
            warn!(
                "InfluxDB /metrics has no go_memstats_heap_inuse_bytes; the InfluxDB memory guard is off"
            );
            return g;
        };
        g.baseline.store(base, Ordering::Relaxed);
        g.peak.store(base, Ordering::Relaxed);
        g.enabled.store(true, Ordering::Relaxed);
        let limit = base + headroom_mb * 1024 * 1024;
        info!(
            influx_heap_mib = base >> 20,
            pause_above_mib = limit >> 20,
            "InfluxDB memory guard on"
        );
        let me = g.clone();
        std::thread::Builder::new()
            .name("influxgate".into())
            .spawn(move || {
                while !me.stop.load(Ordering::Relaxed) {
                    if let Some(heap) = client.heap_inuse_bytes() {
                        me.peak.fetch_max(heap, Ordering::Relaxed);
                        let paused = me.paused.load(Ordering::Relaxed);
                        if !paused && heap > limit {
                            warn!(
                                influx_heap_mib = heap >> 20,
                                limit_mib = limit >> 20,
                                "InfluxDB heap high, pausing writes"
                            );
                            me.paused.store(true, Ordering::Relaxed);
                        } else if paused && heap < limit - (limit - base) / 5 {
                            info!(
                                influx_heap_mib = heap >> 20,
                                "InfluxDB heap back down, resuming writes"
                            );
                            me.paused.store(false, Ordering::Relaxed);
                        }
                    }
                    std::thread::sleep(Duration::from_secs(2));
                }
            })
            .expect("spawning influxgate");
        g
    }

    /// Block while paused. Fails after `max_pause` or when the run aborts.
    pub fn wait(&self, shared: &Shared) -> anyhow::Result<()> {
        if !self.paused.load(Ordering::Relaxed) {
            return Ok(());
        }
        let t0 = Instant::now();
        while self.paused.load(Ordering::Relaxed) {
            if shared.aborted() {
                return Err(aborted_error());
            }
            if t0.elapsed() > self.max_pause {
                return Err(fail(
                    Kind::ResourceLimit,
                    format!("InfluxDB heap stayed above the limit for {} s", self.max_pause.as_secs()),
                )
                .hint("let InfluxDB settle (compactions), raise --influx-heap-headroom-mb, or lower --writers")
                .into());
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        self.paused_total_ms
            .fetch_add(t0.elapsed().as_millis() as u64, Ordering::Relaxed);
        Ok(())
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Token bucket shared by all writers (`--max-rate` values per second).
pub struct RateLimit {
    per_sec: f64,
    next: Mutex<Instant>,
}

impl RateLimit {
    pub fn new(per_sec: u64) -> Self {
        Self {
            per_sec: per_sec as f64,
            next: Mutex::new(Instant::now()),
        }
    }
    pub fn acquire(&self, values: u64) {
        if self.per_sec <= 0.0 {
            return;
        }
        let wait = {
            let mut next = self.next.lock().unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            if *next < now {
                *next = now;
            }
            let start = *next;
            *next += Duration::from_secs_f64(values as f64 / self.per_sec);
            start.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            std::thread::sleep(wait);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_keeps_first_reason() {
        let s = Shared::default();
        s.fail(anyhow::anyhow!("first"));
        s.fail(anyhow::anyhow!("second"));
        assert!(s.aborted());
        assert_eq!(s.take_reason().unwrap().to_string(), "first");
    }

    #[test]
    fn rate_limit_spaces_requests() {
        let r = RateLimit::new(1000);
        let t = Instant::now();
        r.acquire(100);
        r.acquire(100);
        r.acquire(100);
        assert!(t.elapsed() >= Duration::from_millis(190));
        RateLimit::new(0).acquire(u64::MAX);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn rss_is_readable_on_linux() {
        assert!(rss_bytes().unwrap() > 0);
    }
}
