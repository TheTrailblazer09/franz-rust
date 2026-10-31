//! A disk that lies, and a driver that makes it lie the same way twice.
//!
//! The crash test in `scripts/` kills the broker with `SIGKILL`, which proves
//! less than it looks. A `write` to a regular file is not interrupted partway
//! through by a signal: the bytes reach the page cache or they do not. So the
//! failure that actually threatens a commit log — half a record batch on disk
//! and no record of how it got there — is unreachable from a real crash test
//! without pulling the power cord.
//!
//! This module models the two crash modes separately, because they are not the
//! same:
//!
//! * **Process kill.** The process dies; the page cache does not. Everything
//!   written survives, synced or not. Nothing is torn.
//! * **Power loss.** Only what was synced is guaranteed. Anything written since
//!   is a coin toss, and the hardware may persist an arbitrary *prefix* of it —
//!   which is exactly how a batch ends up half on disk.
//!
//! The invariant every run checks: **the log that comes back must be a
//! contiguous prefix of the appends that were acknowledged.** Records may be
//! missing from the tail. There must be no gap, no reordering, no offset that
//! reads back as a different record, and no batch that fails its checksum. And
//! when the policy was `fsync` on every append, nothing acknowledged may be
//! missing at all.
//!
//! Every run is a seed. A failure prints the seed that caused it, and that seed
//! reproduces it exactly, forever.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::batch;
use crate::files::{Blocks, Files};
use crate::log::{Config, Log, Sync};

/// xorshift64*, so a seed is all the state a run needs.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Crash {
    /// The process dies. The page cache survives it.
    ProcessKill,
    /// The machine loses power. Only synced bytes are certain.
    PowerLoss,
}

/// One file's worth of bytes, split by what has actually been persisted.
#[derive(Default, Clone)]
struct Contents {
    /// Survives power loss.
    durable: Vec<u8>,
    /// Survives a process dying. A superset of `durable`.
    cached: Vec<u8>,
}

#[derive(Default)]
struct Disk {
    files: HashMap<PathBuf, Contents>,
    dirs: Vec<PathBuf>,
    /// Bytes appended since the last sync, per file, newest append last.
    appends: HashMap<PathBuf, Vec<usize>>,
}

/// A simulated disk, shared by every file opened from it.
#[derive(Clone)]
pub struct SimDisk {
    disk: Arc<Mutex<Disk>>,
}

impl SimDisk {
    pub fn new() -> Self {
        SimDisk {
            disk: Arc::new(Mutex::new(Disk::default())),
        }
    }

    /// Applies a crash to every file, discarding whatever the crash mode says
    /// was never guaranteed.
    pub fn crash(&self, mode: Crash, rng: &mut Rng) {
        let mut disk = self.disk.lock().expect("disk");

        let paths: Vec<PathBuf> = disk.files.keys().cloned().collect();
        for path in paths {
            let appends = disk.appends.get(&path).cloned().unwrap_or_default();
            let file = disk.files.get_mut(&path).expect("file");

            match mode {
                // The page cache outlives the process, so everything written
                // is still there. The only loss is an append the writer had
                // not finished issuing, modelled as the tail of the last one.
                Crash::ProcessKill => {
                    if let Some(last) = appends.last()
                        && rng.chance(25)
                    {
                        let lose = rng.below(*last as u64 + 1) as usize;
                        let keep = file.cached.len() - lose;
                        file.cached.truncate(keep);
                    }
                    file.durable = file.cached.clone();
                }
                // Only synced bytes are certain. The hardware may still have
                // persisted a prefix of what came after, which is where torn
                // batches come from.
                Crash::PowerLoss => {
                    let unsynced = file.cached.len() - file.durable.len();
                    let survives = rng.below(unsynced as u64 + 1) as usize;
                    file.cached.truncate(file.durable.len() + survives);
                    file.durable = file.cached.clone();
                }
            }
        }

        disk.appends.clear();
    }

    pub fn total_bytes(&self) -> usize {
        self.disk
            .lock()
            .expect("disk")
            .files
            .values()
            .map(|f| f.cached.len())
            .sum()
    }
}

impl Default for SimDisk {
    fn default() -> Self {
        SimDisk::new()
    }
}

impl Files for SimDisk {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let mut disk = self.disk.lock().expect("disk");
        if !disk.dirs.iter().any(|d| d == dir) {
            disk.dirs.push(dir.to_path_buf());
        }
        Ok(())
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let disk = self.disk.lock().expect("disk");
        Ok(disk
            .files
            .keys()
            .filter(|p| p.parent() == Some(dir))
            .cloned()
            .collect())
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn Blocks>> {
        let mut disk = self.disk.lock().expect("disk");
        disk.files.entry(path.to_path_buf()).or_default();
        Ok(Box::new(SimFile {
            disk: Arc::clone(&self.disk),
            path: path.to_path_buf(),
        }))
    }
}

struct SimFile {
    disk: Arc<Mutex<Disk>>,
    path: PathBuf,
}

impl Blocks for SimFile {
    fn size(&self) -> io::Result<u64> {
        let disk = self.disk.lock().expect("disk");
        Ok(disk.files[&self.path].cached.len() as u64)
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut disk = self.disk.lock().expect("disk");
        disk.files
            .get_mut(&self.path)
            .expect("file")
            .cached
            .extend_from_slice(bytes);
        disk.appends
            .entry(self.path.clone())
            .or_default()
            .push(bytes.len());
        Ok(())
    }

    fn read_at(&self, buf: &mut [u8], at: u64) -> io::Result<()> {
        let disk = self.disk.lock().expect("disk");
        let cached = &disk.files[&self.path].cached;
        let at = at as usize;

        if at + buf.len() > cached.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "read {} bytes at {at} of a {}-byte file",
                    buf.len(),
                    cached.len()
                ),
            ));
        }
        buf.copy_from_slice(&cached[at..at + buf.len()]);
        Ok(())
    }

    fn read_all(&self) -> io::Result<Vec<u8>> {
        let disk = self.disk.lock().expect("disk");
        Ok(disk.files[&self.path].cached.clone())
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        let mut disk = self.disk.lock().expect("disk");
        let file = disk.files.get_mut(&self.path).expect("file");
        file.cached.truncate(len as usize);
        file.durable.truncate(len as usize);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        let mut disk = self.disk.lock().expect("disk");
        let file = disk.files.get_mut(&self.path).expect("file");
        file.durable = file.cached.clone();
        disk.appends.remove(&self.path);
        Ok(())
    }
}

// --- the run ----------------------------------------------------------------

pub struct Outcome {
    pub seed: u64,
    pub crash: Crash,
    pub sync: Sync,
    /// Offsets the broker acknowledged before the crash.
    pub acknowledged: i64,
    /// Offsets readable after recovery.
    pub recovered: i64,
    pub truncated_bytes: u64,
    pub bytes_on_disk: usize,
}

#[derive(Debug)]
pub struct Violation {
    pub seed: u64,
    pub what: String,
}

/// Knobs the caller can pin instead of letting the seed choose them.
///
/// Leaving them unset is the right thing for hunting bugs: the seed picks, so
/// a few thousand runs cover combinations nobody thought to try. Pinning them
/// is for measuring, where the question is what one specific setting costs.
#[derive(Debug, Clone, Copy, Default)]
pub struct Knobs {
    pub sync: Option<Sync>,
    pub crash: Option<Crash>,
    pub cycles: u32,
}

/// One seed: a few rounds of appending, crashing and recovering.
///
/// Repeating the cycle is where it earns its keep. A single crash only tests
/// recovery against a log a healthy broker wrote; crashing again tests it
/// against a log that recovery itself produced.
pub fn run(seed: u64) -> Result<Outcome, Violation> {
    run_cycles(seed, 3)
}

pub fn run_cycles(seed: u64, cycles: u32) -> Result<Outcome, Violation> {
    run_with(
        seed,
        Knobs {
            cycles,
            ..Knobs::default()
        },
    )
}

pub fn run_with(seed: u64, knobs: Knobs) -> Result<Outcome, Violation> {
    let mut rng = Rng::new(seed);
    let cycles = if knobs.cycles == 0 { 3 } else { knobs.cycles };

    // Drawn from the rng even when pinned, so a seed means the same sequence
    // of appends either way and two configurations stay comparable.
    let drawn_sync = match rng.below(3) {
        0 => Sync::Always,
        1 => Sync::Never,
        _ => Sync::Interval(std::time::Duration::from_millis(rng.below(20))),
    };
    let drawn_crash = if rng.chance(50) {
        Crash::PowerLoss
    } else {
        Crash::ProcessKill
    };
    let sync = knobs.sync.unwrap_or(drawn_sync);
    let crash = knobs.crash.unwrap_or(drawn_crash);
    let cfg = Config {
        // Small, so segments roll often and recovery has to pick the right one.
        segment_bytes: 400 + rng.below(4000),
        index_interval_bytes: 1 + rng.below(512),
        sync,
    };

    let disk = SimDisk::new();
    let files: Arc<dyn Files> = Arc::new(disk.clone());
    let dir = PathBuf::from("sim/topic-0");

    let mut acknowledged = 0i64;
    // What was acknowledged just before the last crash, for reporting.
    let mut last_acknowledged = 0i64;
    let mut recovered = 0i64;
    let mut truncated_bytes = 0u64;
    // Offsets that survived the previous crash, so they are already durable.
    let mut survived_before = 0i64;

    for cycle in 0..cycles {
        let mut synced_through = survived_before;

        {
            let (mut log, _) = Log::open_with(&dir, cfg, Arc::clone(&files))
                .map_err(|e| Violation::new(seed, format!("cycle {cycle}: open failed: {e}")))?;

            let appends = 1 + rng.below(40);
            for _ in 0..appends {
                let records = 1 + rng.below(8) as i32;
                let mut blob = batch::fixtures::batch(0, records);

                match log.append(&mut blob) {
                    Ok(first) => {
                        acknowledged = first + records as i64;
                        if sync == Sync::Always {
                            synced_through = acknowledged;
                        }
                    }
                    Err(e) => {
                        return Err(Violation::new(
                            seed,
                            format!("cycle {cycle}: append failed: {e}"),
                        ));
                    }
                }

                // An explicit flush now and then, as the broker does on commit.
                if rng.chance(10) && log.sync().is_ok() {
                    synced_through = log.end_offset();
                }
            }

            disk.crash(crash, &mut rng);
        }

        let expected_end = acknowledged;
        last_acknowledged = expected_end;
        let (log, recovery) = Log::open_with(&dir, cfg, Arc::clone(&files)).map_err(|e| {
            Violation::new(seed, format!("cycle {cycle}: recovery could not open: {e}"))
        })?;
        truncated_bytes += recovery.truncated_bytes;
        recovered = log.end_offset();

        if recovered > expected_end {
            return Err(Violation::new(
                seed,
                format!(
                    "cycle {cycle}: recovered offset {recovered} is past the acknowledged end {expected_end}"
                ),
            ));
        }
        if recovered < synced_through {
            return Err(Violation::new(
                seed,
                format!(
                    "cycle {cycle}: lost synced data, recovered {recovered} but synced through {synced_through}"
                ),
            ));
        }
        // Data that survived one crash must survive the next.
        if recovered < survived_before {
            return Err(Violation::new(
                seed,
                format!(
                    "cycle {cycle}: went backwards, recovered {recovered} after {survived_before} had already survived"
                ),
            ));
        }
        if crash == Crash::ProcessKill && recovered < expected_end {
            // The page cache outlives the process, so only the append that was
            // still being issued can be missing.
            if expected_end - recovered > 8 {
                return Err(Violation::new(
                    seed,
                    format!(
                        "cycle {cycle}: a process kill lost {} offsets, more than one append's worth",
                        expected_end - recovered
                    ),
                ));
            }
        }

        check_contiguous(seed, cycle, &log, recovered)?;
        check_every_offset_is_reachable(seed, cycle, &log, recovered)?;

        // The next cycle appends from wherever recovery left the log.
        acknowledged = recovered;
        survived_before = recovered;
    }

    Ok(Outcome {
        seed,
        crash,
        sync,
        acknowledged: last_acknowledged,
        recovered,
        truncated_bytes,
        bytes_on_disk: disk.total_bytes(),
    })
}

/// Everything readable has to be an unbroken run of offsets from the start,
/// with every checksum intact.
fn check_contiguous(seed: u64, cycle: u32, log: &Log, end: i64) -> Result<(), Violation> {
    let mut offset = log.start_offset();

    while offset < end {
        let bytes = log.read(offset, 1 << 20).map_err(|e| {
            Violation::new(seed, format!("cycle {cycle}: read at {offset} failed: {e}"))
        })?;
        if bytes.is_empty() {
            return Err(Violation::new(
                seed,
                format!("cycle {cycle}: read at {offset} returned nothing, log ends at {end}"),
            ));
        }

        // parse_all verifies every checksum, so a torn batch is caught here.
        let batches = batch::parse_all(&bytes).map_err(|e| {
            Violation::new(
                seed,
                format!("cycle {cycle}: corrupt batch at {offset}: {e}"),
            )
        })?;

        for parsed in &batches {
            if parsed.base_offset != offset {
                return Err(Violation::new(
                    seed,
                    format!(
                        "cycle {cycle}: offset gap, expected {offset} but batch starts at {}",
                        parsed.base_offset
                    ),
                ));
            }
            offset += parsed.offsets();
        }
    }

    Ok(())
}

/// Every offset must be findable through the sparse index. A stale index entry
/// left pointing past a truncation would show up here and nowhere else.
fn check_every_offset_is_reachable(
    seed: u64,
    cycle: u32,
    log: &Log,
    end: i64,
) -> Result<(), Violation> {
    for offset in log.start_offset()..end {
        let bytes = log.read(offset, 1 << 20).map_err(|e| {
            Violation::new(seed, format!("cycle {cycle}: seek to {offset} failed: {e}"))
        })?;
        let batches = batch::parse_all(&bytes).map_err(|e| {
            Violation::new(
                seed,
                format!("cycle {cycle}: bad batch seeking {offset}: {e}"),
            )
        })?;

        let Some(first) = batches.first() else {
            return Err(Violation::new(
                seed,
                format!("cycle {cycle}: offset {offset} is not readable, log ends at {end}"),
            ));
        };
        let covers = first.base_offset <= offset
            && offset <= first.base_offset + first.last_offset_delta as i64;
        if !covers {
            return Err(Violation::new(
                seed,
                format!(
                    "cycle {cycle}: asked for {offset}, got a batch covering {}..{}",
                    first.base_offset,
                    first.base_offset + first.last_offset_delta as i64
                ),
            ));
        }
    }

    Ok(())
}

impl Violation {
    fn new(seed: u64, what: String) -> Self {
        Violation { seed, what }
    }
}

pub fn describe(sync: Sync) -> &'static str {
    match sync {
        Sync::Always => "always",
        Sync::Never => "never",
        Sync::EveryN(_) => "every-n",
        Sync::Interval(_) => "interval",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_reproduces_itself() {
        let a = run(12345).expect("seed 12345");
        let b = run(12345).expect("seed 12345 again");

        assert_eq!(a.crash, b.crash);
        assert_eq!(a.acknowledged, b.acknowledged);
        assert_eq!(a.recovered, b.recovered);
        assert_eq!(a.bytes_on_disk, b.bytes_on_disk);
    }

    #[test]
    fn different_seeds_do_different_things() {
        let outcomes: Vec<_> = (1..40).map(|s| run(s).expect("clean run")).collect();
        let ends: std::collections::HashSet<i64> = outcomes.iter().map(|o| o.recovered).collect();
        assert!(ends.len() > 5, "seeds all produced the same log length");
    }

    #[test]
    fn power_loss_actually_tears_batches() {
        // If nothing is ever truncated the model is not exercising recovery.
        let torn = (1..200u64)
            .filter_map(|s| run(s).ok())
            .filter(|o| o.truncated_bytes > 0)
            .count();
        assert!(torn > 0, "no run produced a partial batch to recover from");
    }

    #[test]
    fn fsync_always_never_loses_an_acknowledged_record() {
        for seed in 1..400u64 {
            let Ok(outcome) = run(seed) else { continue };
            if outcome.sync == Sync::Always {
                assert_eq!(
                    outcome.recovered, outcome.acknowledged,
                    "seed {seed}: fsync on every append still lost data"
                );
            }
        }
    }

    #[test]
    fn the_invariant_holds_across_many_seeds() {
        for seed in 1..500u64 {
            if let Err(v) = run(seed) {
                panic!("seed {}: {}", v.seed, v.what);
            }
        }
    }
}
