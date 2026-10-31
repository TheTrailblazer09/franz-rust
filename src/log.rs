//! The commit log for one partition: a directory of append-only segments.
//!
//! ```text
//! demo-0/
//!   00000000000000000000.log     record batches, appended
//!   00000000000000000000.index   sparse offset -> byte position
//!   00000000000000016384.log     rolled at segment_bytes
//!   00000000000000016384.index
//! ```
//!
//! Only the last segment is written to. Everything before it is immutable,
//! which is what makes a reader safe without coordinating with the writer.
//!
//! The index holds one 8-byte entry per `index_interval_bytes` of log, not one
//! per record. Offsets are stored relative to the segment's base offset so
//! they fit in four bytes, which is the only reason the base offset is encoded
//! in the filename. A lookup binary-searches the entries, seeks to the byte
//! position, and scans batch headers forward; the scan is bounded by the index
//! interval rather than by the size of the log.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::batch;

/// Filenames are the base offset, zero-padded, so lexical order is numeric.
const NAME_WIDTH: usize = 20;

/// One index entry: relative offset and byte position, both u32 big-endian.
const INDEX_ENTRY: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sync {
    /// fsync before acknowledging. Survives power loss.
    Always,
    /// fsync at most this often. Loses up to one interval on power loss.
    Interval(Duration),
    /// Leave it to the page cache. Survives a process crash, not power loss.
    Never,
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub segment_bytes: u64,
    pub index_interval_bytes: u64,
    pub sync: Sync,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            segment_bytes: 64 * 1024 * 1024,
            index_interval_bytes: 4096,
            sync: Sync::Never,
        }
    }
}

struct Segment {
    base_offset: i64,
    log: File,
    index: File,
    /// Bytes written to the log file.
    size: u64,
    /// Bytes appended since the last index entry.
    since_index: u64,
    entries: Vec<(u32, u32)>,
}

impl Segment {
    fn open(dir: &Path, base_offset: i64) -> io::Result<Self> {
        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(dir.join(format!("{base_offset:0NAME_WIDTH$}.log")))?;
        let index = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(dir.join(format!("{base_offset:0NAME_WIDTH$}.index")))?;

        let size = log.metadata()?.len();
        Ok(Segment {
            base_offset,
            log,
            index,
            size,
            since_index: 0,
            entries: Vec::new(),
        })
    }

    fn load_index(&mut self) -> io::Result<()> {
        let mut bytes = Vec::new();
        self.index.seek(SeekFrom::Start(0))?;
        self.index.read_to_end(&mut bytes)?;

        self.entries = bytes
            .as_chunks::<INDEX_ENTRY>()
            .0
            .iter()
            .map(|e| {
                let offset = u32::from_be_bytes([e[0], e[1], e[2], e[3]]);
                let position = u32::from_be_bytes([e[4], e[5], e[6], e[7]]);
                (offset, position)
            })
            .collect();
        Ok(())
    }

    fn add_index_entry(&mut self, offset: i64, position: u64) -> io::Result<()> {
        let relative = (offset - self.base_offset) as u32;
        let position = position as u32;

        self.index.write_all(&relative.to_be_bytes())?;
        self.index.write_all(&position.to_be_bytes())?;
        self.entries.push((relative, position));
        self.since_index = 0;
        Ok(())
    }

    /// Byte position to start scanning from for `offset`.
    fn seek_position(&self, offset: i64) -> u64 {
        let relative = (offset - self.base_offset) as u32;
        match self.entries.binary_search_by_key(&relative, |e| e.0) {
            Ok(i) => self.entries[i].1 as u64,
            // Insertion point, so the entry before it is the largest that is
            // still at or below the offset we want.
            Err(0) => 0,
            Err(i) => self.entries[i - 1].1 as u64,
        }
    }

    /// Rebuilds the index from the log, used after recovery truncates it.
    fn rebuild_index(&mut self, batches: &[(i64, u64)], interval: u64) -> io::Result<()> {
        self.index.set_len(0)?;
        self.index.seek(SeekFrom::End(0))?;
        self.entries.clear();
        self.since_index = 0;

        let mut last_indexed = None;
        for (offset, position) in batches {
            let far_enough = match last_indexed {
                None => true,
                Some(previous) => position - previous >= interval,
            };
            if far_enough {
                self.add_index_entry(*offset, *position)?;
                last_indexed = Some(*position);
            }
        }
        self.index.sync_all()
    }
}

pub struct Log {
    dir: PathBuf,
    cfg: Config,
    segments: Vec<Segment>,
    next_offset: i64,
    last_sync: Instant,
    dirty: bool,
}

/// What recovery found when the log was opened.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    pub segments: usize,
    pub end_offset: i64,
    /// Bytes dropped from the tail because they were not a complete batch.
    pub truncated_bytes: u64,
}

impl Log {
    pub fn open(dir: impl Into<PathBuf>, cfg: Config) -> io::Result<(Self, Recovery)> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;

        let mut bases = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "log") {
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                match stem.parse::<i64>() {
                    Ok(base) => bases.push(base),
                    Err(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("{} is not named for a base offset", path.display()),
                        ));
                    }
                }
            }
        }
        bases.sort_unstable();
        if bases.is_empty() {
            bases.push(0);
        }

        let mut segments = Vec::new();
        for base in &bases {
            let mut segment = Segment::open(&dir, *base)?;
            segment.load_index()?;
            segments.push(segment);
        }

        let mut log = Log {
            dir,
            cfg,
            segments,
            next_offset: 0,
            last_sync: Instant::now(),
            dirty: false,
        };

        // Only the active segment can end mid-write. Earlier ones were sealed
        // by a roll, which happens after the bytes are already on their way to
        // disk, so they are trusted without a scan.
        let truncated_bytes = log.recover_active()?;

        let recovery = Recovery {
            segments: log.segments.len(),
            end_offset: log.next_offset,
            truncated_bytes,
        };
        Ok((log, recovery))
    }

    /// Scans the active segment, drops any trailing partial batch, and rebuilds
    /// that segment's index from what survived.
    fn recover_active(&mut self) -> io::Result<u64> {
        let interval = self.cfg.index_interval_bytes;
        let active = self.segments.last_mut().expect("at least one segment");

        let mut bytes = Vec::with_capacity(active.size as usize);
        active.log.seek(SeekFrom::Start(0))?;
        active.log.read_to_end(&mut bytes)?;

        let mut batches = Vec::new();
        let mut at = 0usize;
        let mut next_offset = active.base_offset;

        while at < bytes.len() {
            match batch::parse(&bytes[at..]) {
                Ok(parsed) => {
                    batches.push((parsed.base_offset, at as u64));
                    next_offset = parsed.base_offset + parsed.offsets();
                    at += parsed.total_len;
                }
                // Anything that does not parse is the tail of an interrupted
                // write. Everything before it is known good.
                Err(_) => break,
            }
        }

        let truncated_bytes = bytes.len() as u64 - at as u64;
        if truncated_bytes > 0 {
            active.log.set_len(at as u64)?;
            active.log.seek(SeekFrom::End(0))?;
            active.log.sync_all()?;
            active.size = at as u64;
        }

        active.rebuild_index(&batches, interval)?;
        self.next_offset = next_offset;
        Ok(truncated_bytes)
    }

    /// First offset still on disk.
    pub fn start_offset(&self) -> i64 {
        self.segments[0].base_offset
    }

    /// Offset the next append will land at.
    pub fn end_offset(&self) -> i64 {
        self.next_offset
    }

    /// Validates every batch in `blob`, stamps the assigned offsets into their
    /// headers, and appends it. Returns the offset of the first record.
    ///
    /// Either the whole blob is accepted or none of it is: a batch that fails
    /// its checksum rejects the request rather than storing part of it.
    pub fn append(&mut self, blob: &mut [u8]) -> Result<i64, AppendError> {
        let batches = batch::parse_all(blob).map_err(AppendError::Batch)?;
        if batches.is_empty() {
            return Ok(self.next_offset);
        }

        let first_offset = self.next_offset;
        let mut offset = first_offset;
        let mut at = 0usize;
        let mut positions = Vec::with_capacity(batches.len());

        for parsed in &batches {
            batch::stamp_base_offset(&mut blob[at..], offset);
            positions.push((offset, at as u64));
            offset += parsed.offsets();
            at += parsed.total_len;
        }

        self.roll_if_needed(blob.len() as u64)?;

        let active = self.segments.last_mut().expect("at least one segment");
        let start = active.size;
        active.log.write_all(blob)?;
        active.size += blob.len() as u64;
        active.since_index += blob.len() as u64;

        // One entry per interval of log written, not one per batch.
        if active.since_index >= self.cfg.index_interval_bytes {
            let (batch_offset, batch_position) = positions[0];
            active.add_index_entry(batch_offset, start + batch_position)?;
        }

        self.next_offset = offset;
        self.dirty = true;
        self.sync_if_needed()?;

        Ok(first_offset)
    }

    /// A batch is never split across segments. If the incoming bytes do not
    /// fit, the segment is sealed first and they go whole into the next one.
    fn roll_if_needed(&mut self, incoming: u64) -> io::Result<()> {
        let active = self.segments.last_mut().expect("at least one segment");
        if active.size == 0 || active.size + incoming <= self.cfg.segment_bytes {
            return Ok(());
        }

        active.log.sync_all()?;
        active.index.sync_all()?;

        let segment = Segment::open(&self.dir, self.next_offset)?;
        self.segments.push(segment);
        Ok(())
    }

    fn sync_if_needed(&mut self) -> io::Result<()> {
        let due = match self.cfg.sync {
            Sync::Always => true,
            Sync::Interval(every) => self.last_sync.elapsed() >= every,
            Sync::Never => false,
        };
        if due {
            self.sync()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let active = self.segments.last().expect("at least one segment");
        active.log.sync_all()?;
        active.index.sync_all()?;
        self.last_sync = Instant::now();
        self.dirty = false;
        Ok(())
    }

    /// Whole batches starting at or covering `offset`, up to about `max_bytes`.
    ///
    /// Reads from a single segment. A client that wants more asks again at the
    /// offset it reached, which is what every Kafka consumer already does.
    /// At least one batch comes back even if it alone exceeds `max_bytes`, or a
    /// consumer whose fetch size is smaller than one batch would stall forever.
    pub fn read(&self, offset: i64, max_bytes: usize) -> io::Result<Vec<u8>> {
        if offset >= self.next_offset {
            return Ok(Vec::new());
        }

        let index = match self.segments.iter().rposition(|s| s.base_offset <= offset) {
            Some(index) => index,
            None => return Ok(Vec::new()),
        };
        let segment = &self.segments[index];

        let mut out = Vec::new();
        let mut at = segment.seek_position(offset);
        let mut header = [0u8; batch::HEADER_LEN];

        while at < segment.size {
            if segment.log.read_exact_at(&mut header, at).is_err() {
                break;
            }
            let parsed = match batch::parse_header(&header) {
                Ok(parsed) => parsed,
                // Only possible on the active segment mid-append; the bytes
                // are not ours to serve yet.
                Err(batch::Error::Truncated { .. }) => break,
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{}/{:0NAME_WIDTH$}.log is corrupt at byte {at}: {e}",
                            self.dir.display(),
                            segment.base_offset
                        ),
                    ));
                }
            };

            let covers = parsed.base_offset + parsed.last_offset_delta as i64 >= offset;
            if covers {
                if !out.is_empty() && out.len() + parsed.total_len > max_bytes {
                    break;
                }
                let mut whole = vec![0u8; parsed.total_len];
                segment.log.read_exact_at(&mut whole, at)?;
                out.extend_from_slice(&whole);
                if out.len() >= max_bytes {
                    break;
                }
            }
            at += parsed.total_len as u64;
        }

        Ok(out)
    }
}

#[derive(Debug)]
pub enum AppendError {
    Batch(batch::Error),
    Io(io::Error),
}

impl From<io::Error> for AppendError {
    fn from(e: io::Error) -> Self {
        AppendError::Io(e)
    }
}

impl std::error::Error for AppendError {}

impl std::fmt::Display for AppendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppendError::Batch(e) => write!(f, "{e}"),
            AppendError::Io(e) => write!(f, "{e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::fixtures::batch;

    struct Dir(PathBuf);

    impl Dir {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("franz-test-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            Dir(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open(dir: &Dir, cfg: Config) -> Log {
        Log::open(&dir.0, cfg).unwrap().0
    }

    fn append(log: &mut Log, records: i32) -> i64 {
        let mut blob = batch(0, records);
        log.append(&mut blob).unwrap()
    }

    #[test]
    fn offsets_are_assigned_in_order() {
        let dir = Dir::new("offsets");
        let mut log = open(&dir, Config::default());

        assert_eq!(append(&mut log, 3), 0);
        assert_eq!(append(&mut log, 2), 3);
        assert_eq!(append(&mut log, 1), 5);
        assert_eq!(log.end_offset(), 6);
        assert_eq!(log.start_offset(), 0);
    }

    #[test]
    fn a_read_returns_the_batch_covering_the_offset() {
        let dir = Dir::new("read");
        let mut log = open(&dir, Config::default());
        append(&mut log, 3);
        append(&mut log, 3);

        // Offset 1 is inside the first batch, so that whole batch comes back.
        let bytes = log.read(1, 1 << 20).unwrap();
        let batches = batch::parse_all(&bytes).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].base_offset, 0);
        assert_eq!(batches[1].base_offset, 3);

        // Offset 3 starts the second batch, so the first is skipped.
        let bytes = log.read(3, 1 << 20).unwrap();
        let batches = batch::parse_all(&bytes).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].base_offset, 3);

        assert!(
            log.read(6, 1 << 20).unwrap().is_empty(),
            "nothing yet at the end"
        );
    }

    #[test]
    fn max_bytes_always_yields_at_least_one_batch() {
        let dir = Dir::new("maxbytes");
        let mut log = open(&dir, Config::default());
        append(&mut log, 5);
        append(&mut log, 5);

        let bytes = log.read(0, 1).unwrap();
        assert_eq!(batch::parse_all(&bytes).unwrap().len(), 1);
    }

    #[test]
    fn segments_roll_and_reads_still_find_their_offset() {
        let dir = Dir::new("roll");
        let cfg = Config {
            segment_bytes: 200,
            index_interval_bytes: 1,
            ..Config::default()
        };
        let mut log = open(&dir, cfg);

        for _ in 0..20 {
            append(&mut log, 2);
        }
        assert_eq!(log.end_offset(), 40);

        let logs = std::fs::read_dir(&dir.0)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "log")
            })
            .count();
        assert!(logs > 1, "expected a roll, got {logs} segment(s)");

        for offset in 0..40 {
            let bytes = log.read(offset, 1 << 20).unwrap();
            let first = batch::parse_all(&bytes).unwrap()[0];
            assert!(
                first.base_offset <= offset
                    && offset <= first.base_offset + first.last_offset_delta as i64,
                "offset {offset} came back in a batch starting at {}",
                first.base_offset
            );
        }
    }

    #[test]
    fn reopening_resumes_where_it_left_off() {
        let dir = Dir::new("reopen");
        {
            let mut log = open(&dir, Config::default());
            append(&mut log, 4);
            append(&mut log, 4);
            log.sync().unwrap();
        }

        let (log, recovery) = Log::open(&dir.0, Config::default()).unwrap();
        assert_eq!(recovery.end_offset, 8);
        assert_eq!(recovery.truncated_bytes, 0);
        assert_eq!(log.end_offset(), 8);
        assert_eq!(
            batch::parse_all(&log.read(0, 1 << 20).unwrap())
                .unwrap()
                .len(),
            2
        );
    }

    /// The case a crash actually produces: the last batch is half on disk.
    #[test]
    fn a_half_written_tail_is_truncated_and_the_rest_survives() {
        let dir = Dir::new("torn");
        let good_bytes;
        {
            let mut log = open(&dir, Config::default());
            append(&mut log, 4);
            append(&mut log, 4);
            log.sync().unwrap();
            good_bytes = std::fs::metadata(dir.0.join(format!("{:020}.log", 0)))
                .unwrap()
                .len();
        }

        // Append half of a third batch, as an interrupted write would leave.
        let partial = batch(0, 4);
        let half = &partial[..partial.len() / 2];
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.0.join(format!("{:020}.log", 0)))
                .unwrap();
            file.write_all(half).unwrap();
            file.sync_all().unwrap();
        }

        let (mut log, recovery) = Log::open(&dir.0, Config::default()).unwrap();
        assert_eq!(recovery.truncated_bytes, half.len() as u64);
        assert_eq!(recovery.end_offset, 8, "the acknowledged records survive");
        assert_eq!(
            std::fs::metadata(dir.0.join(format!("{:020}.log", 0)))
                .unwrap()
                .len(),
            good_bytes,
            "the torn tail is gone from the file"
        );

        // And the log is usable afterwards, appending at the recovered offset.
        assert_eq!(append(&mut log, 1), 8);
        assert_eq!(
            batch::parse_all(&log.read(0, 1 << 20).unwrap())
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn a_corrupt_batch_is_rejected_without_being_stored() {
        let dir = Dir::new("corrupt");
        let mut log = open(&dir, Config::default());
        append(&mut log, 2);

        let mut bad = batch(0, 2);
        let last = bad.len() - 1;
        bad[last] ^= 0xff;

        assert!(log.append(&mut bad).is_err());
        assert_eq!(log.end_offset(), 2, "the bad batch did not advance the log");
    }

    #[test]
    fn the_index_stays_sparse() {
        let dir = Dir::new("sparse");
        let cfg = Config {
            index_interval_bytes: 4096,
            ..Config::default()
        };
        let mut log = open(&dir, cfg);
        for _ in 0..200 {
            append(&mut log, 1);
        }

        let log_len = std::fs::metadata(dir.0.join(format!("{:020}.log", 0)))
            .unwrap()
            .len();
        let index_len = std::fs::metadata(dir.0.join(format!("{:020}.index", 0)))
            .unwrap()
            .len();
        assert!(
            index_len * 20 < log_len,
            "index {index_len} bytes against log {log_len} bytes is not sparse"
        );
    }
}
