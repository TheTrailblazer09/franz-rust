//! The seam between the log and the disk.
//!
//! Every byte the commit log reads or writes goes through [`Blocks`], and every
//! file it opens through [`Files`]. In the broker these are thin wrappers over
//! `std::fs`. In the simulator they are an in-memory model that can lose
//! unsynced data, tear a write in half, and be asked to do so from a seed.
//!
//! The seam exists for that second implementation. A `SIGKILL` cannot produce a
//! torn write — a `write` to a regular file is not interrupted partway by a
//! signal — so the failure that worries you most is the one a real crash test
//! cannot reach.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

/// An append-only file with positional reads.
pub trait Blocks: Send {
    /// Bytes a reader can see, synced or not.
    fn size(&self) -> io::Result<u64>;

    /// Appends all of `bytes`, or fails. Partial appends are not a thing a
    /// caller can observe: what a crash exposes is a partial *persist*.
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;

    fn read_at(&self, buf: &mut [u8], at: u64) -> io::Result<()>;

    fn read_all(&self) -> io::Result<Vec<u8>>;

    fn truncate(&mut self, len: u64) -> io::Result<()>;

    fn sync(&mut self) -> io::Result<()>;
}

pub trait Files: Send + Sync {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;

    /// Every path directly inside `dir`.
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;

    fn open(&self, path: &Path) -> io::Result<Box<dyn Blocks>>;
}

// --- the real thing ---------------------------------------------------------

pub struct OnDisk;

impl Files for OnDisk {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            out.push(entry?.path());
        }
        Ok(out)
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn Blocks>> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Box::new(DiskFile { file }))
    }
}

struct DiskFile {
    file: File,
}

impl Blocks for DiskFile {
    fn size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn read_at(&self, buf: &mut [u8], at: u64) -> io::Result<()> {
        self.file.read_exact_at(buf, at)
    }

    fn read_all(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut handle = &self.file;
        handle.seek(SeekFrom::Start(0))?;
        handle.read_to_end(&mut out)?;
        Ok(out)
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.file.seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}
