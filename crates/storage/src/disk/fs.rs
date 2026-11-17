//! The files a database lives in, behind a trait so that tests can run a
//! database on files that lose what was never synced when they crash.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
#[cfg(not(unix))]
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Opens, reads and replaces files. `OsFs` is the real file system.
pub trait Fs: Send + Sync {
    /// Opens `path` for reading and writing, creating it empty if there is
    /// no such file.
    fn open(&self, path: &Path) -> io::Result<Box<dyn FsFile>>;

    /// The contents of `path`, or `None` if there is no such file.
    fn read(&self, path: &Path) -> io::Result<Option<Vec<u8>>>;

    /// Replaces the contents of `path` with `bytes` in one durable step: a
    /// crash leaves either the old file or the new one.
    fn replace(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;

    /// Deletes `path`, if there is such a file, in one durable step: once
    /// this returns, a crash never brings it back.
    fn remove(&self, path: &Path) -> io::Result<()>;

    /// The names of the files in `dir`, the current directory if empty.
    fn list(&self, dir: &Path) -> io::Result<Vec<OsString>>;
}

/// A file opened by an `Fs`, read and written at offsets.
pub trait FsFile: Send + Sync {
    /// Reads at `offset` until `buf` is full or the file ends, and returns
    /// the number of bytes read.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()>;

    fn size(&self) -> io::Result<u64>;

    fn set_size(&self, size: u64) -> io::Result<()>;

    /// Makes every write and size change so far durable.
    fn sync(&self) -> io::Result<()>;

    /// Locks the file for this handle until it is closed. Returns `false`
    /// if another handle holds the lock.
    fn try_lock(&self) -> io::Result<bool>;
}

/// The operating system's file system.
pub struct OsFs;

impl Fs for OsFs {
    fn open(&self, path: &Path) -> io::Result<Box<dyn FsFile>> {
        let existed = path.try_exists()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        if !existed {
            sync_parent_dir(path)?;
        }
        Ok(Box::new(OsFile(file)))
    }

    fn read(&self, path: &Path) -> io::Result<Option<Vec<u8>>> {
        match fs::read(path) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn replace(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let tmp_path = with_suffix(path, ".tmp");
        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(bytes)?;
        tmp.sync_all()?;
        drop(tmp);
        fs::rename(&tmp_path, path)?;
        sync_parent_dir(path)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        sync_parent_dir(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<OsString>> {
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        fs::read_dir(dir)?
            .map(|entry| Ok(entry?.file_name()))
            .collect()
    }
}

struct OsFile(File);

impl FsFile for OsFile {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        read_full_at(&self.0, buf, offset)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        write_all_at(&self.0, buf, offset)
    }

    fn size(&self) -> io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }

    fn set_size(&self, size: u64) -> io::Result<()> {
        self.0.set_len(size)
    }

    fn sync(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    fn try_lock(&self) -> io::Result<bool> {
        match self.0.try_lock() {
            Ok(()) => Ok(true),
            Err(TryLockError::WouldBlock) => Ok(false),
            Err(TryLockError::Error(e)) => Err(e),
        }
    }
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

/// Makes a file created or renamed inside the directory durable.
#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => File::open(dir)?.sync_all(),
        _ => File::open(".")?.sync_all(),
    }
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Writes all of `buf` at `offset` in one positioned write (`pwrite`),
/// without moving the file's cursor.
#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

/// Moving the shared cursor and then writing is only safe one at a time.
#[cfg(not(unix))]
static CURSOR: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[cfg(not(unix))]
fn write_all_at(mut file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let _cursor = CURSOR.lock();
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)
}

/// Reads at `offset` until `buf` is full or EOF, with positioned reads
/// (`pread`), returning the number of bytes read.
#[cfg(unix)]
fn read_full_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    let mut n = 0;
    while n < buf.len() {
        match file.read_at(&mut buf[n..], offset + n as u64) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

#[cfg(not(unix))]
fn read_full_at(mut file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let _cursor = CURSOR.lock();
    file.seek(SeekFrom::Start(offset))?;
    let mut n = 0;
    while n < buf.len() {
        match file.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}
