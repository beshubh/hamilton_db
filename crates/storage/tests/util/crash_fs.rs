//! An in-memory `Fs` that records every write, size change, sync and
//! replace, so a test can build the files a crash after any of them would
//! leave behind: what was synced, plus some, all or none of what was not.

use std::collections::{BTreeMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use storage::disk::fs::{Fs, FsFile};

#[derive(Clone, Debug)]
enum Event {
    Write {
        path: PathBuf,
        offset: u64,
        bytes: Vec<u8>,
    },
    SetSize {
        path: PathBuf,
        size: u64,
    },
    Sync {
        path: PathBuf,
    },
    Replace {
        path: PathBuf,
        bytes: Vec<u8>,
    },
}

/// What a crash does with each write not synced yet.
#[derive(Clone, Copy, Debug)]
pub enum Lost {
    /// Every write reached the disk, as when only the process dies.
    Nothing,
    /// No write reached the disk.
    Everything,
    /// Each write reached the disk whole, in part, or not at all.
    Some(u64),
}

/// The files as one moment left them: synced contents and the writes since.
#[derive(Clone, Default)]
struct Files {
    files: BTreeMap<PathBuf, File>,
}

#[derive(Clone, Default)]
struct File {
    /// What reads see.
    current: Vec<u8>,
    /// What the last sync made durable.
    durable: Vec<u8>,
    /// Writes since the last sync, oldest first.
    unsynced: Vec<(u64, Vec<u8>)>,
}

impl Files {
    fn apply(&mut self, event: &Event) {
        match event {
            Event::Write {
                path,
                offset,
                bytes,
            } => {
                let file = self.files.entry(path.clone()).or_default();
                write_at(&mut file.current, *offset, bytes);
                file.unsynced.push((*offset, bytes.clone()));
            }
            Event::SetSize { path, size } => {
                let file = self.files.entry(path.clone()).or_default();
                file.current.resize(*size as usize, 0);
            }
            Event::Sync { path } => {
                let file = self.files.entry(path.clone()).or_default();
                file.durable = file.current.clone();
                file.unsynced.clear();
            }
            Event::Replace { path, bytes } => {
                let file = self.files.entry(path.clone()).or_default();
                file.current = bytes.clone();
                file.durable = bytes.clone();
                file.unsynced.clear();
            }
        }
    }

    /// The files a crash would leave: each one's durable contents with the
    /// writes since applied as `lost` says. A size change not synced is lost,
    /// but a write that lands past the end makes the file longer.
    fn crash(&self, lost: Lost) -> Self {
        let mut rng = match lost {
            Lost::Some(seed) => seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1,
            _ => 1,
        };
        let files = self
            .files
            .iter()
            .map(|(path, file)| {
                let mut bytes = file.durable.clone();
                for (offset, write) in &file.unsynced {
                    let kept = match lost {
                        Lost::Nothing => write.len(),
                        Lost::Everything => 0,
                        Lost::Some(_) => match next(&mut rng) % 3 {
                            0 => write.len(),
                            1 => 0,
                            _ => next(&mut rng) as usize % write.len().max(1),
                        },
                    };
                    write_at(&mut bytes, *offset, &write[..kept]);
                }
                let file = File {
                    current: bytes.clone(),
                    durable: bytes,
                    unsynced: Vec::new(),
                };
                (path.clone(), file)
            })
            .collect();
        Self { files }
    }
}

/// The files as they were after some event, for `for_each_moment`.
pub struct Moment<'a>(&'a Files);

impl Moment<'_> {
    /// A new fs holding the files a crash at this moment would leave.
    pub fn crash(&self, lost: Lost) -> CrashFs {
        CrashFs::from(self.0.crash(lost))
    }
}

/// An `Fs` whose files live in memory and record what happens to them.
#[derive(Clone)]
pub struct CrashFs(Arc<Shared>);

struct Shared {
    state: Mutex<State>,
}

struct State {
    /// The files when this fs was made.
    start: Files,
    now: Files,
    /// Everything since `start`, in order.
    events: Vec<Event>,
    locked: HashSet<PathBuf>,
}

impl CrashFs {
    pub fn new() -> Self {
        Self::from(Files::default())
    }

    fn from(files: Files) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                start: files.clone(),
                now: files,
                events: Vec::new(),
                locked: HashSet::new(),
            }),
        }))
    }

    /// Writes, size changes, syncs and replaces so far.
    pub fn num_events(&self) -> usize {
        self.0.state.lock().events.len()
    }

    /// A new fs holding the files a crash right after the first `n` events
    /// would leave, recording from there.
    pub fn crash_after(&self, n: usize, lost: Lost) -> CrashFs {
        let state = self.0.state.lock();
        let mut files = state.start.clone();
        for event in &state.events[..n] {
            files.apply(event);
        }
        Self::from(files.crash(lost))
    }

    /// Calls `f` with each moment since this fs was made, in order: before
    /// the first event, then after each.
    pub fn for_each_moment(&self, mut f: impl FnMut(usize, Moment<'_>)) {
        let (mut files, events) = {
            let state = self.0.state.lock();
            (state.start.clone(), state.events.clone())
        };
        f(0, Moment(&files));
        for (n, event) in events.iter().enumerate() {
            files.apply(event);
            f(n + 1, Moment(&files));
        }
    }

    /// A new fs holding the files as they are now, as a crash that loses
    /// nothing would leave them.
    pub fn crash_now(&self) -> CrashFs {
        self.crash_after(self.num_events(), Lost::Nothing)
    }

    /// The contents of `path` now.
    pub fn contents(&self, path: &Path) -> Vec<u8> {
        let state = self.0.state.lock();
        state
            .now
            .files
            .get(path)
            .map_or_else(Vec::new, |file| file.current.clone())
    }

    /// A copy of this fs where `path` holds `bytes`, synced.
    pub fn with_contents(&self, path: &Path, bytes: &[u8]) -> CrashFs {
        let fs = self.crash_now();
        fs.record(Event::Replace {
            path: path.to_owned(),
            bytes: bytes.to_vec(),
        });
        fs.crash_now()
    }

    /// A copy of this fs without the file at `path`.
    pub fn without(&self, path: &Path) -> CrashFs {
        let fs = self.crash_now();
        let mut state = fs.0.state.lock();
        state.now.files.remove(path);
        state.start = state.now.clone();
        drop(state);
        fs
    }

    fn record(&self, event: Event) {
        let mut state = self.0.state.lock();
        state.now.apply(&event);
        state.events.push(event);
    }
}

impl Fs for CrashFs {
    fn open(&self, path: &Path) -> io::Result<Box<dyn FsFile>> {
        self.0
            .state
            .lock()
            .now
            .files
            .entry(path.to_owned())
            .or_default();
        Ok(Box::new(Handle {
            fs: self.clone(),
            path: path.to_owned(),
            locked: Mutex::new(false),
        }))
    }

    fn read(&self, path: &Path) -> io::Result<Option<Vec<u8>>> {
        let state = self.0.state.lock();
        Ok(state.now.files.get(path).map(|file| file.current.clone()))
    }

    fn replace(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.record(Event::Replace {
            path: path.to_owned(),
            bytes: bytes.to_vec(),
        });
        Ok(())
    }
}

struct Handle {
    fs: CrashFs,
    path: PathBuf,
    locked: Mutex<bool>,
}

impl FsFile for Handle {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        let state = self.fs.0.state.lock();
        let data = &state.now.files[&self.path].current;
        let start = (offset as usize).min(data.len());
        let n = buf.len().min(data.len() - start);
        buf[..n].copy_from_slice(&data[start..start + n]);
        Ok(n)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.fs.record(Event::Write {
            path: self.path.clone(),
            offset,
            bytes: buf.to_vec(),
        });
        Ok(())
    }

    fn size(&self) -> io::Result<u64> {
        let state = self.fs.0.state.lock();
        Ok(state.now.files[&self.path].current.len() as u64)
    }

    fn set_size(&self, size: u64) -> io::Result<()> {
        self.fs.record(Event::SetSize {
            path: self.path.clone(),
            size,
        });
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.fs.record(Event::Sync {
            path: self.path.clone(),
        });
        Ok(())
    }

    fn try_lock(&self) -> io::Result<bool> {
        let mut locked = self.locked.lock();
        if !*locked {
            *locked = self.fs.0.state.lock().locked.insert(self.path.clone());
        }
        Ok(*locked)
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if *self.locked.get_mut() {
            self.fs.0.state.lock().locked.remove(&self.path);
        }
    }
}

fn write_at(data: &mut Vec<u8>, offset: u64, bytes: &[u8]) {
    let end = offset as usize + bytes.len();
    if data.len() < end {
        data.resize(end, 0);
    }
    data[offset as usize..end].copy_from_slice(bytes);
}

/// xorshift64: deterministic and dependency-free.
pub fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
