use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;

pub(super) struct ExclusiveFileLock {
    file: File,
}

impl ExclusiveFileLock {
    pub(super) fn acquire(file: File, timeout: std::time::Duration) -> io::Result<Self> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            // SAFETY: `file` owns a valid descriptor for the full lifetime of
            // this guard, and `flock` does not retain a pointer to Rust data.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(Self { file });
            }
            let err = io::Error::last_os_error();
            match err.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                io::ErrorKind::WouldBlock => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for the exclusive file lock",
                    ));
                }
                _ => return Err(err),
            }
        }
    }
}

impl Drop for ExclusiveFileLock {
    fn drop(&mut self) {
        // SAFETY: the descriptor remains valid until `self.file` is dropped
        // after this method. Unlock failure cannot be usefully recovered here.
        let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
    }
}
