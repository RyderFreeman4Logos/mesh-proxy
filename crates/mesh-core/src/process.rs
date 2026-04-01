use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;

use anyhow::{Context, Result, bail};

/// Fork the current process into background daemon mode.
///
/// MUST be called **before** the tokio runtime is created (single-threaded).
/// Creates a new session (setsid) and sets restrictive umask.
pub fn daemonize() -> Result<()> {
    // SAFETY: Called before any threads are spawned (pre-tokio).
    // fork() is safe in a single-threaded process.
    unsafe {
        let pid = libc::fork();
        match pid {
            -1 => bail!("fork() failed: {}", std::io::Error::last_os_error()),
            0 => {}                     // child continues
            _ => std::process::exit(0), // parent exits immediately
        }

        // Detach from controlling terminal, become session leader
        if libc::setsid() == -1 {
            bail!("setsid() failed: {}", std::io::Error::last_os_error());
        }

        // Restrictive umask: owner-only for all files created by daemon
        libc::umask(0o077);
    }

    Ok(())
}

/// Write PID to file and acquire an advisory flock.
///
/// Returns the open `File` handle — the caller **must** keep it alive for the
/// entire daemon lifetime so the flock is held.
pub fn write_pid_file(pid_path: &Path) -> Result<fs::File> {
    if let Some(parent) = pid_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create dir {}", parent.display()))?;
    }

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(pid_path)
        .with_context(|| format!("failed to open PID file {}", pid_path.display()))?;

    // SAFETY: fd obtained from a just-opened File; LOCK_NB makes this non-blocking.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        bail!(
            "another daemon is already running (cannot lock {})",
            pid_path.display()
        );
    }

    let mut file = file;
    file.set_len(0).context("failed to truncate PID file")?;
    file.seek(SeekFrom::Start(0))
        .context("failed to rewind PID file")?;
    writeln!(file, "{}", std::process::id()).context("failed to write PID")?;
    file.flush().context("failed to flush PID file")?;

    Ok(file)
}

fn read_pid_file(pid_path: &Path) -> Option<libc::pid_t> {
    let content = fs::read_to_string(pid_path).ok()?;
    content.trim().parse().ok()
}

fn pid_file_is_locked(pid_path: &Path) -> bool {
    let file = match fs::OpenOptions::new().read(true).write(true).open(pid_path) {
        Ok(file) => file,
        Err(_) => return false,
    };

    // SAFETY: fd obtained from a live File; LOCK_NB avoids blocking on daemon lock.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        // SAFETY: unlocking the advisory lock we just acquired on this fd.
        let _ = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
        return false;
    }

    let err = std::io::Error::last_os_error();
    err.raw_os_error() == Some(libc::EWOULDBLOCK)
}

/// Check whether a daemon process is alive by reading its PID file and
/// probing with `kill(pid, 0)`.
pub fn is_daemon_running(pid_path: &Path) -> bool {
    let pid = match read_pid_file(pid_path) {
        Some(pid) => pid,
        None => return false,
    };

    // SAFETY: signal 0 is a null signal — it only checks process existence.
    (unsafe { libc::kill(pid, 0) == 0 }) && pid_file_is_locked(pid_path)
}

/// Send SIGTERM to the running daemon. Cleans up stale PID file if the
/// process no longer exists.
pub fn stop_daemon(pid_path: &Path) -> Result<()> {
    let content = fs::read_to_string(pid_path)
        .with_context(|| format!("no PID file at {}", pid_path.display()))?;
    let pid: libc::pid_t = content.trim().parse().context("invalid PID in file")?;

    if !is_daemon_running(pid_path) {
        let _ = fs::remove_file(pid_path);
        bail!("daemon not running (stale PID file cleaned up)");
    }

    // SAFETY: sending SIGTERM to a parsed PID.
    let rc = unsafe { libc::kill(pid, libc::SIGTERM) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            let _ = fs::remove_file(pid_path);
            bail!("daemon not running (stale PID file cleaned up)");
        }
        bail!("failed to send SIGTERM to PID {pid}: {err}");
    }

    Ok(())
}

/// Remove a stale Unix socket if no daemon is running.
pub fn cleanup_stale_socket(socket_path: &Path, pid_path: &Path) {
    if socket_path.exists() && !is_daemon_running(pid_path) {
        let _ = fs::remove_file(socket_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Child, Command, Stdio};
    use std::thread;
    use std::time::Duration;

    struct ChildGuard {
        child: Child,
    }

    impl ChildGuard {
        fn spawn_sleep() -> Self {
            let child = Command::new("sh")
                .arg("-c")
                .arg("sleep 30")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("failed to spawn helper process");
            Self { child }
        }

        fn id(&self) -> u32 {
            self.child.id()
        }

        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            self.child.try_wait()
        }
    }

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    #[test]
    fn test_write_pid_file_preserves_contents_when_lock_fails() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("daemon.pid");

        let _lock = write_pid_file(&pid_path).unwrap();
        let original = fs::read_to_string(&pid_path).unwrap();

        let err = write_pid_file(&pid_path).unwrap_err();

        assert!(
            err.to_string()
                .contains("another daemon is already running"),
            "unexpected error: {err:#}"
        );
        assert_eq!(fs::read_to_string(&pid_path).unwrap(), original);
    }

    #[test]
    fn test_is_daemon_running_rejects_unlocked_pid_file() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("daemon.pid");

        fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();

        assert!(!is_daemon_running(&pid_path));
    }

    #[test]
    fn test_stop_daemon_rejects_unlocked_pid_file() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("daemon.pid");
        let mut child = ChildGuard::spawn_sleep();

        fs::write(&pid_path, format!("{}\n", child.id())).unwrap();

        let err = stop_daemon(&pid_path).unwrap_err();

        assert!(
            err.to_string().contains("stale PID file"),
            "unexpected error: {err:#}"
        );
        thread::sleep(Duration::from_millis(100));
        assert!(child.try_wait().unwrap().is_none());
    }

    #[test]
    fn test_cleanup_stale_socket_removes_socket_for_unlocked_pid_file() {
        let dir = tempfile::tempdir().unwrap();
        let socket_path = dir.path().join("daemon.sock");
        let pid_path = dir.path().join("daemon.pid");

        fs::write(&socket_path, b"stale").unwrap();
        fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();

        cleanup_stale_socket(&socket_path, &pid_path);

        assert!(!socket_path.exists());
    }
}
