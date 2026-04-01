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

/// Check whether a daemon process is alive by reading its PID file and
/// probing with `kill(pid, 0)`.
pub fn is_daemon_running(pid_path: &Path) -> bool {
    let content = match fs::read_to_string(pid_path) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let pid: libc::pid_t = match content.trim().parse() {
        Ok(p) => p,
        Err(_) => return false,
    };

    // SAFETY: signal 0 is a null signal — it only checks process existence.
    unsafe { libc::kill(pid, 0) == 0 }
}

/// Send SIGTERM to the running daemon. Cleans up stale PID file if the
/// process no longer exists.
pub fn stop_daemon(pid_path: &Path) -> Result<()> {
    let content = fs::read_to_string(pid_path)
        .with_context(|| format!("no PID file at {}", pid_path.display()))?;
    let pid: libc::pid_t = content.trim().parse().context("invalid PID in file")?;

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
