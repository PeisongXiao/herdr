use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

#[cfg(unix)]
use std::time::{Duration, Instant};

pub(crate) const SSH_BYPASS_ENV_VAR: &str = "HERDR_SSH_BYPASS";
#[cfg(unix)]
pub(crate) const SSH_DISABLE_ENV_VAR: &str = "HERDR_SSH_INTEGRATION";
pub(crate) const SSH_SHIM_DIR_ENV_VAR: &str = "HERDR_SSH_SHIM_DIR";
#[cfg(unix)]
const SSH_ORIGINAL_ZDOTDIR_ENV_VAR: &str = "HERDR_SSH_ORIGINAL_ZDOTDIR";
#[cfg(unix)]
const SSH_ORIGINAL_ZDOTDIR_SET_ENV_VAR: &str = "HERDR_SSH_ORIGINAL_ZDOTDIR_SET";
#[cfg(unix)]
const ZSH_BOOTSTRAP_FILE_NAME: &str = ".zshenv";

#[cfg(unix)]
const MANAGED_CONTROL_PERSIST_SECONDS: u64 = 600;
#[cfg(unix)]
const MANAGED_CONTROL_DIR_PREFIX: &str = "herdr-ssh-control-";
#[cfg(unix)]
const MANAGED_CONTROL_SOCKET_NAME: &str = "c";
#[cfg(unix)]
const PRIVATE_DIR_MODE: u32 = 0o700;
#[cfg(unix)]
const MANAGED_CONTROL_CHECK_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(unix)]
const SSH_PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const SSH_PREFLIGHT_OUTPUT_LIMIT: u64 = 1024 * 1024;
#[cfg(unix)]
const CHILD_STATUS_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(unix)]
const CHILD_REAP_GRACE_PERIOD: Duration = Duration::from_millis(100);
#[cfg(unix)]
const MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES: usize = 103;

#[cfg(unix)]
const ZSH_BOOTSTRAP: &str = r#"# Herdr restores the user's ZDOTDIR before loading normal startup files.
\builtin typeset -g __herdr_ssh_shim_dir="${${(%):-%N}:A:h}"
if [[ "${HERDR_SSH_ORIGINAL_ZDOTDIR_SET:-0}" == 1 ]]; then
  \builtin typeset -g __herdr_ssh_original_zshenv="${HERDR_SSH_ORIGINAL_ZDOTDIR}/.zshenv"
  \builtin export ZDOTDIR="${HERDR_SSH_ORIGINAL_ZDOTDIR}"
else
  \builtin typeset -g __herdr_ssh_original_zshenv="${HOME}/.zshenv"
  \builtin unset ZDOTDIR
fi
\builtin unset HERDR_SSH_ORIGINAL_ZDOTDIR HERDR_SSH_ORIGINAL_ZDOTDIR_SET
if [[ -r "${__herdr_ssh_original_zshenv}" && "${__herdr_ssh_original_zshenv}" != "${__herdr_ssh_shim_dir}/.zshenv" ]]; then
  \builtin source -- "${__herdr_ssh_original_zshenv}"
fi

function __herdr_ssh_shim_precmd {
  \builtin emulate -L zsh
  \builtin typeset -a __herdr_ssh_new_path
  \builtin typeset __herdr_ssh_path_entry
  __herdr_ssh_new_path=("${__herdr_ssh_shim_dir}")
  for __herdr_ssh_path_entry in "${path[@]}"; do
    if [[ "${__herdr_ssh_path_entry}" != "${__herdr_ssh_shim_dir}" ]]; then
      __herdr_ssh_new_path+=("${__herdr_ssh_path_entry}")
    fi
  done
  \builtin typeset -ga path
  path=("${__herdr_ssh_new_path[@]}")
  \builtin export PATH
  \builtin rehash
  \builtin typeset -ga precmd_functions
  precmd_functions=("${(@)precmd_functions:#__herdr_ssh_shim_precmd}")
  \builtin unfunction __herdr_ssh_shim_precmd
  \builtin unset __herdr_ssh_shim_dir __herdr_ssh_original_zshenv
}

\builtin typeset -ga precmd_functions
precmd_functions=("${(@)precmd_functions:#__herdr_ssh_shim_precmd}" __herdr_ssh_shim_precmd)
"#;

#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSshInvocation {
    pub(crate) target: String,
    pub(crate) ssh_args: Vec<String>,
}

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct ManagedSshConnection {
    target: String,
    ssh_args: Vec<String>,
    control_path: PathBuf,
    armed: bool,
}

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct ValidatedManagedControlPath {
    control_path: PathBuf,
    parent_device: u64,
    parent_inode: u64,
    endpoint_device: u64,
    endpoint_inode: u64,
}

#[cfg(unix)]
impl ValidatedManagedControlPath {
    pub(crate) fn as_path(&self) -> &Path {
        &self.control_path
    }
}

#[cfg(not(unix))]
#[derive(Debug)]
pub(crate) struct ManagedSshConnection;

#[cfg(not(unix))]
impl ManagedSshConnection {
    pub(crate) fn control_path(&self) -> String {
        String::new()
    }

    pub(crate) fn transfer(self) -> String {
        String::new()
    }
}

#[cfg(not(unix))]
impl Drop for ManagedSshConnection {
    fn drop(&mut self) {}
}

#[cfg(unix)]
impl ManagedSshConnection {
    pub(crate) fn control_path(&self) -> String {
        self.control_path.display().to_string()
    }

    pub(crate) fn transfer(mut self) -> String {
        self.armed = false;
        self.control_path()
    }

    fn close(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let validated = match validate_managed_control_path(&self.control_path) {
            Ok(validated) => validated,
            Err(_) => {
                let _ = cleanup_created_managed_control_path(&self.control_path);
                return;
            }
        };
        let Ok(program) = real_ssh_program_for_exec() else {
            return;
        };
        let mut command = Command::new(program);
        command
            .args(&self.ssh_args)
            .arg("-S")
            .arg(&self.control_path)
            .arg("-O")
            .arg("exit")
            .arg(&self.target)
            .env(SSH_BYPASS_ENV_VAR, "1")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let _ = command_status_with_timeout(&mut command, MANAGED_CONTROL_CHECK_TIMEOUT);
        let _ = cleanup_managed_control_path(validated);
    }
}

#[cfg(unix)]
impl Drop for ManagedSshConnection {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(unix)]
pub(crate) fn apply_pane_env(
    cmd: &mut portable_pty::CommandBuilder,
    extra_env: &[(String, String)],
    macos_zsh_login_shell: bool,
) {
    if !ssh_integration_enabled() {
        return;
    }
    let Ok(shim_dir) = ensure_shim_dir() else {
        tracing::debug!("could not create herdr ssh shim directory");
        return;
    };
    let shim_dirs = inherited_shim_dirs()
        .into_iter()
        .chain(std::iter::once(shim_dir.clone()))
        .collect::<Vec<_>>();
    if let Ok(value) = std::env::join_paths(&shim_dirs) {
        cmd.env(SSH_SHIM_DIR_ENV_VAR, value);
    }
    if let Some(path) = pane_path_with_shim(&shim_dir, extra_env) {
        cmd.env("PATH", path);
    }
    if macos_zsh_login_shell {
        apply_zsh_bootstrap_env(cmd, &shim_dir);
    }
}

#[cfg(not(unix))]
pub(crate) fn apply_pane_env(
    _cmd: &mut portable_pty::CommandBuilder,
    _extra_env: &[(String, String)],
    _macos_zsh_login_shell: bool,
) {
}

#[cfg(unix)]
fn apply_zsh_bootstrap_env(cmd: &mut portable_pty::CommandBuilder, shim_dir: &Path) {
    match cmd.get_env("ZDOTDIR").map(std::ffi::OsStr::to_os_string) {
        Some(original) => {
            cmd.env(SSH_ORIGINAL_ZDOTDIR_SET_ENV_VAR, "1");
            cmd.env(SSH_ORIGINAL_ZDOTDIR_ENV_VAR, original);
        }
        None => {
            cmd.env(SSH_ORIGINAL_ZDOTDIR_SET_ENV_VAR, "0");
            cmd.env(SSH_ORIGINAL_ZDOTDIR_ENV_VAR, "");
        }
    }
    cmd.env("ZDOTDIR", shim_dir);
}

#[cfg(unix)]
fn ssh_integration_enabled() -> bool {
    if std::env::var(SSH_DISABLE_ENV_VAR)
        .ok()
        .as_deref()
        .is_some_and(is_disabled_value)
    {
        return false;
    }
    crate::config::Config::load().config.remote.ssh_integration
}

#[cfg(unix)]
fn is_disabled_value(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "off" | "no"
    )
}

#[cfg(unix)]
fn ensure_shim_dir() -> io::Result<PathBuf> {
    use std::sync::{Mutex, OnceLock};

    static SHIM_DIR: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();
    let mut cached = SHIM_DIR
        .get_or_init(|| Mutex::new(None))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(path) = cached.as_ref().filter(|path| shim_dir_is_valid(path)) {
        return Ok(path.clone());
    }
    let path = create_shim_dir()?;
    *cached = Some(path.clone());
    Ok(path)
}

#[cfg(unix)]
fn shim_dir_is_valid(path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt as _;

    let uid = current_effective_uid();
    let valid = |path: &Path, mode: u32, directory: bool| {
        std::fs::symlink_metadata(path).is_ok_and(|metadata| {
            let expected_type = if directory {
                metadata.file_type().is_dir()
            } else {
                metadata.file_type().is_file()
            };
            expected_type && metadata.uid() == uid && metadata.mode() & 0o7777 == mode
        })
    };
    valid(path, 0o700, true)
        && valid(&path.join("ssh"), 0o700, false)
        && valid(&path.join(ZSH_BOOTSTRAP_FILE_NAME), 0o600, false)
}

#[cfg(unix)]
fn create_shim_dir() -> io::Result<PathBuf> {
    use std::os::unix::fs::DirBuilderExt;

    let exe = std::env::current_exe()?;
    let exe = exe
        .to_str()
        .ok_or_else(|| io::Error::other("herdr binary path is not UTF-8"))?;
    let script = format!("#!/bin/sh\nexec {} ssh \"$@\"\n", shell_quote(exe));

    let mut bases = Vec::new();
    let configured_temp = std::env::temp_dir();
    if configured_temp.is_absolute() {
        bases.push(configured_temp);
    }
    let slash_tmp = PathBuf::from("/tmp");
    if bases.first() != Some(&slash_tmp) {
        bases.push(slash_tmp);
    }

    let mut last_error = None;
    for base in bases {
        for attempt in 0..100 {
            let dir = base.join(format!("herdr-ssh-shim-{}-{attempt}", std::process::id()));
            match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
                Ok(()) => {
                    let assets = write_shim_asset(&dir.join("ssh"), 0o700, script.as_bytes())
                        .and_then(|()| {
                            write_shim_asset(
                                &dir.join(ZSH_BOOTSTRAP_FILE_NAME),
                                0o600,
                                ZSH_BOOTSTRAP.as_bytes(),
                            )
                        });
                    match assets {
                        Ok(()) => return Ok(dir),
                        Err(err) => {
                            let _ = std::fs::remove_file(dir.join("ssh"));
                            let _ = std::fs::remove_file(dir.join(ZSH_BOOTSTRAP_FILE_NAME));
                            let _ = std::fs::remove_dir(&dir);
                            last_error = Some(err);
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    last_error = Some(err);
                    break;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "failed to create herdr ssh shim directory",
        )
    }))
}

#[cfg(unix)]
fn write_shim_asset(path: &Path, mode: u32, contents: &[u8]) -> io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)?;
    file.write_all(contents)?;
    let mut permissions = file.metadata()?.permissions();
    permissions.set_mode(mode);
    file.set_permissions(permissions)
}

#[cfg(unix)]
fn pane_path_with_shim(shim_dir: &Path, extra_env: &[(String, String)]) -> Option<OsString> {
    let base = extra_env
        .iter()
        .rev()
        .find(|(key, _)| key == "PATH")
        .map(|(_, value)| OsString::from(value))
        .or_else(|| std::env::var_os("PATH"))
        .unwrap_or_default();
    let inherited = inherited_shim_dirs();
    let mut paths = vec![shim_dir.to_path_buf()];
    paths.extend(
        std::env::split_paths(&base).filter(|path| !inherited.iter().any(|shim| shim == path)),
    );
    std::env::join_paths(paths).ok()
}

#[cfg(unix)]
fn inherited_shim_dirs() -> Vec<PathBuf> {
    std::env::var_os(SSH_SHIM_DIR_ENV_VAR)
        .map(|value| std::env::split_paths(&value).collect())
        .unwrap_or_default()
}

#[cfg(unix)]
pub(crate) fn prepare_managed_connection(
    target: &str,
    ssh_args: &[String],
) -> io::Result<Option<ManagedSshConnection>> {
    if !standard_streams_are_terminals() {
        return Ok(None);
    }

    let dir = create_private_temp_dir_for_socket(
        MANAGED_CONTROL_DIR_PREFIX.trim_end_matches('-'),
        MANAGED_CONTROL_SOCKET_NAME,
    )?;
    let control_path = dir.join(MANAGED_CONTROL_SOCKET_NAME);
    let status = Command::new(real_ssh_program_for_exec()?)
        .arg("-T")
        .arg("-M")
        .arg("-N")
        .arg("-f")
        .arg("-o")
        .arg("RemoteCommand=none")
        .arg("-o")
        .arg("StreamLocalBindMask=0177")
        .arg("-o")
        .arg("StreamLocalBindUnlink=yes")
        .arg("-o")
        .arg(format!("ControlPersist={MANAGED_CONTROL_PERSIST_SECONDS}"))
        .arg("-o")
        .arg("ServerAliveInterval=15")
        .arg("-o")
        .arg("ServerAliveCountMax=3")
        .args(ssh_args)
        .arg("-S")
        .arg(&control_path)
        .arg(target)
        .env(SSH_BYPASS_ENV_VAR, "1")
        .status();

    match status {
        Ok(status) if status.success() => {
            if let Err(err) = validate_managed_control_path(&control_path) {
                let _ = cleanup_created_managed_control_path(&control_path);
                return Err(io::Error::new(
                    err.kind(),
                    format!("ssh did not create a valid Herdr control endpoint: {err}"),
                ));
            }
            Ok(Some(ManagedSshConnection {
                target: target.to_string(),
                ssh_args: ssh_args.to_vec(),
                control_path,
                armed: true,
            }))
        }
        Ok(status) => {
            let _ = cleanup_created_managed_control_path(&control_path);
            Err(io::Error::other(format!(
                "ssh authentication failed with {status}; retry after fixing SSH access, or bypass Herdr with HERDR_SSH_BYPASS=1"
            )))
        }
        Err(err) => {
            let _ = cleanup_created_managed_control_path(&control_path);
            Err(io::Error::new(
                err.kind(),
                format!("failed to start ssh authentication: {err}"),
            ))
        }
    }
}

#[cfg(not(unix))]
pub(crate) fn prepare_managed_connection(
    _target: &str,
    _ssh_args: &[String],
) -> io::Result<Option<ManagedSshConnection>> {
    Ok(None)
}

#[cfg(unix)]
fn create_private_temp_dir(prefix: &str) -> io::Result<PathBuf> {
    create_private_temp_dir_with_child(prefix, None)
}

#[cfg(unix)]
fn create_private_temp_dir_for_socket(prefix: &str, socket_name: &str) -> io::Result<PathBuf> {
    create_private_temp_dir_with_child(prefix, Some(socket_name))
}

#[cfg(unix)]
fn create_private_temp_dir_with_child(
    prefix: &str,
    required_child: Option<&str>,
) -> io::Result<PathBuf> {
    use std::os::unix::fs::DirBuilderExt as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);
    let mut bases = Vec::new();
    let configured_temp = std::env::temp_dir();
    if configured_temp.is_absolute() {
        bases.push(configured_temp);
    }
    let slash_tmp = PathBuf::from("/tmp");
    if bases.first() != Some(&slash_tmp) {
        bases.push(slash_tmp);
    }

    let mut last_error = None;
    for base in bases {
        for _ in 0..100 {
            let nonce = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let dir = base.join(format!("{prefix}-{}-{nonce}", std::process::id()));
            if !private_temp_child_path_fits(&dir, required_child) {
                last_error = Some(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "temporary directory is too long for a portable Unix socket path",
                ));
                break;
            }
            match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
                Ok(()) => return Ok(dir),
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    last_error = Some(err);
                    break;
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("failed to create private {prefix} directory"),
        )
    }))
}

#[cfg(unix)]
fn private_temp_child_path_fits(dir: &Path, required_child: Option<&str>) -> bool {
    required_child.is_none_or(|child| {
        dir.join(child).as_os_str().as_encoded_bytes().len() <= MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES
    })
}

#[cfg(unix)]
pub(crate) fn validate_managed_control_path(
    control_path: &Path,
) -> io::Result<ValidatedManagedControlPath> {
    use std::ffi::OsStr;
    use std::os::unix::fs::MetadataExt as _;

    let (parent, parent_metadata) = validate_managed_control_parent(control_path)?;
    let metadata = std::fs::symlink_metadata(control_path).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "could not inspect managed ssh control endpoint {}: {err}",
                control_path.display()
            ),
        )
    })?;
    validate_managed_control_endpoint(control_path, &metadata, current_effective_uid())?;
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        if entry.file_name() != OsStr::new(MANAGED_CONTROL_SOCKET_NAME) {
            return Err(unmanaged_control_path_error(
                control_path,
                "control directory contains an unexpected entry",
            ));
        }
    }

    Ok(ValidatedManagedControlPath {
        control_path: control_path.to_path_buf(),
        parent_device: parent_metadata.dev(),
        parent_inode: parent_metadata.ino(),
        endpoint_device: metadata.dev(),
        endpoint_inode: metadata.ino(),
    })
}

#[cfg(unix)]
pub(crate) fn managed_control_connection_is_alive(target: &str, control_path: &str) -> bool {
    if target.is_empty() || target.starts_with('-') {
        return false;
    }
    if validate_managed_control_path(Path::new(control_path)).is_err() {
        return false;
    }
    let Ok(program) = real_ssh_program_for_exec() else {
        return false;
    };
    let mut command = Command::new(program);
    command
        .arg("-S")
        .arg(control_path)
        .arg("-O")
        .arg("check")
        .arg(target)
        .env(SSH_BYPASS_ENV_VAR, "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    command_status_with_timeout(&mut command, MANAGED_CONTROL_CHECK_TIMEOUT)
        .is_ok_and(|status| status.is_some_and(|status| status.success()))
}

#[cfg(unix)]
fn command_status_with_timeout(
    command: &mut Command,
    timeout: Duration,
) -> io::Result<Option<std::process::ExitStatus>> {
    let mut child = command.spawn()?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(Some(status)),
            Ok(None) => {}
            Err(err) => {
                terminate_child_bounded(&mut child);
                return Err(err);
            }
        }

        let remaining = timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            terminate_child_bounded(&mut child);
            return Ok(None);
        }
        std::thread::sleep(remaining.min(CHILD_STATUS_POLL_INTERVAL));
    }
}

#[cfg(unix)]
fn terminate_child_bounded(child: &mut std::process::Child) {
    let _ = child.kill();
    let started = Instant::now();
    while started.elapsed() < CHILD_REAP_GRACE_PERIOD {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => std::thread::sleep(CHILD_STATUS_POLL_INTERVAL),
        }
    }
}

#[cfg(unix)]
pub(crate) fn cleanup_managed_control_path(
    validated: ValidatedManagedControlPath,
) -> io::Result<()> {
    use std::ffi::OsStr;
    use std::os::unix::fs::MetadataExt as _;

    let control_path = validated.as_path();
    let (parent, parent_metadata) = validate_managed_control_parent(control_path)?;
    if parent_metadata.dev() != validated.parent_device
        || parent_metadata.ino() != validated.parent_inode
    {
        return Err(unmanaged_control_path_error(
            control_path,
            "control directory changed after validation",
        ));
    }
    let mut has_control_endpoint = false;
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        if entry.file_name() != OsStr::new(MANAGED_CONTROL_SOCKET_NAME) {
            return Err(unmanaged_control_path_error(
                control_path,
                "control directory contains an unexpected entry",
            ));
        }
        let endpoint_path = entry.path();
        let metadata = std::fs::symlink_metadata(&endpoint_path)?;
        validate_managed_control_endpoint(&endpoint_path, &metadata, current_effective_uid())?;
        if metadata.dev() != validated.endpoint_device || metadata.ino() != validated.endpoint_inode
        {
            return Err(unmanaged_control_path_error(
                control_path,
                "control endpoint changed after validation",
            ));
        }
        has_control_endpoint = true;
    }

    if has_control_endpoint {
        // Recheck immediately before unlinking rather than trusting the directory scan.
        let metadata = std::fs::symlink_metadata(control_path)?;
        validate_managed_control_endpoint(control_path, &metadata, current_effective_uid())?;
        if metadata.dev() != validated.endpoint_device || metadata.ino() != validated.endpoint_inode
        {
            return Err(unmanaged_control_path_error(
                control_path,
                "control endpoint changed before cleanup",
            ));
        }
        std::fs::remove_file(control_path)?;
    }
    std::fs::remove_dir(parent)
}

#[cfg(unix)]
fn cleanup_created_managed_control_path(control_path: &Path) -> io::Result<()> {
    let validation_error = match validate_managed_control_path(control_path) {
        Ok(validated) => return cleanup_managed_control_path(validated),
        Err(err) => err,
    };
    let (parent, _) = validate_managed_control_parent(control_path)?;
    if std::fs::read_dir(parent)?.next().transpose()?.is_some() {
        return Err(validation_error);
    }
    std::fs::remove_dir(parent)
}

#[cfg(unix)]
fn validate_managed_control_parent(control_path: &Path) -> io::Result<(&Path, std::fs::Metadata)> {
    use std::os::unix::fs::MetadataExt as _;
    use std::path::Component;

    if !control_path.is_absolute() {
        return Err(unmanaged_control_path_error(
            control_path,
            "path is not absolute",
        ));
    }
    if control_path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(unmanaged_control_path_error(
            control_path,
            "path contains relative components",
        ));
    }
    if control_path.file_name().and_then(|name| name.to_str()) != Some(MANAGED_CONTROL_SOCKET_NAME)
    {
        return Err(unmanaged_control_path_error(
            control_path,
            "unexpected control socket basename",
        ));
    }
    let parent = control_path.parent().ok_or_else(|| {
        unmanaged_control_path_error(control_path, "control path has no parent directory")
    })?;
    let valid_parent_name = parent
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(is_managed_control_dir_name);
    if !valid_parent_name {
        return Err(unmanaged_control_path_error(
            control_path,
            "unexpected control directory basename",
        ));
    }

    let metadata = std::fs::symlink_metadata(parent).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "could not inspect managed ssh control directory {}: {err}",
                parent.display()
            ),
        )
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(unmanaged_control_path_error(
            control_path,
            "control parent is not a non-symlink directory",
        ));
    }
    let effective_uid = current_effective_uid();
    if metadata.uid() != effective_uid {
        return Err(unmanaged_control_path_error(
            control_path,
            "control directory is not owned by the effective user",
        ));
    }
    if metadata.mode() & 0o7777 != PRIVATE_DIR_MODE {
        return Err(unmanaged_control_path_error(
            control_path,
            "control directory permissions are not private",
        ));
    }
    Ok((parent, metadata))
}

#[cfg(unix)]
fn validate_managed_control_endpoint(
    control_path: &Path,
    metadata: &std::fs::Metadata,
    effective_uid: libc::uid_t,
) -> io::Result<()> {
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    if !metadata.file_type().is_socket() {
        return Err(unmanaged_control_path_error(
            control_path,
            "control endpoint is not a Unix socket",
        ));
    }
    if metadata.uid() != effective_uid {
        return Err(unmanaged_control_path_error(
            control_path,
            "control endpoint is not owned by the effective user",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn is_managed_control_dir_name(name: &str) -> bool {
    let Some(suffix) = name.strip_prefix(MANAGED_CONTROL_DIR_PREFIX) else {
        return false;
    };
    !suffix.is_empty()
}

#[cfg(unix)]
fn current_effective_uid() -> libc::uid_t {
    // SAFETY: geteuid has no preconditions and does not dereference pointers.
    unsafe { libc::geteuid() }
}

#[cfg(unix)]
fn unmanaged_control_path_error(control_path: &Path, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!(
            "refusing unmanaged ssh control path {}: {reason}",
            control_path.display()
        ),
    )
}

pub(crate) fn preflight_interactive_ssh_args(args: &[String]) -> io::Result<bool> {
    let output = ssh_effective_config_output(args)?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "ssh effective-config preflight failed with {}",
            output.status
        )));
    }
    let config = std::str::from_utf8(&output.stdout).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("ssh effective-config output was not UTF-8: {err}"),
        )
    })?;
    Ok(effective_config_allows_managed_shell(config))
}

#[cfg(unix)]
fn ssh_effective_config_output(args: &[String]) -> io::Result<std::process::Output> {
    let dir = create_private_temp_dir("herdr-ssh-preflight")?;
    let stdout_path = dir.join("stdout");
    let stderr_path = dir.join("stderr");
    let result = (|| {
        let stdout = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&stdout_path)?;
        let stderr = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&stderr_path)?;
        let mut command = Command::new(real_ssh_program_for_exec()?);
        command
            .arg("-G")
            .args(args)
            .env(SSH_BYPASS_ENV_VAR, "1")
            .stdin(std::process::Stdio::null())
            .stdout(stdout)
            .stderr(stderr);
        let status =
            command_status_with_timeout(&mut command, SSH_PREFLIGHT_TIMEOUT)?.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "ssh effective-config preflight timed out",
                )
            })?;
        let read_output = |path: &Path| -> io::Result<Vec<u8>> {
            let metadata = std::fs::metadata(path)?;
            if metadata.len() > SSH_PREFLIGHT_OUTPUT_LIMIT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ssh effective-config preflight produced too much output",
                ));
            }
            std::fs::read(path)
        };
        Ok(std::process::Output {
            status,
            stdout: read_output(&stdout_path)?,
            stderr: read_output(&stderr_path)?,
        })
    })();
    let _ = std::fs::remove_dir_all(dir);
    result
}

#[cfg(not(unix))]
fn ssh_effective_config_output(args: &[String]) -> io::Result<std::process::Output> {
    let _ = args;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "direct SSH federation is only supported on Unix",
    ))
}

fn effective_config_allows_managed_shell(config: &str) -> bool {
    let mut saw_entry = false;
    for line in config
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let mut parts = line.splitn(2, char::is_whitespace);
        let key = parts.next().unwrap_or_default();
        let Some(value) = parts
            .next()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            return false;
        };
        saw_entry = true;
        if !effective_config_option_allows_managed_shell(key, value) {
            return false;
        }
    }
    saw_entry
}

fn effective_config_option_allows_managed_shell(key: &str, value: &str) -> bool {
    let key = key.to_ascii_lowercase();
    let value = value.trim().to_ascii_lowercase();
    match key.as_str() {
        "remotecommand" | "localcommand" | "stdioforwardhost" => value == "none",
        "requesttty" => matches!(value.as_str(), "auto" | "yes" | "true" | "force"),
        "sessiontype" => value == "default",
        "stdinnull" | "forkafterauthentication" | "permitlocalcommand" => {
            matches!(value.as_str(), "no" | "false")
        }
        "tunnel" => matches!(value.as_str(), "no" | "false"),
        "localforward" | "remoteforward" | "dynamicforward" => false,
        _ => true,
    }
}

#[cfg(unix)]
pub(crate) fn parse_interactive_ssh_args(args: &[String]) -> Option<ParsedSshInvocation> {
    let mut ssh_args = Vec::new();
    let mut target = None;
    let mut index = 0;
    let mut end_options = false;

    while index < args.len() {
        let arg = &args[index];
        if target.is_some() {
            return None;
        }
        if !end_options && arg == "--" {
            end_options = true;
            index += 1;
            continue;
        }
        if !end_options && arg.starts_with('-') && arg != "-" {
            match parse_ssh_option(args, &mut index, &mut ssh_args) {
                SshOptionParse::Continue => continue,
                SshOptionParse::Unsupported => return None,
            }
        }
        target = Some(arg.clone());
        index += 1;
    }

    target
        .filter(|target| !target.is_empty() && !target.starts_with('-'))
        .map(|target| ParsedSshInvocation { target, ssh_args })
}

#[cfg(unix)]
enum SshOptionParse {
    Continue,
    Unsupported,
}

#[cfg(unix)]
fn parse_ssh_option(
    args: &[String],
    index: &mut usize,
    ssh_args: &mut Vec<String>,
) -> SshOptionParse {
    let arg = &args[*index];
    if unsupported_ssh_option(arg) {
        return SshOptionParse::Unsupported;
    }
    if is_terminal_allocation_option(arg) {
        *index += 1;
        return SshOptionParse::Continue;
    }
    if arg == "-o" {
        let Some(value) = args.get(*index + 1) else {
            return SshOptionParse::Unsupported;
        };
        if unsupported_o_option(value) {
            return SshOptionParse::Unsupported;
        }
        ssh_args.push(arg.clone());
        ssh_args.push(value.clone());
        *index += 2;
        return SshOptionParse::Continue;
    }
    if let Some(value) = arg.strip_prefix("-o").filter(|value| !value.is_empty()) {
        if unsupported_o_option(value) {
            return SshOptionParse::Unsupported;
        }
        ssh_args.push(arg.clone());
        *index += 1;
        return SshOptionParse::Continue;
    }
    if is_attached_value_option(arg) || is_preserved_no_value_option(arg) {
        ssh_args.push(arg.clone());
        *index += 1;
        return SshOptionParse::Continue;
    }
    if is_separate_value_option(arg) {
        let Some(value) = args.get(*index + 1) else {
            return SshOptionParse::Unsupported;
        };
        ssh_args.push(arg.clone());
        ssh_args.push(value.clone());
        *index += 2;
        return SshOptionParse::Continue;
    }
    if let Some(expanded) = expand_clustered_no_value_option(arg) {
        ssh_args.extend(expanded);
        *index += 1;
        return SshOptionParse::Continue;
    }
    SshOptionParse::Unsupported
}

#[cfg(unix)]
fn unsupported_ssh_option(arg: &str) -> bool {
    matches!(
        arg,
        "-N" | "-f" | "-s" | "-T" | "-w" | "-W" | "-L" | "-R" | "-D" | "-S"
    ) || matches!(arg.chars().nth(1), Some('L' | 'R' | 'D' | 'W' | 'w' | 'S'))
}

#[cfg(unix)]
fn is_terminal_allocation_option(arg: &str) -> bool {
    arg.len() > 1 && arg[1..].chars().all(|ch| ch == 't')
}

#[cfg(unix)]
fn is_separate_value_option(arg: &str) -> bool {
    matches!(
        arg,
        "-B" | "-b" | "-c" | "-E" | "-e" | "-F" | "-I" | "-i" | "-J" | "-l" | "-m" | "-P" | "-p"
    )
}

#[cfg(unix)]
fn is_attached_value_option(arg: &str) -> bool {
    let Some(option) = arg.chars().nth(1) else {
        return false;
    };
    arg.len() > 2
        && matches!(
            option,
            'B' | 'b' | 'c' | 'E' | 'e' | 'F' | 'I' | 'i' | 'J' | 'l' | 'm' | 'P' | 'p'
        )
}

#[cfg(unix)]
fn is_preserved_no_value_option(arg: &str) -> bool {
    matches!(
        arg,
        "-A" | "-a"
            | "-C"
            | "-4"
            | "-6"
            | "-q"
            | "-v"
            | "-vv"
            | "-vvv"
            | "-X"
            | "-Y"
            | "-x"
            | "-K"
            | "-k"
            | "-y"
    )
}

#[cfg(unix)]
fn expand_clustered_no_value_option(arg: &str) -> Option<Vec<String>> {
    if !arg.starts_with('-') || arg.len() <= 2 {
        return None;
    }
    let mut expanded = Vec::new();
    for ch in arg[1..].chars() {
        if ch == 't' {
            continue;
        }
        if matches!(
            ch,
            'A' | 'a' | 'C' | '4' | '6' | 'q' | 'v' | 'X' | 'Y' | 'x' | 'K' | 'k' | 'y'
        ) {
            expanded.push(format!("-{ch}"));
            continue;
        }
        return None;
    }
    Some(expanded)
}

#[cfg(unix)]
fn unsupported_o_option(raw: &str) -> bool {
    let trimmed = raw.trim();
    let (key, value) = if let Some((key, value)) = trimmed.split_once('=') {
        (key, value)
    } else {
        let mut parts = trimmed.splitn(2, char::is_whitespace);
        (
            parts.next().unwrap_or_default(),
            parts.next().unwrap_or_default(),
        )
    };
    let key = key.trim().to_ascii_lowercase();
    let value = value.trim().to_ascii_lowercase();

    match key.as_str() {
        "localforward" | "remoteforward" | "dynamicforward" | "stdioforward" | "localcommand" => {
            true
        }
        "controlmaster"
        | "controlpath"
        | "controlpersist"
        | "streamlocalbindmask"
        | "streamlocalbindunlink" => true,
        "remotecommand" => !value.is_empty() && value != "none",
        "requesttty" => matches!(value.as_str(), "no" | "false"),
        "sessiontype" => matches!(value.as_str(), "none" | "subsystem"),
        "stdinnull" => matches!(value.as_str(), "yes" | "true"),
        "forkafterauthentication" | "permitlocalcommand" => {
            matches!(value.as_str(), "yes" | "true")
        }
        "tunnel" => !matches!(value.as_str(), "" | "no" | "false"),
        _ => false,
    }
}

#[cfg(unix)]
fn standard_streams_are_terminals() -> bool {
    use std::io::IsTerminal as _;

    terminal_state_allows_interception(
        std::io::stdin().is_terminal(),
        std::io::stdout().is_terminal(),
        std::io::stderr().is_terminal(),
    )
}

#[cfg(unix)]
fn terminal_state_allows_interception(stdin: bool, stdout: bool, stderr: bool) -> bool {
    stdin && stdout && stderr
}

#[cfg(unix)]
pub(crate) fn should_integrate_invocation() -> bool {
    if !standard_streams_are_terminals()
        || std::env::var(SSH_BYPASS_ENV_VAR).ok().as_deref() == Some("1")
        || std::env::var(crate::HERDR_ENV_VAR).ok().as_deref() != Some(crate::HERDR_ENV_VALUE)
    {
        return false;
    }
    if std::env::var_os(SSH_SHIM_DIR_ENV_VAR).is_some() && !ssh_integration_enabled() {
        return false;
    }
    true
}

pub(crate) fn run_real_ssh_args(args: &[String]) -> io::Result<i32> {
    let status = Command::new(real_ssh_program_for_exec()?)
        .args(args)
        .env(SSH_BYPASS_ENV_VAR, "1")
        .status()?;
    Ok(status.code().unwrap_or(1))
}

pub(crate) fn run_real_ssh_os_args(args: &[OsString]) -> io::Result<i32> {
    let status = Command::new(real_ssh_program_for_exec()?)
        .args(args)
        .env(SSH_BYPASS_ENV_VAR, "1")
        .status()?;
    Ok(status.code().unwrap_or(1))
}

pub(crate) fn run_ssh_argv(argv: &[String]) -> io::Result<i32> {
    let Some((program, args)) = argv.split_first() else {
        return Ok(1);
    };
    let program = if Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
        == Some("ssh")
    {
        real_ssh_program_for_exec()?
    } else {
        PathBuf::from(program)
    };
    let status = Command::new(program)
        .args(args)
        .env(SSH_BYPASS_ENV_VAR, "1")
        .status()?;
    Ok(status.code().unwrap_or(1))
}

pub(crate) fn real_ssh_program() -> PathBuf {
    find_real_ssh().unwrap_or_else(|| PathBuf::from("ssh"))
}

pub(crate) fn real_ssh_program_for_exec() -> io::Result<PathBuf> {
    find_real_ssh()
        .or_else(|| {
            if std::env::var_os(SSH_SHIM_DIR_ENV_VAR).is_none() {
                Some(PathBuf::from("ssh"))
            } else {
                None
            }
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "could not find a real ssh binary outside Herdr's ssh shim",
            )
        })
}

fn find_real_ssh() -> Option<PathBuf> {
    let shim_dirs = std::env::var_os(SSH_SHIM_DIR_ENV_VAR)
        .map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
        .unwrap_or_default();
    let current_exe = std::env::current_exe()
        .ok()
        .and_then(|path| path.canonicalize().ok());
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        if shim_dirs.iter().any(|shim| shim == &dir) {
            continue;
        }
        let candidate = dir.join("ssh");
        if !is_executable_file(&candidate) {
            continue;
        }
        if current_exe.as_ref().is_some_and(|current| {
            candidate
                .canonicalize()
                .ok()
                .as_ref()
                .is_some_and(|candidate| candidate == current)
        }) {
            continue;
        }
        return Some(candidate);
    }
    None
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = path.metadata() else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

#[cfg(unix)]
fn shell_quote(value: &str) -> String {
    if !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(
                    ch,
                    '@' | '%' | '_' | '+' | '=' | ':' | ',' | '.' | '/' | '-'
                )
        })
    {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    struct TestDir(PathBuf);

    #[cfg(unix)]
    impl TestDir {
        fn new(prefix: &str) -> Self {
            Self(create_private_temp_dir(prefix).unwrap())
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    #[cfg(unix)]
    impl Drop for TestDir {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt as _;

            if let Ok(metadata) = std::fs::symlink_metadata(&self.0) {
                if metadata.is_dir() && !metadata.file_type().is_symlink() {
                    let _ = std::fs::set_permissions(
                        &self.0,
                        std::fs::Permissions::from_mode(PRIVATE_DIR_MODE),
                    );
                }
            }
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[cfg(unix)]
    #[test]
    fn portable_socket_path_limit_includes_the_required_child() {
        assert!(private_temp_child_path_fits(
            Path::new("/tmp/herdr-short"),
            Some(MANAGED_CONTROL_SOCKET_NAME),
        ));

        let long_dir = PathBuf::from(format!(
            "/tmp/{}",
            "x".repeat(MAX_PORTABLE_UNIX_SOCKET_PATH_BYTES)
        ));
        assert!(!private_temp_child_path_fits(
            &long_dir,
            Some(MANAGED_CONTROL_SOCKET_NAME),
        ));
        assert!(private_temp_child_path_fits(&long_dir, None));
    }

    #[cfg(unix)]
    #[test]
    fn zsh_bootstrap_env_preserves_set_empty_and_unset_zdotdir() {
        let shim_dir = Path::new("/tmp/herdr-shim");

        for (original, expected_set, expected_original) in [
            (None, "0", ""),
            (Some(""), "1", ""),
            (Some("/custom/zsh"), "1", "/custom/zsh"),
        ] {
            let mut command = portable_pty::CommandBuilder::new_default_prog();
            command.env_remove("ZDOTDIR");
            if let Some(original) = original {
                command.env("ZDOTDIR", original);
            }

            apply_zsh_bootstrap_env(&mut command, shim_dir);

            assert_eq!(
                command
                    .get_env(SSH_ORIGINAL_ZDOTDIR_SET_ENV_VAR)
                    .and_then(std::ffi::OsStr::to_str),
                Some(expected_set)
            );
            assert_eq!(
                command
                    .get_env(SSH_ORIGINAL_ZDOTDIR_ENV_VAR)
                    .and_then(std::ffi::OsStr::to_str),
                Some(expected_original)
            );
            assert_eq!(
                command.get_env("ZDOTDIR").and_then(std::ffi::OsStr::to_str),
                shim_dir.to_str()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn zsh_bootstrap_repairs_path_once_at_the_first_prompt() {
        assert!(ZSH_BOOTSTRAP.contains("builtin source --"));
        assert!(ZSH_BOOTSTRAP.contains("precmd_functions"));
        assert!(ZSH_BOOTSTRAP.contains("builtin rehash"));
        assert!(ZSH_BOOTSTRAP.contains("builtin unfunction __herdr_ssh_shim_precmd"));
    }

    #[cfg(unix)]
    #[test]
    fn shim_cache_validation_requires_both_private_assets() {
        let dir = TestDir::new("herdr-shim-validation");
        write_shim_asset(&dir.path().join("ssh"), 0o700, b"#!/bin/sh\n").unwrap();
        write_shim_asset(
            &dir.path().join(ZSH_BOOTSTRAP_FILE_NAME),
            0o600,
            ZSH_BOOTSTRAP.as_bytes(),
        )
        .unwrap();

        assert!(shim_dir_is_valid(dir.path()));
        std::fs::remove_file(dir.path().join(ZSH_BOOTSTRAP_FILE_NAME)).unwrap();
        assert!(!shim_dir_is_valid(dir.path()));
    }

    #[cfg(unix)]
    #[test]
    fn zsh_bootstrap_restores_startup_files_and_repairs_the_first_prompt_path() {
        let zsh = Path::new("/usr/bin/zsh");
        if !zsh.is_file() {
            return;
        }
        let shim = TestDir::new("herdr-zsh-shim");
        let user = TestDir::new("herdr-zsh-user");
        write_shim_asset(
            &shim.path().join(ZSH_BOOTSTRAP_FILE_NAME),
            0o600,
            ZSH_BOOTSTRAP.as_bytes(),
        )
        .unwrap();
        write_shim_asset(&shim.path().join("ssh"), 0o700, b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::write(
            user.path().join(".zshenv"),
            "alias builtin=false\nHERDR_TEST_ZSHENV=1\n",
        )
        .unwrap();
        std::fs::write(user.path().join(".zprofile"), "HERDR_TEST_ZPROFILE=1\n").unwrap();
        std::fs::write(
            user.path().join(".zshrc"),
            "HERDR_TEST_ZSHRC=1\nPATH=/usr/bin:/bin\n\\builtin hash ssh\n",
        )
        .unwrap();
        std::fs::write(user.path().join(".zlogin"), "HERDR_TEST_ZLOGIN=1\n").unwrap();
        let command = r#"for hook in "${precmd_functions[@]}"; do "$hook"; done
\builtin print -r -- "ZDOTDIR=${ZDOTDIR}"
\builtin print -r -- "PATH=${PATH}"
\builtin print -r -- "SSH=$(\builtin whence -p ssh)"
\builtin print -r -- "FILES=${HERDR_TEST_ZSHENV:-0}${HERDR_TEST_ZPROFILE:-0}${HERDR_TEST_ZSHRC:-0}${HERDR_TEST_ZLOGIN:-0}"
\builtin print -r -- "HOOKS=${(j:,:)precmd_functions}"
"#;
        let output = Command::new(zsh)
            .args(["-lic", command])
            .env_clear()
            .env("HOME", user.path())
            .env("PATH", "/usr/bin:/bin")
            .env("SHELL", zsh)
            .env("TERM", "xterm-256color")
            .env("ZDOTDIR", shim.path())
            .env(SSH_ORIGINAL_ZDOTDIR_SET_ENV_VAR, "1")
            .env(SSH_ORIGINAL_ZDOTDIR_ENV_VAR, user.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "zsh stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains(&format!("ZDOTDIR={}", user.path().display())));
        let path = stdout
            .lines()
            .find_map(|line| line.strip_prefix("PATH="))
            .unwrap();
        let path_entries = std::env::split_paths(path).collect::<Vec<_>>();
        assert_eq!(
            path_entries.first().map(PathBuf::as_path),
            Some(shim.path())
        );
        assert_eq!(
            path_entries
                .iter()
                .filter(|entry| entry.as_path() == shim.path())
                .count(),
            1
        );
        assert!(stdout.contains(&format!("SSH={}/ssh", shim.path().display())));
        assert!(stdout.contains("FILES=1111"));
        assert!(stdout.lines().any(|line| line == "HOOKS="));
    }

    #[cfg(unix)]
    #[test]
    fn interception_requires_three_terminal_streams() {
        for stdin in [false, true] {
            for stdout in [false, true] {
                for stderr in [false, true] {
                    assert_eq!(
                        terminal_state_allows_interception(stdin, stdout, stderr),
                        stdin && stdout && stderr
                    );
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn parses_simple_interactive_target() {
        let parsed = parse_interactive_ssh_args(&strings(&["workbox"])).unwrap();
        assert_eq!(parsed.target, "workbox");
        assert!(parsed.ssh_args.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn preserves_interactive_connection_options() {
        let parsed = parse_interactive_ssh_args(&strings(&[
            "-p",
            "2222",
            "-J",
            "jump",
            "-i",
            "/tmp/key",
            "-o",
            "StrictHostKeyChecking=no",
            "-A",
            "-tt",
            "user@workbox",
        ]))
        .unwrap();
        assert_eq!(parsed.target, "user@workbox");
        assert_eq!(
            parsed.ssh_args,
            strings(&[
                "-p",
                "2222",
                "-J",
                "jump",
                "-i",
                "/tmp/key",
                "-o",
                "StrictHostKeyChecking=no",
                "-A"
            ])
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_remote_commands_and_tunnels() {
        assert!(parse_interactive_ssh_args(&strings(&["workbox", "echo", "hi"])).is_none());
        assert!(parse_interactive_ssh_args(&strings(&["-N", "workbox"])).is_none());
        assert!(
            parse_interactive_ssh_args(&strings(&["-L", "8080:localhost:80", "workbox"])).is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn rejects_option_like_target_even_after_double_dash() {
        assert!(parse_interactive_ssh_args(&strings(&["--", "-weird-host"])).is_none());
    }

    #[cfg(unix)]
    #[test]
    fn rejects_no_tty_and_semantic_remote_command_options() {
        for args in [
            strings(&["-T", "workbox"]),
            strings(&["-vT", "workbox"]),
            strings(&["-o", "RequestTTY=no", "workbox"]),
            strings(&["-oRemoteCommand=uptime", "workbox"]),
            strings(&["-o", "SessionType=none", "workbox"]),
            strings(&["-o", "StdinNull=yes", "workbox"]),
            strings(&["-oStdinNull=true", "workbox"]),
            strings(&["-o", "LocalForward=8080 localhost:80", "workbox"]),
        ] {
            assert!(parse_interactive_ssh_args(&args).is_none(), "{args:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejects_user_managed_control_options() {
        for args in [
            strings(&["-S", "/tmp/control", "workbox"]),
            strings(&["-S/tmp/control", "workbox"]),
            strings(&["-o", "ControlMaster=auto", "workbox"]),
            strings(&["-oControlPath=/tmp/control", "workbox"]),
            strings(&["-o", "ControlPersist=60", "workbox"]),
            strings(&["-o", "StreamLocalBindMask=000", "workbox"]),
            strings(&["-oStreamLocalBindUnlink=no", "workbox"]),
        ] {
            assert!(parse_interactive_ssh_args(&args).is_none(), "{args:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn accepts_interactive_o_options() {
        let parsed = parse_interactive_ssh_args(&strings(&[
            "-o",
            "RequestTTY=force",
            "-oRemoteCommand=none",
            "workbox",
        ]))
        .unwrap();
        assert_eq!(parsed.target, "workbox");
        assert_eq!(
            parsed.ssh_args,
            strings(&["-o", "RequestTTY=force", "-oRemoteCommand=none"])
        );
    }

    #[test]
    fn accepts_interactive_effective_config() {
        let config = "\
host workbox
requesttty auto
sessiontype default
stdinnull no
forkafterauthentication no
permitlocalcommand no
tunnel false
";

        assert!(effective_config_allows_managed_shell(config));
    }

    #[test]
    fn rejects_noninteractive_effective_config() {
        for option in [
            "remotecommand uptime",
            "requesttty no",
            "sessiontype none",
            "sessiontype subsystem",
            "stdinnull yes",
            "forkafterauthentication yes",
            "localcommand notify-send connected",
            "permitlocalcommand yes",
            "tunnel point-to-point",
            "localforward 8080 [localhost]:80",
            "remoteforward 9090 [localhost]:90",
            "dynamicforward 1080",
            "stdioforwardhost localhost",
        ] {
            let config = format!("host workbox\n{option}\n");
            assert!(
                !effective_config_allows_managed_shell(&config),
                "accepted incompatible effective option {option:?}"
            );
        }
    }

    #[test]
    fn rejects_empty_or_malformed_effective_config() {
        assert!(!effective_config_allows_managed_shell(""));
        assert!(!effective_config_allows_managed_shell("host\n"));
    }

    #[cfg(unix)]
    #[test]
    fn command_status_timeout_is_bounded() {
        let mut command = Command::new("sh");
        command
            .args(["-c", "while :; do :; done"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let started = Instant::now();

        let status = command_status_with_timeout(&mut command, Duration::from_millis(20)).unwrap();

        assert!(status.is_none());
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_unexpected_managed_control_path_shapes() {
        for path in [
            Path::new("herdr-ssh-control-1/c"),
            Path::new("/tmp/other-control-1/c"),
            Path::new("/tmp/herdr-ssh-control-1/not-c"),
            Path::new("/tmp/herdr-ssh-control-/c"),
        ] {
            assert!(validate_managed_control_path(path).is_err(), "{path:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn validates_and_cleans_managed_control_socket() {
        use std::os::unix::net::UnixListener;

        let dir = TestDir::new(MANAGED_CONTROL_DIR_PREFIX.trim_end_matches('-'));
        let control_path = dir.path().join(MANAGED_CONTROL_SOCKET_NAME);
        let listener = UnixListener::bind(&control_path).unwrap();

        let validated = validate_managed_control_path(&control_path).unwrap();
        cleanup_managed_control_path(validated).unwrap();

        assert!(!dir.path().exists());
        drop(listener);
    }

    #[cfg(unix)]
    #[test]
    fn cleans_valid_control_directory_after_socket_exits() {
        use std::os::unix::net::UnixListener;

        let dir = TestDir::new(MANAGED_CONTROL_DIR_PREFIX.trim_end_matches('-'));
        let control_path = dir.path().join(MANAGED_CONTROL_SOCKET_NAME);
        let listener = UnixListener::bind(&control_path).unwrap();
        let validated = validate_managed_control_path(&control_path).unwrap();

        std::fs::remove_file(&control_path).unwrap();
        cleanup_managed_control_path(validated).unwrap();

        assert!(!dir.path().exists());
        drop(listener);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_non_socket_and_non_private_control_paths() {
        use std::os::unix::fs::PermissionsExt as _;
        use std::os::unix::net::UnixListener;

        let file_dir = TestDir::new(MANAGED_CONTROL_DIR_PREFIX.trim_end_matches('-'));
        let file_path = file_dir.path().join(MANAGED_CONTROL_SOCKET_NAME);
        std::fs::write(&file_path, b"not a socket").unwrap();
        assert!(validate_managed_control_path(&file_path).is_err());

        let public_dir = TestDir::new(MANAGED_CONTROL_DIR_PREFIX.trim_end_matches('-'));
        let public_path = public_dir.path().join(MANAGED_CONTROL_SOCKET_NAME);
        let listener = UnixListener::bind(&public_path).unwrap();
        std::fs::set_permissions(public_dir.path(), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        assert!(validate_managed_control_path(&public_path).is_err());
        drop(listener);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlinked_control_parent() {
        use std::os::unix::fs::{symlink, DirBuilderExt as _};
        use std::os::unix::net::UnixListener;

        let root = TestDir::new("herdr-ssh-control-test-root");
        let real_parent = root.path().join("real");
        std::fs::DirBuilder::new()
            .mode(PRIVATE_DIR_MODE)
            .create(&real_parent)
            .unwrap();
        let real_control_path = real_parent.join(MANAGED_CONTROL_SOCKET_NAME);
        let listener = UnixListener::bind(&real_control_path).unwrap();
        let linked_parent = root.path().join("herdr-ssh-control-linked");
        symlink(&real_parent, &linked_parent).unwrap();

        assert!(
            validate_managed_control_path(&linked_parent.join(MANAGED_CONTROL_SOCKET_NAME))
                .is_err()
        );
        drop(listener);
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_refuses_control_directories_with_other_entries() {
        use std::os::unix::net::UnixListener;

        let dir = TestDir::new(MANAGED_CONTROL_DIR_PREFIX.trim_end_matches('-'));
        let control_path = dir.path().join(MANAGED_CONTROL_SOCKET_NAME);
        let listener = UnixListener::bind(&control_path).unwrap();
        let validated = validate_managed_control_path(&control_path).unwrap();
        let unexpected_path = dir.path().join("keep");
        std::fs::write(&unexpected_path, b"keep").unwrap();

        assert!(cleanup_managed_control_path(validated).is_err());
        assert!(control_path.exists());
        assert!(unexpected_path.exists());
        drop(listener);
    }
}
