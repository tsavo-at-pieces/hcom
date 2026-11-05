//! Shared, disposable hooks/list results. No session or delivery state lives here.
//!
//! Unknown script launchers are deliberately uncached: their version/entrypoint
//! cannot identify the native image or materialized setup behind the launcher.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{CodexHookTrustEntry, LaunchCtx, per_run_home};

const SCHEMA: u32 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LauncherInputs {
    version: u32,
    native_binary: PathBuf,
    files: Vec<PathBuf>,
    #[serde(default)]
    environment: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cached {
    schema: u32,
    identity: String,
    entries: Vec<CodexHookTrustEntry>,
}

fn cache_dir() -> Option<PathBuf> {
    let root = std::env::var_os("HCOM_CODEX_CACHE_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .or_else(|| dirs::cache_dir().map(|p| p.join("hcom/codex-hooks/v1")))?;
    if !root.is_absolute() {
        return None;
    }
    // Tests must explicitly claim their disposable cache, never inherit a
    // user's XDG_CACHE_HOME (which may outlive an isolated HOME).
    #[cfg(test)]
    if !crate::paths::test_roots::is_registered(&root) {
        return None;
    }
    Some(root)
}

fn file_digest(path: &Path) -> Result<String> {
    anyhow::ensure!(
        fs::metadata(path)?.is_file(),
        "cache input is not a regular file"
    );
    let mut input = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        digest.update(&buffer[..n]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

fn native_image(path: &Path) -> bool {
    let mut magic = [0; 4];
    if File::open(path)
        .and_then(|mut f| f.read_exact(&mut magic))
        .is_err()
    {
        return false;
    }
    // ELF, PE, Mach-O (including universal images). Scripts require the
    // launcher input contract even when their --version is indistinguishable.
    magic == *b"\x7fELF"
        || magic[..2] == *b"MZ"
        || matches!(
            magic,
            [0xfe, 0xed, 0xfa, 0xce | 0xcf]
                | [0xce | 0xcf, 0xfa, 0xed, 0xfe]
                | [0xca, 0xfe, 0xba, 0xbe | 0xbf]
                | [0xbe | 0xbf, 0xba, 0xfe, 0xca]
        )
}

/// Include both the selected path and its final target: equal-version and
/// equal-content pin retargets still represent a different installation.
fn image_input(inputs: &mut BTreeMap<String, String>, label: &str, path: &Path) -> Result<()> {
    inputs.insert(format!("{label}.selected"), path.to_string_lossy().into());
    inputs.insert(
        format!("{label}.resolved"),
        fs::canonicalize(path)?.to_string_lossy().into(),
    );
    inputs.insert(format!("{label}.digest"), file_digest(path)?);
    Ok(())
}

/// Configs are content-keyed under logical labels, so byte-identical private
/// CODEX_HOMEs can reuse results. Missing vs empty are distinct; unreadable
/// inputs disable reuse. No config text (which may contain secrets) is stored.
fn config_input(inputs: &mut BTreeMap<String, String>, label: &str, path: &Path) -> Result<()> {
    let digest = match file_digest(path) {
        Ok(digest) => digest,
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|e| e.kind() == io::ErrorKind::NotFound) =>
        {
            "missing".into()
        }
        Err(error) => return Err(error),
    };
    inputs.insert(label.to_string(), digest);
    Ok(())
}

fn config_dir(inputs: &mut BTreeMap<String, String>, label: &str, root: &Path) -> Result<()> {
    for file in [
        "config.toml",
        "hooks.json",
        "requirements.toml",
        "managed_config.toml",
    ] {
        config_input(inputs, &format!("{label}/{file}"), &root.join(file))?;
    }
    // Named configurations and exec policy files can be created/deleted between
    // launches. Sort by logical name, without including room/session state.
    for suffix in ["", "rules"] {
        let dir = root.join(suffix);
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".toml") || (suffix == "rules" && name.ends_with(".rules")) {
                config_input(inputs, &format!("{label}/{suffix}/{name}"), &entry.path())?;
            }
        }
    }
    Ok(())
}

fn identity(ctx: &LaunchCtx, version: &str, declarations: &[String]) -> Result<String> {
    let mut inputs = BTreeMap::new();
    inputs.insert("schema".into(), SCHEMA.to_string());
    inputs.insert("hcom.version".into(), env!("CARGO_PKG_VERSION").into());
    inputs.insert("codex.version".into(), version.into());
    inputs.insert("declarations".into(), serde_json::to_string(declarations)?);
    // This is the command/environment the existing inspector actually uses,
    // not LaunchCtx.env's possibly different PATH. CODEX_HOME is overridden
    // explicitly in fetch_codex_hook_list_with_overrides.
    let codex = PathBuf::from(crate::terminal::which_bin("codex").context("codex not found")?);
    image_input(&mut inputs, "codex", &codex)?;
    image_input(&mut inputs, "hcom", &std::env::current_exe()?)?;
    // PTY declarations may invoke bare hcom, whose PATH image may differ from
    // the running supervisor. Include it in addition to the current image.
    if let Some(hcom) = crate::terminal::which_bin("hcom") {
        image_input(&mut inputs, "hook.hcom", Path::new(&hcom))?;
    }

    let manifest = std::env::var_os("HCOM_CODEX_CACHE_INPUTS").filter(|v| !v.is_empty());
    if let Some(path) = manifest {
        let path = PathBuf::from(path);
        anyhow::ensure!(path.is_absolute(), "launcher manifest must be absolute");
        config_input(&mut inputs, "launcher.manifest", &path)?;
        let manifest: LauncherInputs = serde_json::from_slice(&fs::read(path)?)?;
        anyhow::ensure!(manifest.version == SCHEMA, "unknown launcher input schema");
        anyhow::ensure!(
            manifest.native_binary.is_absolute() && native_image(&manifest.native_binary),
            "native image missing"
        );
        image_input(&mut inputs, "launcher.native", &manifest.native_binary)?;
        anyhow::ensure!(!manifest.files.is_empty(), "launcher setup inputs missing");
        for (i, file) in manifest.files.iter().enumerate() {
            anyhow::ensure!(file.is_absolute(), "launcher input must be absolute");
            image_input(&mut inputs, &format!("launcher.file.{i}"), file)?;
        }
        for name in manifest.environment {
            inputs.insert(
                format!("launcher.env.{name}"),
                format!("{:?}", std::env::var_os(name)),
            );
        }
    } else {
        anyhow::ensure!(native_image(&codex), "opaque launcher needs input manifest");
        // Without a producer contract we cannot prove relative config inputs
        // equivalent after relocating CODEX_HOME. Sharing HCOM_DIRs still works.
        let home = per_run_home(ctx);
        inputs.insert("home.path".into(), home.to_string_lossy().into());
        inputs.insert(
            "home.resolved".into(),
            crate::paths::resolve_deepest_existing(&home)
                .context("cannot resolve Codex home")?
                .to_string_lossy()
                .into(),
        );
        // Git worktrees may load trust-root config outside the cwd ancestry.
        // A launcher manifest must describe these extra inputs explicitly.
        anyhow::ensure!(
            !ctx.cwd.ancestors().any(|p| p.join(".git").is_file()),
            "worktree config needs input manifest"
        );
    }
    for (key, value) in std::env::vars_os() {
        let name = key.to_string_lossy();
        if matches!(
            name.as_ref(),
            "PATH"
                | "HOME"
                | "USERPROFILE"
                | "XDG_CONFIG_HOME"
                | "CODEX_PROFILE"
                | "CODEX_CONFIG_PATH"
        ) {
            inputs.insert(format!("env.{name}"), value.to_string_lossy().into());
        }
    }
    inputs.insert(
        "cwd".into(),
        fs::canonicalize(&ctx.cwd)?.to_string_lossy().into(),
    );
    inputs.insert(
        "inspection.cwd".into(),
        std::env::current_dir()?.to_string_lossy().into(),
    );
    // Configuration overrides are validation inputs; prompts and instance names
    // aren't. Keep original spelling/order, since later overrides win.
    inputs.insert(
        "overrides".into(),
        serde_json::to_string(&config_overrides(&ctx.args))?,
    );
    config_dir(&mut inputs, "home", &per_run_home(ctx))?;
    for (i, dir) in ctx.cwd.ancestors().enumerate() {
        config_dir(&mut inputs, &format!("project.{i}"), &dir.join(".codex"))?;
    }
    config_input(&mut inputs, "cwd.config", &ctx.cwd.join("config.toml"))?;
    #[cfg(unix)]
    config_dir(&mut inputs, "system", Path::new("/etc/codex"))?;
    #[cfg(windows)]
    {
        let root = std::env::var_os("ProgramData")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\ProgramData"))
            .join("OpenAI/Codex");
        inputs.insert("system.path".into(), root.to_string_lossy().into());
        config_dir(&mut inputs, "system", &root)?;
    }
    #[cfg(target_os = "macos")]
    {
        // Include conventional managed policy files. A producer must also
        // snapshot effective external policy not represented by these files.
        let managed = Path::new("/Library/Managed Preferences");
        for path in [
            managed.join("com.openai.codex.plist"),
            managed
                .join(std::env::var("USER").unwrap_or_default())
                .join("com.openai.codex.plist"),
        ] {
            config_input(&mut inputs, &format!("managed.{}", path.display()), &path)?;
        }
    }
    let encoded = serde_json::to_vec(&inputs)?;
    Ok(Sha256::digest(encoded)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

fn config_overrides(args: &[String]) -> Vec<String> {
    let mut selected = Vec::new();
    let mut i = 0;
    while i < args.len() && args[i] != "--" {
        let arg = &args[i];
        if ["-c", "--config", "-p", "--profile", "--enable", "--disable"].contains(&arg.as_str()) {
            selected.push(arg.clone());
            i += 1;
            if i < args.len() {
                selected.push(args[i].clone());
            }
        } else if [
            "-c=",
            "--config=",
            "-p=",
            "--profile=",
            "--enable=",
            "--disable=",
        ]
        .iter()
        .any(|prefix| arg.starts_with(prefix))
            || (arg.starts_with("-c") && arg.len() > 2)
        {
            selected.push(arg.clone());
        }
        i += 1;
    }
    selected
}

fn valid(entries: &[CodexHookTrustEntry], expected: &HashSet<String>) -> bool {
    entries.len() == expected.len()
        && entries
            .iter()
            .all(|entry| !entry.key.is_empty() && !entry.current_hash.is_empty())
        && entries
            .iter()
            .map(|entry| &entry.command)
            .collect::<HashSet<_>>()
            == expected.iter().collect()
        && entries
            .iter()
            .map(|entry| &entry.key)
            .collect::<HashSet<_>>()
            .len()
            == entries.len()
}

fn read_cache(
    path: &Path,
    identity: &str,
    expected: &HashSet<String>,
) -> Option<Vec<CodexHookTrustEntry>> {
    let cached: Cached = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
    (cached.schema == SCHEMA && cached.identity == identity && valid(&cached.entries, expected))
        .then_some(cached.entries)
}

fn lock_cache(dir: &Path, key: &str, timeout: std::time::Duration) -> io::Result<File> {
    fs::create_dir_all(dir)?;
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(dir.join(format!("{key}.lock")))?;
    lock_with_timeout(&file, timeout)?;
    Ok(file)
}

fn lock_with_timeout(file: &File, timeout: std::time::Duration) -> io::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(()),
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
            Err(std::fs::TryLockError::WouldBlock) => {
                if std::time::Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Codex validation cache lock busy",
                    ));
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
        }
    }
}

pub(super) fn load_or_fetch(
    ctx: &LaunchCtx,
    version: &str,
    declarations: &[String],
    expected: &HashSet<String>,
    fetch: impl FnOnce() -> Result<Vec<CodexHookTrustEntry>>,
) -> Result<Vec<CodexHookTrustEntry>> {
    cached_or_fetch(
        cache_dir(),
        || identity(ctx, version, declarations),
        expected,
        fetch,
    )
}

fn cached_or_fetch(
    dir: Option<PathBuf>,
    fingerprint: impl Fn() -> Result<String>,
    expected: &HashSet<String>,
    fetch: impl FnOnce() -> Result<Vec<CodexHookTrustEntry>>,
) -> Result<Vec<CodexHookTrustEntry>> {
    cached_or_fetch_with_wait(
        dir,
        fingerprint,
        expected,
        fetch,
        std::time::Duration::from_secs(5),
    )
}

fn cached_or_fetch_with_wait(
    dir: Option<PathBuf>,
    fingerprint: impl Fn() -> Result<String>,
    expected: &HashSet<String>,
    fetch: impl FnOnce() -> Result<Vec<CodexHookTrustEntry>>,
    timeout: std::time::Duration,
) -> Result<Vec<CodexHookTrustEntry>> {
    let location = dir.and_then(|dir| fingerprint().ok().map(|key| (dir, key)));
    // Holding this stable inode across read/inspect/publish collapses concurrent
    // misses across processes. Never unlink a lock: waiters may hold its inode.
    // An unwritable/unsupported cache only loses the optimization.
    let locked = location.and_then(|(dir, key)| {
        lock_cache(&dir, &key, timeout)
            .ok()
            .map(|lock| (dir, key, lock))
    });
    if let Some((dir, key, _lock)) = &locked {
        // Inputs may have changed while this process waited for another room.
        if fingerprint().ok().as_ref() != Some(key) {
            bail!("Codex setup changed while waiting for hook validation; retry launch");
        }
        if let Some(entries) = read_cache(&dir.join(format!("{key}.json")), key, expected) {
            return Ok(entries);
        }
    }
    let entries = fetch()?;
    anyhow::ensure!(
        valid(&entries, expected),
        "Codex hooks/list returned incomplete or duplicate hcom hook trust entries"
    );
    if let Some((dir, key, _lock)) = &locked {
        if fingerprint().ok().as_ref() != Some(key) {
            bail!("Codex setup changed during hook validation; retry launch");
        }
        let cached = Cached {
            schema: SCHEMA,
            identity: key.clone(),
            entries: entries.clone(),
        };
        // Cache writes are best effort; the successful fresh validation still
        // authorizes this launch. Atomic sibling-temp + fsync + rename means a
        // reader never observes partial JSON, including after writer crashes.
        let _ = crate::paths::atomic_write_io(
            &dir.join(format!("{key}.json")),
            &serde_json::to_string(&cached)?,
        );
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn expected() -> HashSet<String> {
        ["hcom codex-stop", "hcom codex-sessionstart"]
            .map(str::to_string)
            .into()
    }

    fn entries() -> Vec<CodexHookTrustEntry> {
        ["hcom codex-stop", "hcom codex-sessionstart"]
            .iter()
            .enumerate()
            .map(|(i, command)| CodexHookTrustEntry {
                key: format!("/<session-flags>/config.toml:{i}"),
                command: command.to_string(),
                current_hash: format!("hash-{i}"),
            })
            .collect()
    }

    #[test]
    fn rejects_semantically_invalid_entries() {
        let good = entries();
        assert!(valid(&good, &expected()));
        for index in 0..5 {
            let mut bad = good.clone();
            match index {
                0 => bad[1].command = bad[0].command.clone(),
                1 => bad[1].key = bad[0].key.clone(),
                2 => bad[1].key.clear(),
                3 => bad[1].current_hash.clear(),
                _ => bad[1].command = "someone else's command".into(),
            }
            assert!(!valid(&bad, &expected()));
        }
        assert!(!valid(&[], &expected()));
    }

    #[test]
    fn invalid_cache_and_failed_validation_are_retried() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.json");
        for contents in [
            "{partial",
            "[]",
            &serde_json::to_string(&Cached {
                schema: SCHEMA,
                identity: "key".into(),
                entries: vec![],
            })
            .unwrap(),
        ] {
            fs::write(&path, contents).unwrap();
            assert!(
                cached_or_fetch(
                    Some(dir.path().into()),
                    || Ok("key".into()),
                    &expected(),
                    || bail!("probe failed")
                )
                .is_err()
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), contents);
            assert!(
                cached_or_fetch(
                    Some(dir.path().into()),
                    || Ok("key".into()),
                    &expected(),
                    || Ok(vec![])
                )
                .is_err()
            );
            assert_eq!(fs::read_to_string(&path).unwrap(), contents);
            assert_eq!(
                cached_or_fetch(
                    Some(dir.path().into()),
                    || Ok("key".into()),
                    &expected(),
                    || Ok(entries())
                )
                .unwrap(),
                entries()
            );
            assert_eq!(
                cached_or_fetch(
                    Some(dir.path().into()),
                    || Ok("key".into()),
                    &expected(),
                    || panic!("cache miss")
                )
                .unwrap(),
                entries()
            );
        }
    }

    #[test]
    fn unavailable_cache_or_identity_still_validates() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-directory");
        fs::write(&file, "file").unwrap();
        for cache in [None, Some(file)] {
            assert_eq!(
                cached_or_fetch(cache, || Ok("key".into()), &expected(), || Ok(entries())).unwrap(),
                entries()
            );
        }
        assert_eq!(
            cached_or_fetch(
                Some(dir.path().into()),
                || bail!("identity unreadable"),
                &expected(),
                || Ok(entries())
            )
            .unwrap(),
            entries()
        );
        assert!(!dir.path().join("key.json").exists());
    }

    #[test]
    fn changing_inputs_during_inspection_never_publish() {
        let dir = tempfile::tempdir().unwrap();
        let calls = AtomicUsize::new(0);
        let error = cached_or_fetch(
            Some(dir.path().into()),
            || {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                Ok(if n < 2 { "before" } else { "after" }.into())
            },
            &expected(),
            || Ok(entries()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("changed during"));
        assert!(!dir.path().join("before.json").exists());
        assert!(!dir.path().join("after.json").exists());
    }

    #[test]
    fn content_and_selected_image_path_invalidate_without_version_change() {
        let dir = tempfile::tempdir().unwrap();
        let image = dir.path().join("image");
        fs::write(&image, b"\x7fELFone").unwrap();
        let snapshot = |path: &Path| {
            let mut inputs = BTreeMap::new();
            image_input(&mut inputs, "image", path).unwrap();
            inputs
        };
        let before = snapshot(&image);
        fs::write(&image, b"\x7fELFtwo").unwrap();
        assert_ne!(before, snapshot(&image));
        let other = dir.path().join("other");
        fs::copy(&image, &other).unwrap();
        assert_ne!(snapshot(&image), snapshot(&other));
        #[cfg(unix)]
        {
            let pin = dir.path().join("pin");
            std::os::unix::fs::symlink(&image, &pin).unwrap();
            let before = snapshot(&pin);
            fs::remove_file(&pin).unwrap();
            std::os::unix::fs::symlink(&other, &pin).unwrap();
            assert_ne!(before, snapshot(&pin), "same-content pin retarget");
        }
    }

    #[test]
    fn private_config_home_locations_share_but_changes_invalidate() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("home-a");
        let b = dir.path().join("home-b");
        let snapshot = |path: &Path| {
            let mut inputs = BTreeMap::new();
            config_dir(&mut inputs, "home", path).unwrap();
            inputs
        };
        for home in [&a, &b] {
            fs::create_dir_all(home.join("rules")).unwrap();
            fs::write(home.join("config.toml"), "model='x'").unwrap();
        }
        assert_eq!(snapshot(&a), snapshot(&b));
        fs::write(b.join("rules/hcom.rules"), "changed").unwrap();
        assert_ne!(snapshot(&a), snapshot(&b));
        fs::remove_file(b.join("rules/hcom.rules")).unwrap();
        fs::write(b.join("config.toml"), "model='y'").unwrap();
        assert_ne!(snapshot(&a), snapshot(&b));
    }

    #[test]
    fn config_flags_preserve_names_order_and_separator() {
        let args = |items: &[&str]| items.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_ne!(
            config_overrides(&args(&["--enable", "hooks"])),
            config_overrides(&args(&["--disable", "hooks"]))
        );
        assert_eq!(
            config_overrides(&args(&[
                "exec",
                "--profile=x",
                "-cmodel='x'",
                "--",
                "--enable",
                "ignored"
            ])),
            args(&["--profile=x", "-cmodel='x'"])
        );
    }

    #[test]
    fn busy_lock_times_out_without_unlinking_holder() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock_cache(dir.path(), "key", std::time::Duration::from_millis(30)).unwrap();
        let waiter = OpenOptions::new()
            .write(true)
            .open(dir.path().join("key.lock"))
            .unwrap();
        let start = std::time::Instant::now();
        let error = lock_with_timeout(&waiter, std::time::Duration::from_millis(30)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
        assert!(dir.path().join("key.lock").exists());
        let fetched = AtomicUsize::new(0);
        assert_eq!(
            cached_or_fetch_with_wait(
                Some(dir.path().into()),
                || Ok("key".into()),
                &expected(),
                || {
                    fetched.fetch_add(1, Ordering::SeqCst);
                    Ok(entries())
                },
                std::time::Duration::from_millis(30)
            )
            .unwrap(),
            entries()
        );
        assert_eq!(
            fetched.load(Ordering::SeqCst),
            1,
            "busy cache must perform fresh validation"
        );
        assert!(
            !dir.path().join("key.json").exists(),
            "unlocked fallback must not publish"
        );
        drop(held);
        lock_with_timeout(&waiter, std::time::Duration::from_millis(30)).unwrap();
    }

    // This same executable is used as an isolated cross-process lock worker.
    // The normal suite does nothing here; no inherited room is ever opened.
    #[test]
    fn cache_process_worker() {
        let Some(dir) = std::env::var_os("HCOM_CACHE_TEST_WORKER_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        cached_or_fetch(
            Some(dir.clone()),
            || Ok("key".into()),
            &expected(),
            || {
                if std::env::var_os("HCOM_CACHE_TEST_CRASH").is_some() {
                    std::process::exit(19);
                }
                use std::io::Write;
                let mut count = OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(dir.join("probes"))?;
                writeln!(count, "inspection")?;
                Ok(entries())
            },
        )
        .unwrap();
    }

    #[test]
    fn simultaneous_processes_share_one_probe_and_recover_dead_writer() {
        let dir = tempfile::tempdir().unwrap();
        let worker = || {
            let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
            cmd.args([
                "--exact",
                "hooks::codex::cache::tests::cache_process_worker",
                "--nocapture",
            ])
            .env("HCOM_CACHE_TEST_WORKER_DIR", dir.path())
            .env_remove("HCOM_CACHE_TEST_CRASH")
            .stdout(std::process::Stdio::null());
            cmd
        };
        let crashed = worker().env("HCOM_CACHE_TEST_CRASH", "1").status().unwrap();
        assert_eq!(crashed.code(), Some(19));
        assert!(!dir.path().join("key.json").exists());
        let children: Vec<_> = (0..12).map(|_| worker().spawn().unwrap()).collect();
        for mut child in children {
            assert!(child.wait().unwrap().success());
        }
        assert_eq!(
            fs::read_to_string(dir.path().join("probes")).unwrap(),
            "inspection\n"
        );
        assert!(read_cache(&dir.path().join("key.json"), "key", &expected()).is_some());
        assert!(
            dir.path().join("key.lock").exists(),
            "lock inode must survive publication"
        );
    }
}
