//! Shared runtime helpers for invoking hcom and locating tool config roots.

/// Cached hcom invocation prefix (computed once per process lifetime).
static HCOM_PREFIX: std::sync::LazyLock<Vec<String>> = std::sync::LazyLock::new(|| {
    if std::env::var("HCOM_DEV_ROOT").is_ok_and(|root| !root.is_empty()) {
        #[cfg(windows)]
        if let Ok(exe) = std::env::current_exe()
            && let Ok(resolved) = exe.canonicalize()
        {
            let resolved = crate::shared::platform::child_process_path(&resolved);
            return vec![resolved.to_string_lossy().replace('\\', "/")];
        }
        return vec!["hcom".into()];
    }

    vec!["hcom".into()]
});

/// True for uv's disposable `<cache>/archive-vN/<id>/bin/hcom` envs.
/// Require both the uv archive bucket and its tagged cache root: a venv in
/// an unrelated tagged cache (or a persistent uv tool env) is not ephemeral.
pub(crate) fn is_uvx_ephemeral_exe(exe: &std::path::Path) -> bool {
    let Some(env_root) = exe.parent().and_then(|bin| bin.parent()) else {
        return false;
    };
    let Some(bucket) = env_root.parent() else {
        return false;
    };
    let is_archive = bucket
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("archive-v"))
        .is_some_and(|version| !version.is_empty() && version.bytes().all(|b| b.is_ascii_digit()));
    env_root.join("pyvenv.cfg").is_file()
        && is_archive
        && bucket
            .parent()
            .is_some_and(|cache| cache.join("CACHEDIR.TAG").is_file())
}

/// Detect hcom invocation prefix based on execution context.
/// The canonical path of this hcom image, for per-run hooks that must run this
/// exact implementation (the stdio transports). `current_exe()` can be the
/// symlink hcom was invoked by; a link retargeted during a live session would
/// otherwise move that session's hooks to another image.
pub(crate) fn pinned_hook_executable() -> std::io::Result<String> {
    Ok(pinned_image(std::env::current_exe()?)
        .to_string_lossy()
        .into_owned())
}

/// The image behind `exe`, which may be the symlink hcom was invoked by.
fn pinned_image(exe: std::path::PathBuf) -> std::path::PathBuf {
    let exe = std::fs::canonicalize(&exe).unwrap_or(exe);
    #[cfg(windows)]
    let exe = crate::shared::platform::child_process_path(&exe);
    exe
}

pub(crate) fn get_hcom_prefix() -> Vec<String> {
    HCOM_PREFIX.clone()
}

/// Base directory for each tool's default config dir (`.claude/`, `.codex/`, ...)
/// when the tool's own env override is unset. HCOM_DIR only isolates hcom state;
/// it never relocates tool config.
pub(crate) fn tool_home() -> std::path::PathBuf {
    user_home().unwrap_or_default()
}

/// Fixtures explicitly scope cleanup so Windows platform-profile discovery
/// cannot touch the developer's configuration despite HOME/CODEX_HOME overrides.
pub(crate) fn hook_cleanup_allowed(path: &std::path::Path) -> bool {
    let root = std::env::var_os("HCOM_TEST_ROOT").map(std::path::PathBuf::from);
    let Some(root) = root else {
        return true;
    };
    match (
        crate::paths::resolve_deepest_existing(path),
        crate::paths::resolve_deepest_existing(&root),
    ) {
        (Some(path), Some(root)) => path.starts_with(root),
        _ => false,
    }
}

/// Parent of a non-default HCOM_DIR. Older hcom versions installed tool hooks,
/// plugins and config under `<this>/.<tool>/`; only legacy cleanup looks here.
pub(crate) fn legacy_tool_config_root() -> Option<std::path::PathBuf> {
    let env: std::collections::HashMap<String, String> = std::env::vars().collect();
    let cwd = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let (hcom_dir, _) = crate::paths::resolve_hcom_dir_from_env(&env, &cwd);
    let root = hcom_dir.parent()?.to_path_buf();
    let canonical = |p: &std::path::Path| p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
    let root_canonical = canonical(&root);
    let is_home = [user_home(), dirs::home_dir()]
        .into_iter()
        .flatten()
        .any(|home| canonical(&home) == root_canonical);
    (!is_home).then_some(root)
}

/// Every dir that may hold an hcom install for a tool, deduplicated: the
/// default `~/<dirname>`, `$<env_var>`, and the legacy `<HCOM_DIR parent>/<dirname>`.
pub(crate) fn tool_config_cleanup_dirs(dirname: &str, env_var: &str) -> Vec<std::path::PathBuf> {
    let candidates = [
        Some(tool_home().join(dirname)),
        std::env::var(env_var)
            .ok()
            .filter(|dir| !dir.is_empty())
            .map(std::path::PathBuf::from),
        legacy_tool_config_root().map(|root| root.join(dirname)),
    ];
    let mut dirs = Vec::new();
    for dir in candidates.into_iter().flatten() {
        if hook_cleanup_allowed(&dir) && !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    dirs
}

/// Build hcom command string for prompts, config, and hook commands.
pub(crate) fn build_hcom_command() -> String {
    get_hcom_prefix().join(" ")
}

/// Gemini / Antigravity shared config directory (`~/.gemini` or under `GEMINI_CLI_HOME`).
pub(crate) fn gemini_family_config_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("GEMINI_CLI_HOME")
        && !dir.is_empty()
    {
        return std::path::PathBuf::from(dir).join(".gemini");
    }
    tool_home().join(".gemini")
}

/// Every `.gemini` dir that may hold an hcom install: `~/.gemini`,
/// `$GEMINI_CLI_HOME/.gemini`, and the legacy `<HCOM_DIR parent>/.gemini`.
pub(crate) fn gemini_family_cleanup_dirs() -> Vec<std::path::PathBuf> {
    let mut dirs = vec![tool_home().join(".gemini")];
    for dir in [
        Some(gemini_family_config_dir()),
        legacy_tool_config_root().map(|r| r.join(".gemini")),
    ]
    .into_iter()
    .flatten()
    {
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    }
    dirs.into_iter()
        .filter(|dir| hook_cleanup_allowed(dir))
        .collect()
}

/// User home directory, honoring an explicit `HOME` override before falling back
/// to the platform default (`dirs::home_dir()` resolves `%USERPROFILE%` on Windows).
pub(crate) fn user_home() -> Option<std::path::PathBuf> {
    if let Ok(home) = std::env::var("HOME")
        && !home.is_empty()
    {
        return Some(std::path::PathBuf::from(home));
    }
    dirs::home_dir()
}

/// Cross-platform user config base directory.
///
/// Resolution order:
/// 1. `$XDG_CONFIG_HOME` (explicit override, all platforms)
/// 2. `$HOME/.config` — on every platform, including Windows and macOS
///
/// OpenCode and Kilo resolve their config directory via the `xdg-basedir` npm
/// package, which has no Windows- or macOS-specific branch at all: it always
/// resolves to `~/.config` (falling back to `$XDG_CONFIG_HOME` when set) on
/// every OS. There is no `%APPDATA%` or `~/Library/Application Support`
/// involved, so hcom must not special-case Windows here either.
pub(crate) fn user_config_home() -> Option<std::path::PathBuf> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg.is_empty()
    {
        return Some(std::path::PathBuf::from(xdg));
    }
    user_home().map(|h| h.join(".config"))
}

/// Cross-platform data directory for an OpenCode-family tool (`opencode`/`kilo`),
/// i.e. where the tool keeps its SQLite session DB.
///
/// Resolution order:
/// 1. `$XDG_DATA_HOME/<tool>` (explicit override, all platforms) — trusted
///    unconditionally, with no existence probe: an explicit override always
///    wins, even if the directory hasn't been created yet.
/// 2. `~/.local/share/<tool>` — on every platform, including Windows and macOS
///
/// Like [`user_config_home`], this follows OpenCode/Kilo's use of the
/// `xdg-basedir` npm package, which always resolves to `~/.local/share`
/// (falling back to `$XDG_DATA_HOME` when set) regardless of OS. There is no
/// `%APPDATA%` or Apple-style data dir involved, so `dirs::data_dir()` is
/// never a correct candidate for this tool family on any platform.
///
/// This is the single source of truth for opencode/kilo data-dir resolution,
/// shared by the hook dispatcher, the transcript search, and `resume`.
pub(crate) fn opencode_family_data_dir(tool: &str) -> Option<std::path::PathBuf> {
    if let Ok(xdg) = std::env::var("XDG_DATA_HOME")
        && !xdg.is_empty()
    {
        return Some(std::path::PathBuf::from(xdg).join(tool));
    }
    user_home().map(|h| h.join(".local/share").join(tool))
}

/// Resolve the SQLite database path for an OpenCode-family tool (`opencode`/`kilo`).
///
/// Builds on [`opencode_family_data_dir`]. Honors the tool's DB override
/// (`OPENCODE_DB` / `KILO_DB`): `:memory:` means no on-disk DB (returns `None`),
/// an absolute path is used as-is, and a relative path is joined onto the data
/// dir. Unset or empty defaults to `<data_dir>/<tool>.db`.
///
/// This performs path construction only — it does not check whether the
/// resulting path exists on disk. Callers that need "exists" semantics should
/// apply their own `.exists()` check.
pub(crate) fn opencode_family_db_path(tool: &str) -> Option<std::path::PathBuf> {
    let data_dir = opencode_family_data_dir(tool)?;
    let (env_var, default_name) = if tool == "kilo" {
        ("KILO_DB", "kilo.db")
    } else {
        ("OPENCODE_DB", "opencode.db")
    };
    match std::env::var(env_var)
        .ok()
        .filter(|value| !value.is_empty())
    {
        Some(value) if value == ":memory:" => None,
        Some(value) => {
            let path = std::path::PathBuf::from(value);
            Some(if path.is_absolute() {
                path
            } else {
                data_dir.join(path)
            })
        }
        None => Some(data_dir.join(default_name)),
    }
}

// Records the name each `set_terminal_title` call would have written, so
// tests can assert that a code path renames its pane. Under `cfg(test)` the
// escape codes are recorded instead of written: a test run must not repaint
// the developer's own terminal.
#[cfg(test)]
thread_local! {
    static LAST_TERMINAL_TITLE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

/// Take and clear the name of the last `set_terminal_title` call on this thread.
#[cfg(test)]
pub(crate) fn take_last_terminal_title() -> Option<String> {
    LAST_TERMINAL_TITLE.with(|cell| cell.borrow_mut().take())
}

/// Set terminal title via escape codes written to /dev/tty.
pub(crate) fn set_terminal_title(instance_name: &str) {
    #[cfg(test)]
    LAST_TERMINAL_TITLE.with(|cell| {
        *cell.borrow_mut() = Some(instance_name.to_string());
    });

    #[cfg(not(test))]
    {
        let title = format!("hcom: {}", instance_name);
        if let Ok(mut tty) = std::fs::OpenOptions::new().write(true).open("/dev/tty") {
            use std::io::Write;
            let _ = write!(tty, "\x1b]1;{}\x07\x1b]2;{}\x07", title, title);
        }
    }
}

/// Escape a filesystem path for embedding in a TOML basic (double-quoted) string.
/// Backslashes are doubled first, then quotes — order matters: the quote escape
/// itself introduces a backslash that must not be re-doubled.
pub(crate) fn toml_escape_path(path: &str) -> String {
    path.replace('\\', r"\\").replace('"', "\\\"")
}

#[cfg(test)]
mod escape_tests {
    #[test]
    fn toml_escape_path_doubles_backslashes_then_quotes() {
        assert_eq!(super::toml_escape_path(r"C:\foo\bar"), r"C:\\foo\\bar");
        assert_eq!(super::toml_escape_path(r#"a"b"#), r#"a\"b"#);
        assert_eq!(super::toml_escape_path(r#"C:\a"b"#), r#"C:\\a\"b"#);
    }
}

// Unix-only: these assert $HOME resolution and POSIX path canonicalization
// (Windows resolves USERPROFILE and prefixes canonical paths with \\?\).
#[cfg(all(test, unix))]
mod tests {
    use crate::hooks::test_helpers::{EnvGuard, EnvVarGuard};
    use serial_test::serial;

    #[test]
    #[serial]
    fn legacy_tool_config_root_none_for_default_hcom_dir() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("HCOM_DIR", home.join(".hcom"));
        }
        assert_eq!(super::legacy_tool_config_root(), None);

        unsafe { std::env::set_var("HCOM_DIR", "/") };
        assert_eq!(super::legacy_tool_config_root(), None);
    }

    #[test]
    #[serial]
    fn tool_home_ignores_project_local_hcom_dir() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let home = temp.path().join("home");
        let sandbox = workspace.join(".sandbox");
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&sandbox).unwrap();

        let prev_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(&workspace).unwrap();
        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("HCOM_DIR", ".sandbox/.hcom");
        }

        let tool_home = super::tool_home();
        let legacy = super::legacy_tool_config_root();
        let expected = sandbox.canonicalize().unwrap();

        std::env::set_current_dir(prev_cwd).unwrap();
        assert_eq!(tool_home, home);
        assert_eq!(legacy, Some(expected));
    }

    #[test]
    #[serial]
    fn user_home_prefers_home_env_when_set() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
        }

        assert_eq!(super::user_home(), Some(home));
    }

    #[test]
    #[serial]
    fn user_home_falls_through_when_home_env_empty() {
        let _guard = EnvGuard::new();

        unsafe {
            std::env::set_var("HOME", "");
        }

        // Empty HOME is treated as unset, so resolution falls through to
        // dirs::home_dir() rather than returning Some(PathBuf::from("")).
        assert_eq!(super::user_home(), dirs::home_dir());
    }

    #[test]
    #[serial]
    fn user_config_home_prefers_xdg_config_home_when_set() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let xdg = temp.path().join("xdg-config");
        std::fs::create_dir_all(&xdg).unwrap();

        unsafe {
            std::env::set_var("XDG_CONFIG_HOME", &xdg);
        }

        assert_eq!(super::user_config_home(), Some(xdg));
    }

    #[test]
    #[serial]
    fn user_config_home_empty_xdg_falls_back_to_dot_config() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
            std::env::set_var("XDG_CONFIG_HOME", "");
        }

        // Empty XDG_CONFIG_HOME is treated as unset, so resolution falls
        // back to $HOME/.config (the unix branch, given this test's cfg gate).
        assert_eq!(super::user_config_home(), Some(home.join(".config")));
    }

    #[test]
    #[serial]
    fn opencode_family_data_dir_prefers_xdg_data_home_when_it_exists() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let xdg_data = temp.path().join("xdg-data");
        let xdg_tool_dir = xdg_data.join("opencode");
        let local_share_tool_dir = home.join(".local/share/opencode");
        std::fs::create_dir_all(&xdg_tool_dir).unwrap();
        std::fs::create_dir_all(&local_share_tool_dir).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
        }
        let _xdg_guard = EnvVarGuard::set("XDG_DATA_HOME", xdg_data.to_str().unwrap());

        // XDG_DATA_HOME wins even though ~/.local/share/opencode also exists.
        assert_eq!(
            super::opencode_family_data_dir("opencode"),
            Some(xdg_tool_dir)
        );
    }

    #[test]
    #[serial]
    fn opencode_family_data_dir_trusts_nonexistent_xdg_candidate() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let local_share_tool_dir = home.join(".local/share/opencode");
        std::fs::create_dir_all(&local_share_tool_dir).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
        }
        // XDG_DATA_HOME is explicitly set but its opencode subdir does not
        // exist on disk yet. An explicit override must win unconditionally
        // rather than falling back to a stale ~/.local/share/opencode.
        let xdg_data_missing = temp.path().join("xdg-data-missing");
        let _xdg_guard = EnvVarGuard::set("XDG_DATA_HOME", xdg_data_missing.to_str().unwrap());

        assert_eq!(
            super::opencode_family_data_dir("opencode"),
            Some(xdg_data_missing.join("opencode"))
        );
    }

    #[test]
    #[serial]
    fn opencode_family_data_dir_empty_xdg_falls_back_to_local_share() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let local_share_tool_dir = home.join(".local/share/opencode");
        std::fs::create_dir_all(&local_share_tool_dir).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
        }
        let _xdg_guard = EnvVarGuard::set("XDG_DATA_HOME", "");

        // Empty XDG_DATA_HOME is treated as unset (no candidate added for it).
        assert_eq!(
            super::opencode_family_data_dir("opencode"),
            Some(local_share_tool_dir)
        );
    }

    #[test]
    #[serial]
    fn opencode_family_data_dir_returns_local_share_even_when_it_does_not_exist() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();

        unsafe {
            std::env::set_var("HOME", &home);
        }
        let _xdg_guard = EnvVarGuard::set("XDG_DATA_HOME", "");

        // ~/.local/share/opencode doesn't exist under this isolated HOME, but
        // it's still the only non-override candidate (OpenCode/Kilo never use
        // dirs::data_dir() on any platform), so it's returned unconditionally.
        assert_eq!(
            super::opencode_family_data_dir("opencode"),
            Some(home.join(".local/share/opencode"))
        );
    }

    #[test]
    #[serial]
    fn opencode_family_db_path_honors_per_tool_db_override() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let _xdg_guard = EnvVarGuard::set("XDG_DATA_HOME", temp.path().to_str().unwrap());
        let saved: Vec<_> = ["OPENCODE_DB", "KILO_DB"]
            .iter()
            .map(|k| (*k, std::env::var(k).ok()))
            .collect();
        let set = |k: &str, v: Option<&str>| unsafe {
            match v {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        };
        let abs = temp.path().join("elsewhere.db");
        for (tool, var) in [("opencode", "OPENCODE_DB"), ("kilo", "KILO_DB")] {
            let data = temp.path().join(tool);
            set(var, None);
            assert_eq!(
                super::opencode_family_db_path(tool),
                Some(data.join(format!("{tool}.db")))
            );
            set(var, Some(":memory:"));
            assert_eq!(super::opencode_family_db_path(tool), None);
            set(var, Some("custom.db"));
            assert_eq!(
                super::opencode_family_db_path(tool),
                Some(data.join("custom.db"))
            );
            set(var, Some(abs.to_str().unwrap()));
            assert_eq!(super::opencode_family_db_path(tool), Some(abs.clone()));
            set(var, None);
        }
        // Each tool reads only its own override.
        set("KILO_DB", Some(":memory:"));
        assert!(super::opencode_family_db_path("opencode").is_some());
        for (k, v) in saved {
            set(k, v.as_deref());
        }
    }

    /// Mirrors uv's on-disk layouts: every uv venv has `pyvenv.cfg` and its own
    /// `CACHEDIR.TAG`; only uvx's ephemeral envs sit under the tagged cache root.
    #[test]
    fn uvx_ephemeral_detection_matches_uv_layouts() {
        let temp = tempfile::tempdir().unwrap();
        let venv = |root: std::path::PathBuf| {
            std::fs::create_dir_all(root.join("bin")).unwrap();
            std::fs::write(root.join("pyvenv.cfg"), "uv = 0.7.13\n").unwrap();
            std::fs::write(root.join("CACHEDIR.TAG"), "").unwrap();
            root.join("bin/hcom")
        };

        let cache = temp.path().join(".cache/uv");
        std::fs::create_dir_all(&cache).unwrap();
        std::fs::write(cache.join("CACHEDIR.TAG"), "").unwrap();
        let ephemeral = venv(cache.join("archive-v0/abc123"));
        assert!(super::is_uvx_ephemeral_exe(&ephemeral));

        let tool = venv(temp.path().join(".local/share/uv/tools/hcom"));
        std::fs::write(tool.parent().unwrap().join("../uv-receipt.toml"), "").unwrap();
        assert!(!super::is_uvx_ephemeral_exe(&tool));

        let unrelated = venv(cache.join("other/venv"));
        assert!(!super::is_uvx_ephemeral_exe(&unrelated));
        let invalid_bucket = venv(cache.join("archive-version/venv"));
        assert!(!super::is_uvx_ephemeral_exe(&invalid_bucket));
        let brew = venv(temp.path().join("Cellar/hcom/0.7.30/libexec"));
        assert!(!super::is_uvx_ephemeral_exe(&brew));
        let pip = venv(temp.path().join("pip-venv"));
        std::fs::remove_file(pip.parent().unwrap().parent().unwrap().join("CACHEDIR.TAG")).unwrap();
        assert!(!super::is_uvx_ephemeral_exe(&pip));

        let project_venv = venv(temp.path().join("project/.venv"));
        assert!(!super::is_uvx_ephemeral_exe(&project_venv));

        let cargo_target = temp.path().join("target");
        std::fs::create_dir_all(cargo_target.join("debug")).unwrap();
        std::fs::write(cargo_target.join("CACHEDIR.TAG"), "").unwrap();
        assert!(!super::is_uvx_ephemeral_exe(
            &cargo_target.join("debug/hcom")
        ));
        assert!(!super::is_uvx_ephemeral_exe(std::path::Path::new(
            "/usr/local/bin/hcom"
        )));
    }
}

#[cfg(all(test, unix))]
mod pinned_image_tests {
    /// Linux's current_exe() is already resolved by the kernel, so only a path
    /// handed in directly proves the pin on every Unix; macOS exercises it end
    /// to end through tests/claude_stdio.rs.
    #[test]
    fn pinned_image_resolves_an_invoked_symlink() {
        let temp = tempfile::tempdir().unwrap();
        let image = temp.path().join("image").join("hcom");
        std::fs::create_dir_all(image.parent().unwrap()).unwrap();
        std::fs::write(&image, b"").unwrap();
        let link = temp.path().join("link-bin").join("hcom");
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&image, &link).unwrap();
        let pinned = super::pinned_image(link.clone());
        assert_eq!(pinned, std::fs::canonicalize(&image).unwrap());
        assert_ne!(pinned, link);
    }
}
