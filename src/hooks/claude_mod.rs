//! Native Claude wakes (default delivery). Classic hooks own the queue and acknowledgements.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::db::HcomDb;
use crate::hooks::runtime;
use crate::shared::context::HcomContext;
use crate::shared::time::now_epoch_f64;

const WAIT: Duration = Duration::from_secs(10);
// Allow the bounded wait, SQLite contention, and process startup to complete.
const HEALTH_TIMEOUT: f64 = 40.0;
// Match the mod's bounded BUSY retry window. Evidence expires if retries stop.
const BUSY_WINDOW: f64 = 60.0;
/// Time for Claude to start and load the mod before health is reported.
pub const CONNECT_GRACE: Duration = Duration::from_secs(15);
/// Launch env and `claude_delivery` config values; the launch freezes one.
pub const DELIVERY_ENV: &str = "HCOM_CLAUDE_DELIVERY";
const INJECT_HINT: &str = "hcom config claude_delivery inject";

/// Whether this process was launched with native (mod) delivery.
pub fn launched_native() -> bool {
    std::env::var(DELIVERY_ENV).as_deref() == Ok("native")
}

/// There is no automatic PTY fallback; every failure names the opt-out instead.
fn opt_out(name: &str) -> String {
    format!(
        "messages stay queued until a prompt is typed in Claude; to use inject delivery: \
         {INJECT_HINT}, then hcom kill {name} and hcom r {name}"
    )
}

/// Managed `disableSideloadFlags` makes Claude exit at startup on hcom's
/// `--plugin-dir`. Recognized from Claude's own message, which covers every
/// managed source (files, MDM, server-managed) without reading any of them.
pub fn launch_failure_hint(output: &str) -> Option<String> {
    sideload_hint(launched_native(), output)
}

fn sideload_hint(native: bool, output: &str) -> Option<String> {
    // The screen tail wraps long lines, so match the unwrapped text.
    let unwrapped: String = output.chars().filter(|c| !c.is_whitespace()).collect();
    (native && unwrapped.contains("disableSideloadFlags")).then(|| {
        format!(
            "hcom: Claude's managed settings (disableSideloadFlags) block the --plugin-dir \
             that native delivery needs. Switch to inject delivery: {INJECT_HINT}"
        )
    })
}

#[derive(Serialize, Deserialize)]
struct ModState {
    name: String,
    /// Time of the last fully successful poll cycle; 0 until the first one.
    checked_at: f64,
    error: Option<String>,
    submission_failed: bool,
    // Survives module reloads: never enqueue repeated wakes without hook progress.
    attempted_cursor: Option<i64>,
    #[serde(default)]
    attempted_at: Option<f64>,
    /// First load for this session; the first cycle may wait up to WAIT.
    #[serde(default)]
    started_at: f64,
}

pub fn session_prefix(session: &str) -> String {
    format!("claude_mod:{session}:")
}

fn key(session: &str, process: &str) -> String {
    format!("{}{process}", session_prefix(session))
}

fn state(db: &HcomDb, session: &str, process: &str) -> Result<Option<ModState>> {
    db.kv_get(&key(session, process))?
        .map(|value| serde_json::from_str(&value).map_err(Into::into))
        .transpose()
}

// SQLite cannot record health evidence while its writer is locked. This file
// is diagnostic only: never let its I/O affect polling or delivery.
fn busy_path(db_path: &Path, session: &str, process: &str) -> PathBuf {
    let digest = runtime::content_digest(&[
        ("db", db_path.as_os_str().as_encoded_bytes()),
        ("session", session.as_bytes()),
        ("process", process.as_bytes()),
    ]);
    db_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".tmp/claude-mod-busy")
        .join(format!("{digest}.json"))
}

fn mark_busy(db_path: &Path, session: &str, process: &str) {
    let path = busy_path(db_path, session, process);
    if let Some(parent) = path.parent() {
        let _ = crate::paths::ensure_private_directory(parent);
    }
    let _ = crate::paths::atomic_write_io(
        &path,
        &serde_json::json!({"kind": "busy", "at": now_epoch_f64()}).to_string(),
    );
}

fn clear_busy(db_path: &Path, session: &str, process: &str) {
    let _ = std::fs::remove_file(busy_path(db_path, session, process));
}

fn recently_busy(db_path: &Path, session: &str, process: &str, checked_at: f64) -> bool {
    let read = || -> Option<bool> {
        let value: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(busy_path(db_path, session, process)).ok()?,
        )
        .ok()?;
        let at = value.get("at")?.as_f64()?;
        let age = now_epoch_f64() - at;
        Some(
            value.get("kind")?.as_str()? == "busy"
                && at > checked_at
                && (0.0..=BUSY_WINDOW).contains(&age),
        )
    };
    read().unwrap_or(false)
}

fn log(event: &str, session: &str, process: &str, name: &str, detail: &str) {
    crate::log::log_with_fields(
        if event.contains("failed") || event.contains("unavailable") {
            "WARN"
        } else {
            "INFO"
        },
        "hooks",
        event,
        detail,
        &[
            ("session_id", session),
            ("process_id", process),
            ("instance", name),
        ],
    );
}

/// Surface mod health without changing the transport or consuming any messages.
pub fn report_health(db: &HcomDb, process: &str, name: &str) -> Result<()> {
    let Some(row) = db.get_instance_full(name)? else {
        return Ok(());
    };
    // Active/blocked hook status already explains why idle delivery is waiting.
    if !db.is_idle(name) {
        return Ok(());
    }
    let session = row.session_id.as_deref().unwrap_or("");
    let current = state(db, session, process)?;
    let (context, detail) = match current.as_ref() {
        // Error first: a mod that fails every cycle never refreshes checked_at.
        Some(s) if s.error.is_some() => (
            "tui:mod-failed",
            format!(
                "mod error: {}; {}",
                s.error.as_deref().unwrap(),
                opt_out(name)
            ),
        ),
        _ if recently_busy(
            db.path(),
            session,
            process,
            current.as_ref().map_or(0.0, |s| s.checked_at),
        ) =>
        {
            (
                "tui:mod-busy",
                "Claude mod is retrying SQLite contention; messages remain queued".into(),
            )
        }
        // A new session (/clear, /resume) has not finished its first wait yet.
        Some(s) if s.checked_at == 0.0 && s.started_at + HEALTH_TIMEOUT >= now_epoch_f64() => {
            ("", String::new())
        }
        Some(s) if s.checked_at == 0.0 || s.checked_at + HEALTH_TIMEOUT < now_epoch_f64() => (
            "tui:mod-unavailable",
            format!(
                "Claude mod {}; check hcom.log; {}",
                if s.checked_at == 0.0 {
                    "has not completed a poll"
                } else {
                    "stopped polling"
                },
                opt_out(name)
            ),
        ),
        None => (
            "tui:mod-unavailable",
            format!(
                "Claude mod never connected (Claude Code older than 2.1.287, \
                 allowManagedModsOnly, or mods switched off by Anthropic); {}",
                opt_out(name)
            ),
        ),
        Some(s) if db.has_pending(name) && s.attempted_cursor == Some(db.get_cursor(name)) => {
            if s.attempted_at.is_some_and(|at| now_epoch_f64() - at > 10.0) {
                (
                    "tui:mod-unacknowledged",
                    format!(
                        "Claude has not acknowledged the mod wake (a menu may be open); {}",
                        opt_out(name)
                    ),
                )
            } else {
                (
                    "tui:mod-waiting",
                    "mod wake queued; waiting for Claude to accept it".into(),
                )
            }
        }
        Some(_) if db.has_pending(name) => (
            "tui:mod-waiting",
            "waiting for the mod to submit a wake".into(),
        ),
        Some(_) => ("", String::new()),
    };
    let detail = detail.as_str();
    if context.is_empty() && !row.status_context.starts_with("tui:mod-") {
        return Ok(());
    }
    if row.status_context != context || row.status_detail != detail {
        // The row may have left listening since is_idle; don't log a no-op.
        let applied = if context.is_empty() {
            db.clear_gate_status(name, &row.status_context)?;
            true
        } else {
            db.set_gate_status(name, context, detail)?
        };
        if !applied {
            return Ok(());
        }
        let event = if context.is_empty() {
            "claude.mod_ready".to_string()
        } else {
            format!(
                "claude.{}",
                context.trim_start_matches("tui:").replace('-', "_")
            )
        };
        log(&event, session, process, name, detail);
    }
    Ok(())
}

pub fn publish() -> Result<PathBuf> {
    Ok(runtime::publish_dir(
        "claude-mod",
        &[
            (
                ".claude-plugin/plugin.json",
                include_bytes!("../claude_mod/.claude-plugin/plugin.json"),
            ),
            (
                "hooks/hooks.json",
                include_bytes!("../claude_mod/hooks/hooks.json"),
            ),
            (
                "hooks/register.js",
                include_bytes!("../claude_mod/hooks/register.js"),
            ),
        ],
    )?)
}

pub fn dispatch(args: &[String]) -> (i32, String) {
    let result = poll(args);
    // Outcome reports are not poll cycles, and must not clear BUSY evidence.
    if !args.iter().any(|arg| arg == "--event")
        && let Some(session) = args.windows(2).find(|pair| pair[0] == "--session-id")
        && let Some(process) = HcomContext::from_os().process_id.as_deref()
    {
        let db_path = crate::paths::db_path();
        if result.as_ref().is_err_and(is_busy) {
            mark_busy(&db_path, &session[1], process);
        } else {
            clear_busy(&db_path, &session[1], process);
        }
    }
    poll_result(result)
}

fn is_busy(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(code, _))
                if matches!(code.code, rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
        )
    })
}

fn poll_result(result: Result<serde_json::Value>) -> (i32, String) {
    match result {
        Ok(value) => (0, value.to_string()),
        Err(error) if is_busy(&error) => {
            crate::log::log_info("hooks", "claude.mod_poll_busy", &error.to_string());
            (
                75,
                serde_json::json!({"busy": true, "error": error.to_string()}).to_string(),
            )
        }
        Err(error) => {
            crate::log::log_warn("hooks", "claude.mod_poll_failed", &error.to_string());
            (
                1,
                serde_json::json!({"error": error.to_string()}).to_string(),
            )
        }
    }
}

fn poll(args: &[String]) -> Result<serde_json::Value> {
    let ctx = HcomContext::from_os();
    let session = args
        .windows(2)
        .find(|pair| pair[0] == "--session-id")
        .map(|pair| pair[1].as_str())
        .ok_or_else(|| anyhow::anyhow!("Missing --session-id"))?;
    let Some(process) = ctx.process_id.as_deref() else {
        return Ok(serde_json::json!({"wake": false}));
    };
    let mut db = HcomDb::open()?;
    if let Some(event) = args
        .windows(2)
        .find(|pair| pair[0] == "--event")
        .map(|pair| pair[1].as_str())
    {
        let detail = args
            .windows(2)
            .find(|pair| pair[0] == "--detail")
            .map(|pair| pair[1].as_str())
            .unwrap_or("");
        let event = if event == "poll-failed" && args.iter().any(|arg| arg == "--busy") {
            "poll-busy"
        } else {
            event
        };
        record_report(&db, session, process, event, detail)?;
        if let Some(name) = validated_owner(&db, session, process)? {
            crate::notify::wake(&db, &name, &[crate::notify::WakeKind::Plugin]);
        }
        return Ok(serde_json::json!({"wake": false}));
    }

    let Some(current) = load(&db, session, process)? else {
        return Ok(serde_json::json!({"wake": false}));
    };
    let name = &current.name;
    db.update_heartbeat(name)?;

    let notify = crate::notify::NotifyServer::new()?;
    db.upsert_notify_endpoint(name, "plugin", notify.port())?;
    let observe = args.iter().any(|arg| arg == "--observe");
    if observe || !wake_due(&db, name, &current) {
        notify.wait(WAIT);
    }
    db.reconnect_if_stale();
    db.conn().execute(
        "DELETE FROM notify_endpoints WHERE instance = ?1 AND kind = 'plugin' AND port = ?2",
        rusqlite::params![name, notify.port()],
    )?;
    // Last step: a claimed wake is only committed with the healthy cycle.
    let wake = finish(&db, session, process, !observe)?;
    Ok(serde_json::json!({"wake": wake}))
}

/// Records a mod report. Reports never count as a successful cycle, so they
/// neither refresh liveness nor clear an error.
fn record_report(
    db: &HcomDb,
    session: &str,
    process: &str,
    event: &str,
    detail: &str,
) -> Result<()> {
    db.with_write_scope(|| {
        let Some(mut current) = load(db, session, process)? else {
            log(&format!("claude.mod_{event}"), session, process, "", detail);
            return Ok(());
        };
        if event == "poll-busy" {
            mark_busy(db.path(), session, process);
        }
        // Claude holds a submitted wake until the session is idle. When a turn
        // delivered the batch first (load() just saw the cursor move), the
        // hook blocks the late wake as having nothing pending: no failure.
        if event == "submission-failed" && current.attempted_cursor.is_none() {
            log(
                "claude.mod_stale_wake",
                session,
                process,
                &current.name,
                detail,
            );
            return report_health(db, process, &current.name);
        }
        if event == "submission-failed" || (event == "poll-failed" && !current.submission_failed) {
            current.error = Some(detail.chars().take(1000).collect());
            current.submission_failed = event == "submission-failed";
            db.kv_set(
                &key(session, process),
                Some(&serde_json::to_string(&current)?),
            )?;
        }
        log(
            &format!("claude.mod_{event}"),
            session,
            process,
            &current.name,
            detail,
        );
        report_health(db, process, &current.name)
    })
}

// Short write scopes serialize overlapping polls after a module reload. Never
// hold a database lock while waiting on TCP or on Claude's prompt submission.
// Loading records hook progress only; health changes belong to finish().
fn load(db: &HcomDb, session: &str, process: &str) -> Result<Option<ModState>> {
    db.with_write_scope(|| {
        // Only root hooks establish ownership; mod arguments cannot bind a session.
        let Some(name) = validated_owner(db, session, process)? else {
            return Ok(None);
        };
        let mut current = state(db, session, process)?
            .filter(|l| l.name == name)
            .unwrap_or(ModState {
                name,
                checked_at: 0.0,
                error: None,
                submission_failed: false,
                attempted_cursor: None,
                attempted_at: None,
                started_at: now_epoch_f64(),
            });
        if current
            .attempted_cursor
            .is_some_and(|cursor| cursor != db.get_cursor(&current.name))
        {
            log(
                "claude.mod_acknowledged",
                session,
                process,
                &current.name,
                "hook advanced the message cursor",
            );
            current.attempted_cursor = None;
            current.attempted_at = None;
            // Hook progress proves Claude accepts prompts again; the error
            // itself stays until a full poll cycle succeeds.
            current.submission_failed = false;
        }
        db.kv_set(
            &key(session, process),
            Some(&serde_json::to_string(&current)?),
        )?;
        Ok(Some(current))
    })
}

/// Completes a poll cycle atomically: claims a due wake, marks the mod healthy,
/// and clears a poll error. Any failure rolls all of it back.
fn finish(db: &HcomDb, session: &str, process: &str, claim: bool) -> Result<bool> {
    db.with_write_scope(|| {
        // /resume, /clear, stop, or reset can happen while the listener waits.
        let state_key = key(session, process);
        let Some(mut current) = load(db, session, process)? else {
            db.kv_set(&state_key, None)?;
            return Ok(false);
        };
        let name = current.name.clone();
        if current.checked_at == 0.0 {
            log(
                "claude.mod_connected",
                session,
                process,
                &name,
                "native delivery connected",
            );
        }
        current.checked_at = now_epoch_f64();
        if !current.submission_failed {
            current.error = None;
        }
        let wake = claim && wake_due(db, &name, &current);
        if wake {
            current.attempted_cursor = Some(db.get_cursor(&name));
            current.attempted_at = Some(now_epoch_f64());
            log(
                "claude.mod_wake_requested",
                session,
                process,
                &name,
                "native wake requested; queue awaits hook acknowledgement",
            );
        }
        db.kv_set(&state_key, Some(&serde_json::to_string(&current)?))?;
        report_health(db, process, &name)?;
        Ok(wake)
    })
}

fn wake_due(db: &HcomDb, name: &str, current: &ModState) -> bool {
    !current.submission_failed
        && db.is_idle(name)
        && db.has_pending(name)
        && current.attempted_cursor != Some(db.get_cursor(name))
}

fn validated_owner(db: &HcomDb, session: &str, process: &str) -> Result<Option<String>> {
    let Some((bound_session, name)) = db.get_process_binding_full(process)? else {
        return Ok(None);
    };
    if bound_session.as_deref() != Some(session)
        || db.get_validated_claude_session_owner(session)?.as_deref() != Some(&name)
    {
        return Ok(None);
    }
    Ok(db
        .get_instance_full(&name)?
        .filter(|row| row.tool == "claude" && row.session_id.as_deref() == Some(session))
        .map(|_| name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_contention_is_a_transient_poll_result() {
        for code in [rusqlite::ffi::SQLITE_BUSY, rusqlite::ffi::SQLITE_LOCKED] {
            let error = anyhow::Error::from(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(code),
                Some("database is locked".into()),
            ))
            .context("poll write failed");
            let (exit, output) = poll_result(Err(error));
            assert_eq!(exit, 75);
            let value: serde_json::Value = serde_json::from_str(&output).unwrap();
            assert_eq!(value["busy"], true);
        }
        assert_eq!(poll_result(Err(anyhow::anyhow!("notify failed"))).0, 1);
    }

    #[test]
    fn recent_busy_health_has_no_inject_hint_and_does_not_refresh_success() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        let mut current = state(&db, "session-1", "process-1").unwrap().unwrap();
        current.checked_at -= HEALTH_TIMEOUT + 1.0;
        let checked_at = current.checked_at;
        db.kv_set(
            &key("session-1", "process-1"),
            Some(&serde_json::to_string(&current).unwrap()),
        )
        .unwrap();
        record_report(
            &db,
            "session-1",
            "process-1",
            "poll-busy",
            "database is locked",
        )
        .unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-busy");
        assert!(!row.status_detail.contains(INJECT_HINT));
        assert_eq!(
            state(&db, "session-1", "process-1")
                .unwrap()
                .unwrap()
                .checked_at,
            checked_at
        );
        assert_eq!(db.get_cursor("nova"), 0);
    }

    #[test]
    fn busy_evidence_survives_a_locked_writer_but_expires_when_retries_stop() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        let mut current = state(&db, "session-1", "process-1").unwrap().unwrap();
        current.checked_at -= BUSY_WINDOW + 1.0;
        db.kv_set(
            &key("session-1", "process-1"),
            Some(&serde_json::to_string(&current).unwrap()),
        )
        .unwrap();
        let writer = HcomDb::open_raw(db.path()).unwrap();
        writer.conn().execute_batch("BEGIN IMMEDIATE").unwrap();
        mark_busy(db.path(), "session-1", "process-1");
        assert!(recently_busy(
            db.path(),
            "session-1",
            "process-1",
            current.checked_at
        ));
        assert!(!recently_busy(db.path(), "session-2", "process-1", 0.0));
        assert!(!recently_busy(db.path(), "session-1", "process-2", 0.0));
        assert!(!recently_busy(
            &db.path().with_file_name("other.db"),
            "session-1",
            "process-1",
            0.0
        ));
        writer.conn().execute_batch("ROLLBACK").unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            "tui:mod-busy"
        );
        std::fs::write(
            busy_path(db.path(), "session-1", "process-1"),
            serde_json::json!({"kind":"busy", "at": now_epoch_f64() - BUSY_WINDOW - 1.0})
                .to_string(),
        )
        .unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-unavailable");
        assert!(row.status_detail.contains(INJECT_HINT));
        assert!(row.status_detail.contains("stopped polling"));
    }

    #[test]
    fn real_errors_take_precedence_over_busy_and_a_healthy_cycle_clears_it() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        mark_busy(db.path(), "session-1", "process-1");
        record_report(
            &db,
            "session-1",
            "process-1",
            "poll-failed",
            "notify bind failed",
        )
        .unwrap();
        record_report(
            &db,
            "session-1",
            "process-1",
            "poll-busy",
            "database is locked",
        )
        .unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-failed");
        assert!(row.status_detail.contains("notify bind failed"));
        assert!(row.status_detail.contains(INJECT_HINT));
        finish(&db, "session-1", "process-1", false).unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            ""
        );
        clear_busy(db.path(), "session-1", "process-1");
        assert!(!busy_path(db.path(), "session-1", "process-1").exists());
    }

    #[test]
    fn invalid_or_unwritable_busy_markers_do_not_change_poll_health() {
        let (dir, db) = database();
        let path = busy_path(db.path(), "session-1", "process-1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "invalid JSON").unwrap();
        assert!(!recently_busy(db.path(), "session-1", "process-1", 0.0));
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();
        mark_busy(db.path(), "session-1", "process-1");
        clear_busy(db.path(), "session-1", "process-1");
        assert!(!recently_busy(db.path(), "session-1", "process-1", 0.0));
        let blocked_db = dir.path().join("not-a-directory/hcom.db");
        std::fs::write(blocked_db.parent().unwrap(), "file").unwrap();
        mark_busy(&blocked_db, "session-1", "process-1");
    }

    fn database() -> (tempfile::TempDir, HcomDb) {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_raw(&dir.path().join("hcom.db")).unwrap();
        db.init_db().unwrap();
        db.conn().execute(
            "INSERT INTO instances (name, tool, session_id, status, created_at) VALUES ('nova', 'claude', 'session-1', 'listening', 0)", [],
        ).unwrap();
        db.set_session_binding("session-1", "nova").unwrap();
        db.set_process_binding("process-1", "session-1", "nova")
            .unwrap();
        (dir, db)
    }

    #[test]
    fn sideload_refusal_names_inject_delivery_only_for_native_launches() {
        let output = "--plugin-dir is disabled by your organization's managed settings \
                      (disableSideloadFlags)";
        assert!(sideload_hint(true, output).unwrap().contains(INJECT_HINT));
        assert!(sideload_hint(true, "settings (disableSideloa\ndFlags)").is_some());
        assert!(sideload_hint(false, output).is_none());
        assert!(sideload_hint(true, "Error: unknown option").is_none());
    }

    #[test]
    fn submission_failure_stays_visible_and_never_claims_another_wake() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        let mut current = load(&db, "session-1", "process-1").unwrap().unwrap();
        current.submission_failed = true;
        current.error = Some("submission denied".into());
        db.kv_set(
            &key("session-1", "process-1"),
            Some(&serde_json::to_string(&current).unwrap()),
        )
        .unwrap();
        // A healthy cycle does not hide a submission failure.
        finish(&db, "session-1", "process-1", false).unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-failed");
        assert!(row.status_detail.contains("submission denied"));
        assert!(row.status_detail.contains("hcom kill nova and hcom r nova"));
        assert!(!finish(&db, "session-1", "process-1", true).unwrap());
    }

    #[test]
    fn hook_progress_clears_a_submission_failure() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        for _ in 0..2 {
            db.conn().execute(
                "INSERT INTO events (type,timestamp,instance,data) VALUES ('message','2026-01-01T00:00:00Z','luna','{\"from\":\"luna\",\"text\":\"hello\",\"scope\":\"broadcast\"}')", [],
            ).unwrap();
        }
        finish(&db, "session-1", "process-1", false).unwrap();
        assert!(finish(&db, "session-1", "process-1", true).unwrap());
        let mut current = state(&db, "session-1", "process-1").unwrap().unwrap();
        current.submission_failed = true;
        current.error = Some("wake dropped".into());
        db.kv_set(
            &key("session-1", "process-1"),
            Some(&serde_json::to_string(&current).unwrap()),
        )
        .unwrap();
        // The user's own prompt delivers through the hook.
        db.advance_instance_cursor("nova", 1).unwrap();
        let current = load(&db, "session-1", "process-1").unwrap().unwrap();
        assert!(!current.submission_failed);
        // The error stays visible until a whole poll cycle succeeds.
        assert!(current.error.is_some());
        assert!(finish(&db, "session-1", "process-1", true).unwrap());
        assert_eq!(
            state(&db, "session-1", "process-1").unwrap().unwrap().error,
            None
        );
    }

    #[test]
    fn a_late_wake_dropped_after_the_hook_delivered_is_not_a_failure() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        db.conn().execute(
            "INSERT INTO events (type,timestamp,instance,data) VALUES ('message','2026-01-01T00:00:00Z','luna','{\"from\":\"luna\",\"text\":\"hello\",\"scope\":\"broadcast\"}')", [],
        ).unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        assert!(finish(&db, "session-1", "process-1", true).unwrap());
        // A turn Claude started on its own delivers the batch while the wake
        // waits; the wake then comes in with nothing pending and is blocked.
        db.advance_instance_cursor("nova", 1).unwrap();
        record_report(
            &db,
            "session-1",
            "process-1",
            "submission-failed",
            "wake dropped",
        )
        .unwrap();
        let current = state(&db, "session-1", "process-1").unwrap().unwrap();
        assert!(!current.submission_failed);
        assert_eq!(current.error, None);
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            ""
        );
        // The next message still gets a wake.
        db.conn().execute(
            "INSERT INTO events (type,timestamp,instance,data) VALUES ('message','2026-01-01T00:00:01Z','luna','{\"from\":\"luna\",\"text\":\"again\",\"scope\":\"broadcast\"}')", [],
        ).unwrap();
        assert!(finish(&db, "session-1", "process-1", true).unwrap());
    }

    #[test]
    fn a_dropped_wake_still_awaiting_its_hook_is_a_failure() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        db.conn().execute(
            "INSERT INTO events (type,timestamp,instance,data) VALUES ('message','2026-01-01T00:00:00Z','luna','{\"from\":\"luna\",\"text\":\"hello\",\"scope\":\"broadcast\"}')", [],
        ).unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        assert!(finish(&db, "session-1", "process-1", true).unwrap());
        record_report(
            &db,
            "session-1",
            "process-1",
            "submission-failed",
            "wake dropped",
        )
        .unwrap();
        assert!(
            state(&db, "session-1", "process-1")
                .unwrap()
                .unwrap()
                .submission_failed
        );
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            "tui:mod-failed"
        );
    }

    #[test]
    fn poll_errors_clear_only_after_a_full_successful_cycle() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        let mut current = load(&db, "session-1", "process-1").unwrap().unwrap();
        current.error = Some("notify bind failed".into());
        db.kv_set(
            &key("session-1", "process-1"),
            Some(&serde_json::to_string(&current).unwrap()),
        )
        .unwrap();
        // The start of the next cycle, which then fails again, keeps the error.
        load(&db, "session-1", "process-1").unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-failed");
        assert!(row.status_detail.contains("notify bind failed"));
        // A late failure in finish rolls back instead of clearing the error.
        db.conn()
            .execute_batch("CREATE TRIGGER fail_health BEFORE UPDATE ON instances BEGIN SELECT RAISE(ABORT, 'disk'); END;")
            .unwrap();
        assert!(finish(&db, "session-1", "process-1", false).is_err());
        assert!(
            state(&db, "session-1", "process-1")
                .unwrap()
                .unwrap()
                .error
                .is_some()
        );
        db.conn().execute_batch("DROP TRIGGER fail_health").unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        assert_eq!(
            state(&db, "session-1", "process-1").unwrap().unwrap().error,
            None
        );
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            ""
        );
    }

    #[test]
    fn health_diagnostics_preserve_active_status() {
        let (_dir, db) = database();
        db.set_status("nova", "active", "tool").unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            "tool"
        );
    }

    #[test]
    fn mod_cannot_establish_session_ownership() {
        let (_dir, db) = database();
        assert_eq!(
            validated_owner(&db, "session-1", "process-1").unwrap(),
            None
        );
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        assert_eq!(
            validated_owner(&db, "session-1", "process-1")
                .unwrap()
                .as_deref(),
            Some("nova")
        );
        assert_eq!(
            validated_owner(&db, "session-2", "process-1").unwrap(),
            None
        );
        assert_eq!(
            validated_owner(&db, "session-1", "process-2").unwrap(),
            None
        );
    }

    #[test]
    fn unavailable_mod_is_visible_without_changing_status_or_cursor() {
        let (_dir, db) = database();
        report_health(&db, "process-1", "nova").unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-unavailable");
        assert!(row.status_detail.contains("hcom kill nova and hcom r nova"));
        assert_eq!(row.status, "listening");
        assert_eq!(db.get_cursor("nova"), 0);
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            ""
        );
    }

    #[test]
    fn a_new_session_gets_one_wait_before_it_is_reported_unavailable() {
        let (_dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        let mut current = load(&db, "session-1", "process-1").unwrap().unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        assert_eq!(
            db.get_instance_full("nova")
                .unwrap()
                .unwrap()
                .status_context,
            ""
        );
        current.started_at -= HEALTH_TIMEOUT + 1.0;
        db.kv_set(
            &key("session-1", "process-1"),
            Some(&serde_json::to_string(&current).unwrap()),
        )
        .unwrap();
        report_health(&db, "process-1", "nova").unwrap();
        let row = db.get_instance_full("nova").unwrap().unwrap();
        assert_eq!(row.status_context, "tui:mod-unavailable");
        assert!(row.status_detail.contains("has not completed a poll"));
    }

    #[test]
    fn wake_claim_keeps_messages_queued_and_rearms_only_after_hook_progress() {
        let (_dir, db) = database();
        let mut current = ModState {
            name: "nova".into(),
            checked_at: now_epoch_f64(),
            error: None,
            submission_failed: false,
            attempted_cursor: None,
            attempted_at: None,
            started_at: 0.0,
        };
        assert!(!wake_due(&db, "nova", &current));
        for _ in 0..2 {
            db.conn().execute(
                "INSERT INTO events (type,timestamp,instance,data) VALUES ('message','2026-01-01T00:00:00Z','luna','{\"from\":\"luna\",\"text\":\"hello\",\"scope\":\"broadcast\"}')", [],
            ).unwrap();
        }
        assert!(wake_due(&db, "nova", &current));
        current.attempted_cursor = Some(db.get_cursor("nova"));
        assert!(db.has_pending("nova"));
        assert!(!wake_due(&db, "nova", &current));
        // The hook, rather than successful prompt submission, confirms delivery.
        db.advance_instance_cursor("nova", 1).unwrap();
        assert!(wake_due(&db, "nova", &current));
        db.set_status("nova", "active", "tool").unwrap();
        assert!(!wake_due(&db, "nova", &current));
    }

    #[test]
    fn overlapping_polls_claim_one_wake_and_reload_preserves_the_claim() {
        let (dir, db) = database();
        db.mark_claude_session_validated("session-1", "nova")
            .unwrap();
        db.conn().execute(
            "INSERT INTO events (type,timestamp,instance,data) VALUES ('message','2026-01-01T00:00:00Z','luna','{\"from\":\"luna\",\"text\":\"hello\",\"scope\":\"broadcast\"}')", [],
        ).unwrap();
        finish(&db, "session-1", "process-1", false).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let path = dir.path().join("hcom.db");
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let db = HcomDb::open_raw(&path).unwrap();
                    barrier.wait();
                    finish(&db, "session-1", "process-1", true).unwrap()
                })
            })
            .collect();
        let claims = threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .filter(|claimed| *claimed)
            .count();
        assert_eq!(claims, 1);
        finish(&db, "session-1", "process-1", false).unwrap();
        assert!(!finish(&db, "session-1", "process-1", true).unwrap());
        assert!(db.has_pending("nova"));
    }
}
