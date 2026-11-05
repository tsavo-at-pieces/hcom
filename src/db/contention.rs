//! Narrow admission policy for launch persistence and heartbeats. Retrying only
//! BEGIN means no closure, child spawn, or notification can be replayed.
use std::time::{Duration, Instant};

use super::HcomDb;
use anyhow::{Context, Result};

const LAUNCH_BUDGET: Duration = Duration::from_secs(15);
const HEARTBEAT_BUDGET: Duration = Duration::from_secs(8);

fn transient_busy(error: &rusqlite::Error) -> bool {
    matches!(error, rusqlite::Error::SqliteFailure(code, _)
        if matches!(code.extended_code,
            rusqlite::ffi::SQLITE_BUSY | rusqlite::ffi::SQLITE_BUSY_RECOVERY | rusqlite::ffi::SQLITE_BUSY_TIMEOUT))
}

impl HcomDb {
    #[track_caller]
    pub(crate) fn with_launch_write<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        self.with_busy_write("launch", LAUNCH_BUDGET, f)
    }

    #[track_caller]
    pub(crate) fn with_heartbeat_write<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        self.with_busy_write("heartbeat", HEARTBEAT_BUDGET, f)
    }

    #[track_caller]
    fn with_busy_write<T>(
        &self,
        operation: &'static str,
        budget: Duration,
        f: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        // An outer transaction already owns admission and rollback. Never try
        // to refresh a snapshot or replay a nested transaction here.
        if !self.conn.is_autocommit() {
            return self.with_write_scope(f);
        }
        let mut timing = WriteTiming::new(self, operation);
        let original: u32 = self
            .conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
        let deadline = Instant::now() + budget;
        let mut backoff = Duration::from_millis(10);
        let admitted = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            // SQLite waiting counts against the same monotonic budget. Short
            // slices let us recheck it; the policy extends total admission time
            // without changing the room's settings or any other connection.
            self.conn
                .busy_timeout(remaining.min(Duration::from_millis(250)))?;
            match rusqlite::Transaction::new_unchecked(
                &self.conn,
                rusqlite::TransactionBehavior::Immediate,
            ) {
                Ok(transaction) => break Ok(transaction),
                Err(error) if transient_busy(&error) && Instant::now() < deadline => {
                    std::thread::sleep(
                        backoff.min(deadline.saturating_duration_since(Instant::now())),
                    );
                    backoff = (backoff * 2).min(Duration::from_millis(100));
                }
                Err(error) => break Err(error),
            }
        };
        // Restore the caller's busy handler on success and failure. It is a
        // connection setting, never a database/WAL setting.
        let restored = self
            .conn
            .busy_timeout(Duration::from_millis(u64::from(original)));
        let _transaction = match admitted {
            Ok(transaction) => transaction, // rollback guard, including panics
            Err(error) => {
                restored?;
                return Err(error)
                    .with_context(|| format!("{operation} write admission exhausted or failed"));
            }
        };
        timing.acquired();
        let result = restored
            .map_err(anyhow::Error::from)
            .and_then(|()| f())
            .and_then(|value| {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            });
        match result {
            Ok(value) => Ok(value),
            Err(error) => match self.conn.execute_batch("ROLLBACK") {
                Ok(()) => Err(error),
                Err(rollback) => Err(error.context(format!(
                    "rollback failed; database state uncertain: {rollback}"
                ))),
            },
        }
    }
}

/// Reports completed slow write scopes to the filesystem log, never SQLite.
/// Construct before the transaction so its Drop runs after transaction Drop.
/// Savepoints must not be timed as independent writer ownership.
pub(crate) struct WriteTiming<'a> {
    db: &'a HcomDb,
    operation: &'static str,
    caller: &'static std::panic::Location<'static>,
    started: Instant,
    acquired: Option<Instant>,
}

impl<'a> WriteTiming<'a> {
    #[track_caller]
    pub(crate) fn new(db: &'a HcomDb, operation: &'static str) -> Self {
        Self {
            db,
            operation,
            caller: std::panic::Location::caller(),
            started: Instant::now(),
            acquired: None,
        }
    }

    pub(crate) fn acquired(&mut self) {
        self.acquired = Some(Instant::now());
    }
}

impl Drop for WriteTiming<'_> {
    fn drop(&mut self) {
        let finished = Instant::now();
        if finished.duration_since(self.started) < Duration::from_millis(250) {
            return;
        }
        let wait = self
            .acquired
            .unwrap_or(finished)
            .duration_since(self.started);
        let held = self.acquired.map(|start| finished.duration_since(start));
        let released = self.db.conn.is_autocommit();
        crate::log::log_with_fields(
            "WARN",
            "db",
            "db.write_timing",
            "slow writer admission or transaction",
            &[
                ("operation", self.operation),
                ("caller", &self.caller.to_string()),
                ("pid", &std::process::id().to_string()),
                ("db", &self.db.path().display().to_string()),
                ("wait_ms", &wait.as_millis().to_string()),
                (
                    if released { "held_ms" } else { "unreleased_ms" },
                    &held.map(|d| d.as_millis().to_string()).unwrap_or_default(),
                ),
                ("released", if released { "true" } else { "false" }),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_busy_is_retryable() {
        for (code, expected) in [
            (5, true),
            (261, true),
            (773, true),
            (517, false),
            (6, false),
            (19, false),
            (10, false),
        ] {
            let error = rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(code), None);
            assert_eq!(transient_busy(&error), expected, "{code}");
        }
        assert!(!transient_busy(&rusqlite::Error::InvalidQuery));
    }

    #[test]
    #[serial_test::serial]
    fn admission_budget_restores_timeout_without_running_callback() {
        let (_dir, room, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open_at(&room.join("hcom.db")).unwrap();
        let blocker = HcomDb::open_at(db.path()).unwrap();
        blocker.conn().execute_batch("BEGIN IMMEDIATE").unwrap();
        db.conn().busy_timeout(Duration::from_secs(30)).unwrap();
        let started = Instant::now();
        let error = db
            .with_busy_write("test", Duration::from_millis(80), || -> Result<()> {
                panic!("callback ran without admission")
            })
            .unwrap_err();
        assert_eq!(
            error
                .downcast_ref::<rusqlite::Error>()
                .unwrap()
                .sqlite_error_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy)
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(db.conn.is_autocommit());
        assert_eq!(
            db.conn
                .query_row("PRAGMA busy_timeout", [], |r| r.get::<_, u32>(0))
                .unwrap(),
            30000
        );
        blocker.conn().execute_batch("ROLLBACK").unwrap();
        assert!(db.get_instance_full("nova").unwrap().is_none());
    }

    #[test]
    #[serial_test::serial]
    fn callback_failure_rolls_back_and_is_never_replayed() {
        let (_dir, room, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open_at(&room.join("hcom.db")).unwrap();
        let mut calls = 0;
        let result = db.with_launch_write(|| -> Result<()> {
            calls += 1;
            db.kv_set("test", Some("partial"))?;
            Err(rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(5), None).into())
        });
        assert!(result.is_err());
        assert_eq!(calls, 1);
        assert!(db.conn.is_autocommit());
        assert!(db.kv_get("test").unwrap().is_none());
        db.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        db.with_launch_write(|| db.kv_set("outer", Some("retained")))
            .unwrap();
        assert!(!db.conn.is_autocommit());
        db.conn.execute_batch("ROLLBACK").unwrap();
        assert!(db.kv_get("outer").unwrap().is_none());
    }

    #[test]
    #[serial_test::serial]
    fn writer_measurement_separates_wait_from_ownership() {
        let (_dir, room, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        let db = HcomDb::open_at(&room.join("hcom.db")).unwrap();
        let blocker = HcomDb::open_at(db.path()).unwrap();
        let (held, ready) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            blocker
                .with_write_scope(|| {
                    held.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(350));
                    Ok(())
                })
                .unwrap();
        });
        ready.recv().unwrap();
        db.with_launch_write(|| Ok(())).unwrap();
        writer.join().unwrap();
        let records = std::fs::read_to_string(crate::paths::log_path()).unwrap();
        let rows: Vec<serde_json::Value> = records
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .filter(|row: &serde_json::Value| row["event"] == "db.write_timing")
            .collect();
        for row in &rows {
            eprintln!("{row}");
        }
        let holder = rows
            .iter()
            .find(|row| row["operation"] == "write_scope")
            .unwrap();
        let waiter = rows
            .iter()
            .find(|row| row["operation"] == "launch")
            .unwrap();
        let ms = |row: &serde_json::Value, key: &str| {
            row[key].as_str().unwrap().parse::<u128>().unwrap()
        };
        assert!(ms(holder, "held_ms") >= 300);
        assert!(ms(waiter, "wait_ms") >= 250);
        assert_eq!(holder["released"], "true");
        assert_eq!(waiter["released"], "true");
    }
}
