//! Optional programmatic injection receipts. Raw terminal input never enters
//! this module. Disk persistence runs on a bounded worker, outside the PTY loop.

use std::sync::mpsc::{self, SyncSender};
use std::thread::JoinHandle;

use rusqlite::{Connection, OpenFlags};

use crate::provenance::Receipt;

pub(super) struct InjectionReceipts {
    connection: Option<Connection>,
    owner: Option<RecipientOwner>,
    database_file: Option<(std::path::PathBuf, u64)>,
    sender: Option<SyncSender<Receipt>>,
    worker: Option<JoinHandle<()>>,
}

struct RecipientOwner {
    process_id: String,
    name: String,
    created_at: f64,
    child_pid: u32,
}

fn launch_process_id(env_vars: &[(String, String)], inherited: Option<String>) -> Option<String> {
    env_vars
        .iter()
        .rev()
        .find(|(key, _)| key == "HCOM_PROCESS_ID")
        .map(|(_, value)| value.clone())
        .or(inherited)
        .filter(|value| !value.is_empty())
}

impl InjectionReceipts {
    fn disabled() -> Self {
        Self {
            connection: None,
            owner: None,
            database_file: None,
            sender: None,
            worker: None,
        }
    }

    pub fn new(config: &super::ProxyConfig, child_pid: Option<u32>) -> Self {
        if !crate::provenance::enabled() {
            return Self::disabled();
        }
        // Per-child environment overrides the wrapper's inherited environment.
        // An explicitly empty value must not fall back to a parent's token.
        let process_id = launch_process_id(&config.env_vars, std::env::var("HCOM_PROCESS_ID").ok());
        let (Some(process_id), Some(name), Some(child_pid)) =
            (process_id, config.instance_name.as_deref(), child_pid)
        else {
            return Self::disabled();
        };
        // Open once before the poll loop. No schema initialization, migrations,
        // creation, or waiting for a writer lock on this connection.
        let connection =
            Connection::open_with_flags(crate::paths::db_path(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .and_then(|connection| {
                    connection.busy_timeout(std::time::Duration::ZERO)?;
                    Ok(connection)
                });
        match connection {
            Ok(connection) => Self::with_connection(
                connection,
                process_id,
                name,
                child_pid,
                crate::provenance::record,
            ),
            Err(error) => {
                crate::log::log_warn("provenance", "pty.open", &error.to_string());
                Self::disabled()
            }
        }
    }

    fn with_connection(
        connection: Connection,
        process_id: String,
        name: &str,
        child_pid: u32,
        persist: impl Fn(&Receipt) + Send + 'static,
    ) -> Self {
        // The row must already belong to the child held by this proxy. Native
        // session binding can arrive later without changing this incarnation.
        let Ok(created_at) = connection.query_row(
            "SELECT created_at FROM instances WHERE name = ?1 AND pid = ?2",
            rusqlite::params![name, child_pid],
            |row| row.get::<_, f64>(0),
        ) else {
            return Self::disabled();
        };
        let owner = RecipientOwner {
            process_id,
            name: name.to_owned(),
            created_at,
            child_pid,
        };
        let database_file = connection.path().map(|path| {
            let path = std::path::PathBuf::from(path);
            let identity = crate::sys::fs::file_id(&path);
            (path, identity)
        });
        let (sender, receiver) = mpsc::sync_channel::<Receipt>(64);
        let worker = std::thread::Builder::new()
            .name("hcom-pty-receipts".into())
            .spawn(move || {
                while let Ok(receipt) = receiver.recv() {
                    persist(&receipt);
                }
            });
        match worker {
            Ok(worker) => Self {
                connection: Some(connection),
                owner: Some(owner),
                database_file,
                sender: Some(sender),
                worker: Some(worker),
            },
            Err(error) => {
                crate::log::log_warn("provenance", "pty.worker", &error.to_string());
                Self::disabled()
            }
        }
    }

    /// Snapshot the recipient and digest the exact decoded bytes that will be
    /// written. The raw TCP protocol supplies no authenticated sender identity.
    pub fn prepare(&self, bytes: &[u8]) -> Option<Receipt> {
        if bytes.is_empty() {
            return None;
        }
        Some(Receipt::new(
            "delivery",
            "pty_write",
            bytes,
            crate::provenance::system_actor(),
            self.current_recipient()?,
        ))
    }

    /// Obtain the current native recipient only while this proxy still owns
    /// the original launch and row incarnation.
    fn current_recipient(&self) -> Option<serde_json::Value> {
        let connection = self.connection.as_ref()?;
        let owner = self.owner.as_ref()?;
        let (path, identity) = self.database_file.as_ref()?;
        if *identity == 0 || crate::sys::fs::file_id(path) != *identity {
            // A reset may replace the DB while this proxy stays alive. Never
            // attach an old incarnation's native IDs to a new delivery.
            return None;
        }
        // A display name can be reused while an old proxy is still alive.
        // Bind evidence to this launch and this row, never to that label.
        // Keep ownership checks and the native-ID snapshot in one read snapshot.
        let transaction = connection.unchecked_transaction().ok()?;
        let name: String = transaction
            .query_row(
                "SELECT i.name FROM instances i JOIN process_bindings p ON p.instance_name = i.name
             WHERE p.process_id = ?1 AND i.name = ?2 AND i.created_at = ?3 AND i.pid = ?4",
                rusqlite::params![
                    owner.process_id,
                    owner.name,
                    owner.created_at,
                    owner.child_pid
                ],
                |row| row.get(0),
            )
            .ok()?;
        Some(crate::provenance::recipient_from_connection(
            &transaction,
            &name,
        ))
    }

    /// Called only for a completed PTY write. Queue exhaustion produces a gap,
    /// never a false receipt or backpressure on human terminal input.
    pub fn completed(&self, receipt: Option<Receipt>) {
        let (Some(sender), Some(receipt)) = (&self.sender, receipt) else {
            return;
        };
        // A session switch may occur while bytes are queued. Suppress the
        // assertion if either ownership or any native recipient field changed
        // before the PTY completed the write.
        let Some(current) = self.current_recipient() else {
            return;
        };
        if receipt.data.get("recipient") != Some(&current) {
            return;
        }
        if sender.try_send(receipt).is_err() {
            crate::log::log_warn(
                "provenance",
                "pty.queue",
                "Completed PTY receipt dropped: writer unavailable or queue full",
            );
        }
    }
}

impl Drop for InjectionReceipts {
    fn drop(&mut self) {
        // Orderly proxy exit drains completed writes. A crash can still leave a
        // provenance gap; receipt absence never proves the absence of delivery.
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn fixture() -> (tempfile::TempDir, crate::db::HcomDb, InjectionReceipts) {
        fixture_with_persist(|_| {})
    }

    fn fixture_with_persist(
        persist: impl Fn(&Receipt) + Send + 'static,
    ) -> (tempfile::TempDir, crate::db::HcomDb, InjectionReceipts) {
        let directory = tempfile::tempdir().unwrap();
        let db = crate::db::HcomDb::open_raw(&directory.path().join("test.db")).unwrap();
        db.init_db().unwrap();
        db.conn().execute(
            "INSERT INTO instances (name, tool, created_at, pid) VALUES ('luna', 'codex', 1, 1234)", [],
        ).unwrap();
        let observer = InjectionReceipts::with_connection(
            Connection::open_with_flags(db.path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap(),
            "launch-one".into(),
            "luna",
            1234,
            persist,
        );
        (directory, db, observer)
    }

    #[test]
    fn queued_receipt_is_dropped_when_native_session_changes_before_completion() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let output = observed.clone();
        let (_directory, db, observer) = fixture_with_persist(move |receipt| {
            output.lock().unwrap().push(receipt.clone());
        });
        db.conn().execute("INSERT INTO process_bindings (process_id, instance_name, updated_at) VALUES ('launch-one', 'luna', 0)", []).unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET session_id = 'session-one' WHERE name = 'luna'",
                [],
            )
            .unwrap();
        let receipt = observer.prepare(b"queued text").unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET session_id = 'session-two' WHERE name = 'luna'",
                [],
            )
            .unwrap();
        observer.completed(Some(receipt));
        drop(observer);
        assert!(observed.lock().unwrap().is_empty());
    }

    #[test]
    fn old_proxy_cannot_receipt_a_replacement_with_the_same_name() {
        let (_directory, db, observer) = fixture();
        db.conn().execute("INSERT INTO process_bindings (process_id, instance_name, updated_at) VALUES ('launch-one', 'luna', 0)", []).unwrap();
        db.conn()
            .execute("DELETE FROM instances WHERE name = 'luna'", [])
            .unwrap();
        // Even a leftover process binding and copied PID cannot adopt a new
        // row incarnation's native identifiers.
        db.conn().execute(
            "INSERT INTO instances (name, tool, created_at, pid, session_id) VALUES ('luna', 'codex', 2, 1234, 'replacement-session')", [],
        ).unwrap();
        assert!(observer.prepare(b"text").is_none());
    }

    #[test]
    fn placeholder_can_bind_natively_but_must_still_own_the_child() {
        let (_directory, db, observer) = fixture();
        assert!(observer.prepare(b"text").is_none(), "no launch binding yet");
        db.conn().execute("INSERT INTO process_bindings (process_id, instance_name, updated_at) VALUES ('launch-one', 'luna', 0)", []).unwrap();
        db.conn().execute("UPDATE instances SET session_id = 'native-session', agent_id = 'native-agent' WHERE name = 'luna'", []).unwrap();
        let receipt = observer.prepare(b"text").unwrap();
        assert_eq!(
            receipt.data["recipient"]["nativeSessionId"],
            "native-session"
        );
        db.conn()
            .execute("UPDATE instances SET pid = 5678 WHERE name = 'luna'", [])
            .unwrap();
        assert!(observer.prepare(b"text").is_none(), "replacement child PID");
        db.conn()
            .execute("UPDATE instances SET pid = 1234 WHERE name = 'luna'", [])
            .unwrap();
        db.conn()
            .execute("DELETE FROM process_bindings", [])
            .unwrap();
        assert!(
            observer.prepare(b"text").is_none(),
            "launch no longer owns row"
        );
    }

    #[test]
    fn configured_process_token_overrides_inherited_token_even_when_empty() {
        let config = vec![("HCOM_PROCESS_ID".into(), "child-token".into())];
        assert_eq!(
            launch_process_id(&config, Some("parent-token".into())),
            Some("child-token".into())
        );
        let empty = vec![("HCOM_PROCESS_ID".into(), "".into())];
        assert!(launch_process_id(&empty, Some("parent-token".into())).is_none());
        assert_eq!(
            launch_process_id(&[], Some("inherited".into())),
            Some("inherited".into())
        );
    }

    #[test]
    fn completed_receipts_use_recipient_snapshot_and_drain_on_close() {
        let directory = tempfile::tempdir().unwrap();
        let db = crate::db::HcomDb::open_raw(&directory.path().join("test.db")).unwrap();
        db.init_db().unwrap();
        db.conn().execute(
            "INSERT INTO instances (name, tool, tag, session_id, agent_id, created_at, pid) VALUES ('luna', 'codex', 'team', 'session-one', 'agent-one', 1, 1234)",
            [],
        ).unwrap();
        db.conn().execute("INSERT INTO process_bindings (process_id, instance_name, updated_at) VALUES ('launch-one', 'luna', 0)", []).unwrap();
        let observed = Arc::new(Mutex::new(Vec::new()));
        let output = observed.clone();
        let observer = InjectionReceipts::with_connection(
            Connection::open_with_flags(db.path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap(),
            "launch-one".into(),
            "luna",
            1234,
            move |receipt| output.lock().unwrap().push(receipt.clone()),
        );
        let receipt = observer.prepare("café\r".as_bytes()).unwrap();
        assert!(
            observed.lock().unwrap().is_empty(),
            "preparation is not delivery"
        );
        assert!(observer.prepare(b"").is_none());
        observer.completed(Some(receipt.clone()));
        db.conn()
            .execute(
                "UPDATE instances SET session_id = 'session-two' WHERE name = 'luna'",
                [],
            )
            .unwrap();
        #[cfg(unix)]
        {
            std::fs::rename(db.path(), directory.path().join("retired.db")).unwrap();
            assert!(
                observer.prepare(b"later").is_none(),
                "a replaced DB must not reuse stale native IDs"
            );
        }
        drop(observer);
        assert_eq!(*observed.lock().unwrap(), vec![receipt.clone()]);
        assert_eq!(receipt.data["recipient"]["nativeSessionId"], "session-one");
        assert_eq!(receipt.data["recipient"]["agentId"], "agent-one");
        assert_eq!(
            receipt.data["contentHash"],
            crate::provenance::digest("café\r".as_bytes())
        );
        assert_eq!(receipt.data["byteLength"], 6);
    }
}
