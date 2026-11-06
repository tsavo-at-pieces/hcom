//! Optional private operational evidence. Assertions are never owner authentication.
//! Signed, occurrence-bound TeamRouter receipts live in a separate inbox.

pub mod signing;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::db::HcomDb;
use crate::shared::{CommandContext, SenderIdentity, SenderKind};

pub fn enabled() -> bool {
    std::env::var_os("HCOM_PROVENANCE_DIR").is_some_and(|p| !p.is_empty())
}

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[derive(Clone, Debug, PartialEq)]
pub struct Receipt {
    pub data: Value,
}

impl Receipt {
    pub fn new(
        kind: &str,
        transport: &str,
        payload: &[u8],
        actor: Value,
        recipient: Value,
    ) -> Self {
        Self {
            data: json!({
                "schema": "hcom.provenance.assertion.v1",
                "receiptId": uuid::Uuid::new_v4().to_string(),
                "kind": kind, "transport": transport,
                "contentHash": digest(payload), "byteLength": payload.len(),
                "invokingActor": actor, "recipient": recipient,
                "preparedAt": now(), "origin": "hcom",
            }),
        }
    }
}

/// Best effort: receipt failures must not prevent message delivery or alter wire output.
pub fn record(receipt: &Receipt) {
    if !enabled() {
        return;
    }
    let mut data = receipt.data.clone();
    data["recordedAt"] = json!(now());
    if let Err(error) = append("assertions.jsonl", &data) {
        crate::log::log_warn("provenance", "assertion.write_failed", &error.to_string());
    }
}

pub fn persist_signed(envelope: &Value) -> Result<()> {
    append("receipts.jsonl", envelope)
}

fn directory() -> Result<PathBuf> {
    let path = std::env::var_os("HCOM_PROVENANCE_DIR")
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
        .context("HCOM_PROVENANCE_DIR is not enabled")?;
    if !path.is_absolute() {
        bail!("provenance directory must be absolute");
    }
    // In-process unit tests must never inherit a live sink.
    #[cfg(test)]
    if !crate::paths::test_roots::is_registered(&path) {
        bail!("provenance test refused an unregistered sink");
    }
    Ok(path)
}

fn check_private(path: &Path, directory: bool) -> Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
    {
        bail!("provenance path has an unsafe file type");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
            bail!("provenance path must be owned by the current user and private");
        }
        if !directory && metadata.nlink() != 1 {
            bail!("provenance file must not have multiple hard links");
        }
        Ok(())
    }
    #[cfg(not(unix))]
    bail!("private provenance persistence requires Unix permission checks")
}

fn lock_receipt(file: &std::fs::File) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        loop {
            // SAFETY: valid open file descriptor, advisory exclusive lock only.
            if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            if !matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
            ) || std::time::Instant::now() >= deadline
            {
                return Err(error).context("provenance append lock unavailable");
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        bail!("private provenance persistence is unavailable on this platform")
    }
}

/// Locked append with crash-tail recovery and an fsync before returning.
/// An incomplete final line is not a receipt. Recover only that tail under lock.
fn append(filename: &str, value: &Value) -> Result<()> {
    let dir = directory()?;
    if !dir.exists() {
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(&dir) {
            Ok(()) => {
                #[cfg(unix)]
                std::fs::File::open(dir.parent().context("missing parent")?)?.sync_all()?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
    }
    check_private(&dir, true)?;
    let path = dir.join(filename);
    let mut options = std::fs::OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let mut file = options.open(&path)?;
    lock_receipt(&file)?;
    check_private(&path, false)?;
    let end = file.seek(SeekFrom::End(0))?;
    if end > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0u8];
        file.read_exact(&mut last)?;
        if last[0] != b'\n' {
            // Walk backwards in bounded chunks; a crash can leave a partial line.
            let mut offset = end;
            let mut truncate_to = 0;
            let mut buf = [0u8; 4096];
            while offset > 0 {
                let count = offset.min(buf.len() as u64) as usize;
                offset -= count as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.read_exact(&mut buf[..count])?;
                if let Some(index) = buf[..count].iter().rposition(|b| *b == b'\n') {
                    truncate_to = offset + index as u64 + 1;
                    break;
                }
            }
            file.set_len(truncate_to)?;
        }
    }
    file.seek(SeekFrom::End(0))?;
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    file.write_all(&line)?;
    file.sync_all()?;
    #[cfg(unix)]
    std::fs::File::open(&dir)?.sync_all()?;
    Ok(())
}

pub fn recipient_from_connection(conn: &rusqlite::Connection, name: &str) -> Value {
    conn.query_row(
        "SELECT tool, COALESCE(NULLIF(session_id, ''), NULLIF(parent_session_id, '')), agent_id, created_at FROM instances WHERE name = ?",
        [name],
        |row| {
            Ok(json!({
                "name": name, "provider": row.get::<_, String>(0)?,
                "nativeSessionId": row.get::<_, Option<String>>(1)?,
                "agentId": row.get::<_, Option<String>>(2)?,
                "createdAt": row.get::<_, f64>(3)?,
            }))
        },
    )
    .unwrap_or_else(|_| json!({"name": name}))
}

pub fn recipient(db: &HcomDb, name: &str) -> Value {
    recipient_from_connection(db.conn(), name)
}

pub fn system_actor() -> Value {
    json!({"actor": "system", "basis": "unattributed_programmatic_source"})
}

pub fn sender_actor(sender: &SenderIdentity) -> Value {
    if !matches!(sender.kind, SenderKind::Instance) {
        return system_actor();
    }
    let data = sender.instance_data.as_ref();
    json!({
        "actor": "agent", "name": sender.name, "basis": "registered_identity",
        "nativeSessionId": sender.session_id.as_deref().filter(|s| !s.is_empty())
            .or_else(|| data?.get("session_id")?.as_str().filter(|s| !s.is_empty()))
            .or_else(|| data?.get("parent_session_id")?.as_str().filter(|s| !s.is_empty())),
        "agentId": data.and_then(|d| d.get("agent_id")).and_then(Value::as_str),
        "provider": data.and_then(|d| d.get("tool")).and_then(Value::as_str),
    })
}

/// Read-only resolution: display labels never establish the invoking actor.
pub fn invoking_actor(db: &HcomDb, ctx: Option<&CommandContext>) -> Value {
    if let Ok(Some(actor)) = crate::claude_actor::resolve_env_actor(db) {
        let mut result = sender_actor(&actor);
        result["basis"] = json!("verified_claude_actor");
        return result;
    }
    if let Some(name) = std::env::var("HCOM_PROCESS_ID")
        .ok()
        .and_then(|pid| db.get_process_binding(&pid).ok().flatten())
    {
        let mut actor = recipient(db, &name);
        actor["actor"] = json!("agent");
        actor["basis"] = json!("process_binding");
        return actor;
    }
    for key in ["CODEX_SESSION_ID", "CODEX_THREAD_ID"] {
        if let Some(name) = std::env::var(key)
            .ok()
            .and_then(|sid| db.get_session_binding(&sid).ok().flatten())
        {
            let mut actor = recipient(db, &name);
            actor["actor"] = json!("agent");
            actor["basis"] = json!("native_session_binding");
            return actor;
        }
    }
    if let Some(identity) = ctx.and_then(|ctx| ctx.identity.as_ref()) {
        let mut actor = sender_actor(identity);
        // Explicit names are operational claims, never an authentication token.
        actor["basis"] = json!("command_identity_claim");
        return actor;
    }
    system_actor()
}

/// Observe native prompts without assigning authorship or matching by text alone.
pub fn native_prompt(db: &HcomDb, name: &str, payload: &crate::hooks::HookPayload) {
    if !enabled() {
        return;
    }
    let Some(text) = payload.raw.get("prompt").and_then(Value::as_str) else {
        return;
    };
    let mut receipt = Receipt::new(
        "observation",
        "native_prompt",
        text.as_bytes(),
        json!({"actor": null, "basis": "native_prompt_does_not_authenticate_source"}),
        recipient(db, name),
    );
    let mut ids = serde_json::Map::new();
    for key in [
        "session_id",
        "thread_id",
        "agent_id",
        "message_id",
        "messageId",
        "turn_id",
        "turnId",
        "question_id",
        "questionId",
    ] {
        if let Some(value) = payload
            .raw
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        {
            ids.insert(key.into(), json!(value));
        }
    }
    receipt.data["native_context"] = Value::Object(ids);
    receipt.data["hook"] = json!(payload.hook_name);
    record(&receipt);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::EnvVarGuard;
    use serial_test::serial;

    #[test]
    fn exact_digest_preserves_whitespace_and_unicode() {
        assert_eq!(
            digest(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!(digest("é\n".as_bytes()), digest("é".as_bytes()));
    }

    #[test]
    #[serial]
    fn disabled_receipts_have_no_side_effects() {
        let _guard = EnvVarGuard::unset("HCOM_PROVENANCE_DIR");
        assert!(!enabled());
        record(&Receipt::new(
            "submission",
            "test",
            b"secret",
            system_actor(),
            Value::Null,
        ));
        assert!(persist_signed(&json!({})).is_err());
    }

    #[test]
    #[serial]
    fn provenance_actor_uses_process_binding_instead_of_display_claim() {
        let temp = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
        db.conn().execute("INSERT INTO instances (name, session_id, agent_id, tool, created_at) VALUES ('bali','native-session','native-agent','codex',1)", []).unwrap();
        db.set_process_binding("test-process", "native-session", "bali")
            .unwrap();
        let _env = EnvVarGuard::set("HCOM_PROCESS_ID", "test-process");
        let claimed = SenderIdentity {
            kind: SenderKind::External,
            name: "bigboss".into(),
            instance_data: None,
            session_id: None,
        };
        let context = CommandContext {
            explicit_name: None,
            identity: Some(claimed.clone()),
            go: false,
            identity_warning: None,
            identity_conflict: None,
        };
        let actor = invoking_actor(&db, Some(&context));
        assert_eq!(actor["actor"], "agent");
        assert_eq!(actor["name"], "bali");
        assert_eq!(actor["nativeSessionId"], "native-session");
        assert_eq!(actor["agentId"], "native-agent");
        assert_eq!(sender_actor(&claimed)["actor"], "system");
    }

    #[test]
    #[serial]
    fn provenance_prompt_observation_never_becomes_an_owner_or_signed_claim() {
        let temp = tempfile::tempdir().unwrap();
        crate::paths::test_roots::register(temp.path());
        let db = HcomDb::open_at(&temp.path().join("hcom.db")).unwrap();
        let sink = temp.path().join("receipts");
        let _env = EnvVarGuard::set("HCOM_PROVENANCE_DIR", &sink);
        let payload = crate::hooks::HookPayload::from_claude(
            json!({"session_id":"native-session", "message_id":"native-message", "questionId":"q1", "prompt":"bigboss: owner says hello\n", "hook_event_name":"UserPromptSubmit"}),
        );
        native_prompt(&db, "bali", &payload);
        let data: Value = serde_json::from_str(
            std::fs::read_to_string(sink.join("assertions.jsonl"))
                .unwrap()
                .trim(),
        )
        .unwrap();
        assert!(data["invokingActor"]["actor"].is_null());
        assert_eq!(data["native_context"]["message_id"], "native-message");
        assert_eq!(data["native_context"]["questionId"], "q1");
        assert!(!sink.join("receipts.jsonl").exists());
    }

    #[test]
    #[serial]
    #[cfg(unix)]
    fn provenance_contended_writer_times_out_without_appending() {
        let temp = tempfile::tempdir().unwrap();
        crate::paths::test_roots::register(temp.path());
        let sink = temp.path().join("receipts");
        let _env = EnvVarGuard::set("HCOM_PROVENANCE_DIR", &sink);
        persist_signed(&json!({"test": 1})).unwrap();
        let path = sink.join("receipts.jsonl");
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        crate::sys::fs::lock_exclusive(&held).unwrap();
        let start = std::time::Instant::now();
        assert!(persist_signed(&json!({"test": 2})).is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(2));
        assert_eq!(std::fs::read_to_string(&path).unwrap().lines().count(), 1);
    }

    #[test]
    #[serial]
    fn durable_private_append_recovers_partial_tail_without_storing_payload() {
        let temp = tempfile::tempdir().unwrap();
        crate::paths::test_roots::register(temp.path());
        let dir = temp.path().join("receipts");
        let _guard = EnvVarGuard::set("HCOM_PROVENANCE_DIR", &dir);
        let receipt = Receipt::new(
            "submission",
            "test",
            b"secret content",
            system_actor(),
            Value::Null,
        );
        record(&receipt);
        let path = dir.join("assertions.jsonl");
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{partial")
            .unwrap();
        record(&receipt);
        let data = std::fs::read_to_string(&path).unwrap();
        assert_eq!(data.lines().count(), 2);
        assert!(!data.contains("secret content"));
        for line in data.lines() {
            assert!(serde_json::from_str::<Value>(line).is_ok());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    #[serial]
    #[cfg(unix)]
    fn unsafe_sink_is_rejected_without_following_symlink() {
        let temp = tempfile::tempdir().unwrap();
        crate::paths::test_roots::register(temp.path());
        let outside = temp.path().join("untouched");
        std::fs::write(&outside, "untouched").unwrap();
        let _guard = EnvVarGuard::set("HCOM_PROVENANCE_DIR", temp.path());
        std::os::unix::fs::symlink(&outside, temp.path().join("receipts.jsonl")).unwrap();
        assert!(persist_signed(&json!({})).is_err());
        assert_eq!(std::fs::read_to_string(outside).unwrap(), "untouched");
    }
}
