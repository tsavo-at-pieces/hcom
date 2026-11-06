//! Optional TeamRouter occurrence-receipt signer for native delivery adapters.
//!
//! Signing authenticates the adapter's assertion. It does not establish native
//! occurrence correspondence; the archive must independently check that.

use std::fs::OpenOptions;
use std::io::{IsTerminal, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, ensure};
use base64::Engine;
use rustls::SignatureScheme;
use rustls::pki_types::{PrivateKeyDer, pem::PemObject};
use serde_json::{Value, json};

const DOMAIN: &str = "teamrouter.transcript.receipt.v1\n";
/// Adapter signing configuration must not leak into newly launched agents.
pub const ENV_VARS: &[&str] = &["HCOM_PROVENANCE_KEY_FILE", "HCOM_PROVENANCE_ISSUER"];
const MAX_INPUT: u64 = 16 * 1024;
const PAYLOAD_KEYS: &[&str] = &[
    "type",
    "submissionId",
    "provider",
    "nativeSessionId",
    "agentId",
    "messageId",
    "turnId",
    "contentHash",
    "actor",
    "origin",
    "state",
    "issuedAt",
    "eventKind",
    "questionId",
];

fn identifier(value: &Value) -> bool {
    value.as_str().is_some_and(|s| {
        !s.is_empty()
            && s.encode_utf16().count() <= 256
            && !s.chars().any(|c| c <= '\u{1f}' || c == '\u{7f}')
    })
}

fn validate_payload(payload: &Value) -> Result<()> {
    let object = payload
        .as_object()
        .context("receipt payload must be an object")?;
    ensure!(
        object.keys().all(|k| PAYLOAD_KEYS.contains(&k.as_str())),
        "unsupported receipt payload field"
    );
    ensure!(
        payload.get("type").is_none_or(|v| v == "complete"),
        "only complete occurrence receipts can be signed"
    );
    for field in ["submissionId", "nativeSessionId", "agentId", "messageId"] {
        ensure!(
            identifier(&payload[field]),
            "missing or invalid native receipt identifier"
        );
    }
    for field in ["turnId", "questionId"] {
        ensure!(
            payload.get(field).is_none_or(identifier),
            "invalid optional native receipt identifier"
        );
    }
    ensure!(
        matches!(payload["provider"].as_str(), Some("claude" | "codex")),
        "unsupported receipt provider"
    );
    ensure!(
        matches!(payload["actor"].as_str(), Some("agent" | "system")),
        "hcom receipts can only claim agent or system"
    );
    ensure!(
        matches!(payload["origin"].as_str(), Some("hcom" | "automation")),
        "unsupported hcom receipt origin"
    );
    ensure!(
        matches!(payload["state"].as_str(), Some("written" | "matched")),
        "native occurrence state is required"
    );
    ensure!(
        payload
            .get("eventKind")
            .is_none_or(|v| matches!(v.as_str(), Some("input" | "tool_result"))),
        "unsupported native event kind"
    );
    ensure!(
        payload["eventKind"] != "tool_result" || identifier(&payload["questionId"]),
        "tool result receipts require questionId"
    );
    ensure!(
        payload["contentHash"]
            .as_str()
            .is_some_and(|s| s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))),
        "contentHash must be lowercase SHA-256 hex"
    );
    let issued_at = payload["issuedAt"]
        .as_str()
        .context("issuedAt is required")?;
    // Exactly TeamRouter's UTC timestamp grammar, with at most milliseconds.
    let timestamp_format =
        regex::Regex::new(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?Z$").unwrap();
    ensure!(
        timestamp_format.is_match(issued_at)
            && chrono::DateTime::parse_from_rfc3339(issued_at).is_ok(),
        "issuedAt must be an ISO UTC timestamp"
    );
    Ok(())
}

/// The supported envelope has ASCII keys and strings plus schemaVersion=1.
/// Sort recursively rather than depending on serde_json's map implementation.
fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            let fields: Vec<_> = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        canonical_json(&object[key])
                    )
                })
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Vec<_>>()
                .join(",")
        ),
        _ => serde_json::to_string(value).unwrap(),
    }
}

fn sign_with_key(
    payload: Value,
    issuer: &str,
    receipt_id: &str,
    key: &PrivateKeyDer<'_>,
) -> Result<Value> {
    validate_payload(&payload)?;
    ensure!(
        identifier(&json!(issuer)) && identifier(&json!(receipt_id)),
        "invalid receipt issuer or ID"
    );
    let mut envelope = json!({
        "schemaVersion": 1,
        "issuerId": issuer,
        "receiptId": receipt_id,
        "payload": payload,
    });
    let key = rustls::crypto::aws_lc_rs::sign::any_supported_type(key)
        .map_err(|_| anyhow::anyhow!("invalid receipt private key"))?;
    let signer = key
        .choose_scheme(&[SignatureScheme::ED25519])
        .context("receipt key must be Ed25519")?;
    let bytes = format!("{DOMAIN}{}", canonical_json(&envelope));
    let signature = signer
        .sign(bytes.as_bytes())
        .map_err(|_| anyhow::anyhow!("receipt signing failed"))?;
    envelope["signature"] = json!(base64::engine::general_purpose::STANDARD.encode(signature));
    Ok(envelope)
}

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    ensure!(path.is_absolute(), "receipt key path must be absolute");
    let metadata = std::fs::symlink_metadata(path).context("cannot inspect receipt key file")?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "receipt key must be a regular file, not a symlink"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path).context("cannot open receipt key file")?;
    let metadata = file
        .metadata()
        .context("cannot inspect opened receipt key")?;
    ensure!(
        metadata.is_file() && metadata.len() <= MAX_INPUT,
        "invalid receipt key file type or size"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid has no preconditions and does not alter process state.
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() } && metadata.mode() & 0o077 == 0,
            "receipt key must be owned by this user with owner-only permissions"
        );
    }
    #[cfg(not(unix))]
    anyhow::bail!(
        "receipt signing requires an owner-private key check unavailable on this platform"
    );

    #[cfg(unix)]
    {
        let mut bytes = Vec::new();
        file.take(MAX_INPUT + 1)
            .read_to_end(&mut bytes)
            .context("cannot read receipt key")?;
        ensure!(bytes.len() as u64 <= MAX_INPUT, "receipt key is too large");
        PrivateKeyDer::from_pem_slice(&bytes)
            .map_err(|_| anyhow::anyhow!("receipt key must be PEM PKCS#8 Ed25519"))
    }
}

/// Explicit native-adapter assertion. Never called automatically for hook
/// stdout, TCP acceptance, PTY writes, or a matching digest alone.
pub fn sign_complete(payload: Value) -> Result<Value> {
    ensure!(
        super::enabled(),
        "receipt signing is disabled (HCOM_PROVENANCE_DIR is unset)"
    );
    validate_payload(&payload)?;
    let issuer =
        std::env::var("HCOM_PROVENANCE_ISSUER").context("HCOM_PROVENANCE_ISSUER is required")?;
    let key_path = std::env::var_os("HCOM_PROVENANCE_KEY_FILE")
        .context("HCOM_PROVENANCE_KEY_FILE is required")?;
    let key = private_key(Path::new(&key_path))?;
    sign_with_key(payload, &issuer, &uuid::Uuid::new_v4().to_string(), &key)
}

/// This adapter command bypasses hcom DB/status/inbox processing. Its sole
/// output is the persisted receipt, so it is safe to consume as one JSON value.
pub fn cmd_receipt_sign(args: &[String]) -> i32 {
    fn run(args: &[String]) -> Result<()> {
        ensure!(
            args.is_empty(),
            "usage: hcom receipt-sign < complete-payload.json"
        );
        ensure!(
            super::enabled(),
            "receipt signing is disabled (HCOM_PROVENANCE_DIR is unset)"
        );
        ensure!(
            !std::io::stdin().is_terminal(),
            "receipt-sign requires a JSON payload on stdin"
        );
        let mut bytes = Vec::new();
        std::io::stdin()
            .take(MAX_INPUT + 1)
            .read_to_end(&mut bytes)
            .context("cannot read receipt input")?;
        ensure!(
            bytes.len() as u64 <= MAX_INPUT,
            "receipt input is too large"
        );
        let payload: Value =
            serde_json::from_slice(&bytes).map_err(|_| anyhow::anyhow!("invalid receipt JSON"))?;
        let receipt = sign_complete(payload)?;
        super::persist_signed(&receipt)?;
        let mut output = serde_json::to_vec(&receipt)?;
        output.push(b'\n');
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(&output)
            .context("cannot write receipt output")?;
        stdout.flush().context("cannot flush receipt output")?;
        Ok(())
    }
    match run(args) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("Error: {error}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Public RFC 8032 test seed; not an operational key.
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g\n-----END PRIVATE KEY-----\n";

    fn payload() -> Value {
        json!({
            "submissionId": "submission-1", "provider": "codex",
            "nativeSessionId": "session-1", "agentId": "agent-1", "messageId": "message-1",
            "contentHash": "00".repeat(32), "actor": "agent", "origin": "hcom",
            "state": "matched", "issuedAt": "2026-11-06T20:00:00.000Z",
        })
    }

    #[test]
    fn ed25519_matches_teamrouter_node_vector() {
        let key = PrivateKeyDer::from_pem_slice(TEST_KEY.as_bytes()).unwrap();
        let receipt = sign_with_key(payload(), "hcom-test", "receipt-1", &key).unwrap();
        // Generated with TeamRouter canonicalReceiptJSON and node:crypto sign.
        assert_eq!(
            receipt["signature"],
            "VUGGoY7zuIqDveCv3n+ZOR68Boh/Hv/1ahUiPoCjr+wy9NWqo0xuO2u3ARtaLtGhTfLPEE3wJG7U04Ec8VbSCw=="
        );
        assert_eq!(receipt["payload"], payload());
    }

    #[test]
    fn canonical_json_sorts_nested_keys_and_keeps_unicode_bytes() {
        assert_eq!(
            canonical_json(&json!({"z": {"b": "é\n", "a": "😀"}, "a": 1})),
            "{\"a\":1,\"z\":{\"a\":\"😀\",\"b\":\"é\\n\"}}"
        );
    }

    #[test]
    fn rejects_owner_labels_unbound_claims_and_unknown_fields() {
        for (key, value) in [
            ("actor", json!("owner")),
            ("origin", json!("owner_ui")),
            ("nativeSessionId", Value::Null),
            ("agentId", json!("")),
            ("messageId", json!("bad\nidentifier")),
            ("state", json!("prepared")),
            ("from", json!("bigboss")),
            ("type", json!("submission")),
            ("contentHash", json!("AB".repeat(32))),
            ("issuedAt", json!("2026-11-06T20:00:00.0000Z")),
        ] {
            let mut invalid = payload();
            invalid[key] = value;
            assert!(
                validate_payload(&invalid).is_err(),
                "accepted invalid {key}"
            );
        }
        let mut missing = payload();
        missing.as_object_mut().unwrap().remove("messageId");
        assert!(validate_payload(&missing).is_err());
    }

    #[test]
    fn question_answers_require_native_question_id() {
        let mut value = payload();
        value["eventKind"] = json!("tool_result");
        assert!(validate_payload(&value).is_err());
        value["questionId"] = json!("question-1");
        assert!(validate_payload(&value).is_ok());
    }

    #[test]
    #[cfg(unix)]
    fn refuses_public_and_symlink_keys_without_changing_permissions() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key.pem");
        std::fs::write(&path, TEST_KEY).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(private_key(&path).is_err());
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o644
        );
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(private_key(&path).is_ok());
        let link = dir.path().join("link.pem");
        symlink(&path, &link).unwrap();
        assert!(private_key(&link).is_err());
    }
}
