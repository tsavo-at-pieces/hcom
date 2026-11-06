# Native occurrence receipt signing

`hcom receipt-sign` is an optional adapter boundary for a producer that **already knows the exact native occurrence**. It signs a complete TeamRouter transcript receipt and durably appends it to `receipts.jsonl` in the configured private receipt directory before writing the same envelope to stdout. No hcom database is opened by the command and no inbox messages are appended to its output.

The adapter supplies the payload as a single JSON value on stdin. The command never guesses native IDs, joins by text hash or timestamp, or changes `written` to `matched`. Hook output, PTY writes, and native prompt observations with incomplete origin evidence remain unsigned internal assertions. In particular, the native IDs of the prompt that triggers a hook do not identify the hook's output.

Set the following environment variables only for the trusted adapter process:

- `HCOM_PROVENANCE_DIR`: absolute private directory used for receipt storage.
- `HCOM_PROVENANCE_ISSUER`: issuer ID registered with TeamRouter.
- `HCOM_PROVENANCE_KEY_FILE`: absolute path to an existing Ed25519 PKCS#8 PEM private key, owned by the current user and accessible only to that user.

Signing is off by default. hcom does not generate or install production keys. Key files must be regular files, not symlinks, and at most 16 KiB. On Unix the file is opened with `O_NOFOLLOW` and its opened descriptor's ownership and permissions are checked. Signing fails closed on platforms without an implemented owner-private key check. Newly launched agents do not inherit the issuer or key-file environment variables; unsigned assertion collection can remain enabled.

For an adapter that has already identified the native occurrence:

```sh
hcom receipt-sign < complete-payload.json
```

The payload requires `submissionId`, `provider` (`claude` or `codex`), `nativeSessionId`, `agentId`, `messageId`, `contentHash`, `actor` (`agent` or `system`), `origin` (`hcom` or `automation`), `state` (`written` or `matched`), and `issuedAt` (UTC ISO timestamp, at most milliseconds). Optional fields are `type: "complete"`, `turnId`, `eventKind` (`input` or `tool_result`), and `questionId`. A `tool_result` requires its actual native `questionId`. `contentHash` is lowercase SHA-256 hex of the **exact UTF-8 native text**, without trimming or normalization. The adapter must obtain these identifiers and bytes from its native delivery acknowledgment; fabricated or substituted IDs are invalid operational evidence.

Unknown fields, missing occurrence IDs, `owner`, `owner_ui`, and display labels such as `from` are rejected. Inputs are capped at 16 KiB. Errors never print payload or private key contents. The generated envelope uses a random UUID receipt ID and signs `teamrouter.transcript.receipt.v1\n` plus recursive-key-sorted JSON of `schemaVersion`, `issuerId`, `receiptId`, and `payload`. Ed25519 comes from the repository's existing Rust crypto dependency; there is no new crypto dependency or subprocess signer.

TeamRouter must register this issuer with `allowedTypes: ["complete"]`, `allowedActors: ["agent", "system"]`, and the needed subset of `allowedOrigins: ["hcom", "automation"]`. The signature authenticates the adapter's claim; TeamRouter still verifies the exact session/agent/message/optional turn/question and content hash against the native transcript and enforces replay rules. It never becomes proof of owner input. A signing or receipt-storage failure must not block the adapter's primary delivery operation; retain unknown provenance instead.

For bounded interoperability validation against a TeamRouter checkout, after building the hcom test binary:

```sh
node scripts/check-receipt-interop.mjs target/debug/hcom /path/to/TeamRouter/src/transcript-receipts.js
```

This uses only disposable directories and temporary test keys, checks actual CLI signing against TeamRouter's verifier and occurrence matcher, exercises owner/missing-ID rejection and signature tampering, and removes its fixtures. It changes no live configuration.
