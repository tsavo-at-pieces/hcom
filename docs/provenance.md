# Optional transcript provenance

hcom can keep private, durable operational evidence for TeamRouter session archives. Collection is off by default. Set `HCOM_PROVENANCE_DIR` to an absolute directory only in processes where collection is wanted. Its parent must exist; hcom creates the leaf with mode `0700` and JSONL files with mode `0600`. Existing permissive paths, symlinks and nonregular files are refused. Persistence and signing require Unix owner/permission checks; Windows input forwarding works as before but private persistence is currently unavailable.

This change does not enable live collection, install keys, configure TeamRouter, or modify existing running sessions. An unset or empty variable means no receipt storage or hashing on delivery paths. The CLI's existing `send --json` output is unchanged and is not a transcript provenance receipt.

## Operational assertions

`assertions.jsonl` contains unsigned `hcom.provenance.assertion.v1` records. Never configure it as TeamRouter's trusted receipt inbox. Records contain a random `receiptId`, `kind`, `transport`, SHA-256 `contentHash`, UTF-8 `byteLength`, recipient identity snapshot, invoking actor evidence, and preparation/recording times. Raw message text, raw terminal input, private keys, and full hook payloads are not stored.

| Boundary | Evidence and digest scope |
| --- | --- |
| `send` / `hcom_event` | A message event was stored. Digest covers its exact text, including whitespace. Event ID links it to hcom; recipient snapshots describe intended recipients. This does not mean they received it. |
| `term inject` / `pty_tcp` | The local TCP write succeeded. Text and Enter (`CR`) are separate writes and assertions. Digest covers each exact submitted byte sequence. |
| `pty_write` | All injected bytes reached the PTY writer. Digest covers the bytes after legacy single-final-LF removal and any legacy Latin-1 fallback. It does not prove native prompt acceptance. |
| Hook / `hook` | Output was written and flushed, then this acknowledgement successfully advanced the delivery cursor for the same instance incarnation. `contentHash` covers the formatted message batch; `emitted_stdout_hash` covers exact emitted JSON plus newline when that output path provides it. `event_ids` identify the batch. |
| `native_acp` | Successful Grok ACP completion and cursor acknowledgement, with observed session/prompt IDs. This is operational evidence, outside the Claude/Codex signed receipt schema. |
| `native_prompt` | A native prompt hook observed exact prompt text. Explicit message, session, agent, turn or question IDs are retained when present. Authorship remains unknown. |

The display `--from`/`-b` label is stored separately as `displayFrom`. The invoking actor uses verified Claude capabilities, process/native session bindings, or a clearly marked command identity claim. Unattributed programmatic sources are `system`; registered actors are `agent`. No external label, native `user` role, or `bigboss` spelling establishes owner identity. The raw TCP wire has no actor or operation ID, so recipient-side PTY evidence is explicitly system/unattributed, even when the sender recorded its own actor.

Hook `native_context` IDs belong to the hook invocation that triggered delivery. They must not be used as the native occurrence IDs of the hook output. Duplicate, failed, or obsolete acknowledgements produce no new delivery assertion. The legacy pre-output delivery helper, plugin numeric `--ack --up-to`, and Copilot's legacy persisted extension inflight acknowledgement do not supply enough exact output/incarnation evidence and do not emit these receipts.

## Signed inbox

`receipts.jsonl` is reserved for complete, signed TeamRouter envelopes. [Receipt signing](receipt-signing.md) documents `hcom receipt-sign`, its opt-in Ed25519 key and issuer settings, exact schema and signing bytes. Configure the archive's `receiptSources` to read this private JSONL file only. The hcom issuer scope must permit only `agent`/`system` and `hcom`/`automation` origins. No new cryptography dependency is needed.

The signer is a usable boundary for native adapters that already know a complete occurrence. Existing hook/PTY transports do not supply the necessary final native message ID and source binding, so they do not automatically call it or turn a matching text hash into a signed claim. Missing IDs leave provenance pending/unknown. TeamRouter owns signature verification, issuer scope, exact session/agent/message/turn/question/hash matching, replay enforcement and archive collection. hcom does not implement owner UI assertions or assertion/binding chains.

## Durability and gaps

Appends are locked, flushed and synced, including directory metadata. An incomplete final JSONL line from a crashed writer is discarded under lock before the next append. Complete earlier lines remain intact. Locks wait at most 100 ms; unavailable storage, invalid permissions and lock failures log an auxiliary error instead of changing successful delivery. Cursor advancement and the receipt file are separate commits: a crash between them can leave a receipt gap. Absence of a receipt never proves absence of delivery.

The PTY loop uses bounded metadata and writer queues (64 each), with persistence on a separate worker. Failed or partial writes cannot generate completion evidence. Queue exhaustion, database replacement, a stopped worker or process crash can leave gaps. Orderly shutdown drains the writer. Raw TTY keystrokes follow their existing path and are neither recorded nor intercepted by this feature. Signer configuration is stripped from newly launched child agents; receipt collection settings can remain enabled.

PTY recipient evidence requires the original launch token, instance incarnation and child PID. Missing ownership bindings or migration to a different canonical instance row suppresses these assertions. Initial native session binding on the same owned placeholder is supported. This conservative boundary keeps an old proxy from attributing its writes to a replacement session.

## Bounded checks

Run focused unit filters with `cargo test --locked --bin hcom provenance`, `receipt`, `commands::term::tests`, and `pty::forward::tests`, plus the affected hook/delivery regressions. `cargo fmt --all -- --check` and `git diff --check` need no full build. The [Node interoperability script](../scripts/check-receipt-interop.mjs) exercises the actual hcom signer against the TeamRouter verifier with disposable keys and directories. Live sessions, provider binaries, deployment and the full release/CI build are unnecessary for these checks.
