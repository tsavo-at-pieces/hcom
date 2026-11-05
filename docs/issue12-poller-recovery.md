# Claude completed wake with failed delivery acknowledgement

Investigation against `e2e90c9`, issue #12's 2026-11-05 14:50Z comment.

## Incident evidence

Read-only diagnostic logs for banuzo in the rem room establish this sequence
(UTC, 2026-11-05):

| Time | Diagnostic |
| --- | --- |
| 14:04:35 | `claude.mod_wake_requested` |
| 14:04:41 | `commit_delivery_ack`: `database is locked` |
| 14:04:51 | `userpromptsubmit` hook exits 0 |
| 14:04:52 | `claude.mod_submitted`, then `claude.mod_unacknowledged` |
| 14:11:59 | Another `claude.mod_poll_busy` |
| 14:13:56 | Successful polling restores `claude.mod_unacknowledged` |
| 14:55:45 | First subsequent `claude.mod_acknowledged` |

The 14:11:59 poll error was not the beginning of the unrecovered delivery.
`write_hook_output` flushes context before `commit_delivery_ack`. That commit
logs a database failure but does not undo output or fail prompt submission.
The mod's successful `submitted` report previously did nothing to its durable
`attempted_cursor`. Every later poll suppressed the same cursor indefinitely,
even after SQLite recovered. Subsequent messages could not trigger another wake.

The proposed missing-rebind cause is incorrect for this reproducible incident.
Every poll creates a fresh `NotifyServer` after opening/loading the database.
A failed startup can leave an earlier stale endpoint and no socket; a healthy
respawn replaces it. A no-socket snapshot does not prove a persistent listener
failure. `mod-unacknowledged` also requires a recent successful poll; a stopped
poller becomes `mod-unavailable` after the existing health timeout.

Banuzo's loaded plugin path ends in `claude-mod/50f6402258763588619506d8d755f531`.
Its `register.js` SHA256 is
`8f5d71bb7b829a67b9bf7bd3501a67a62841a1993a6ea5da8ac5a21c219f0951`,
matching git blob `e15557b9053728a012186dccda3ff5cd3e33dd9c` (pre-BUSY handling).
The launch script uses `HCOM_MOD_COMMAND=["hcom"]`, so the binary resolves
through PATH while the loaded JS remains fixed. Logs showing `poll_busy` followed
by ordinary `poll-failed` are consistent with newer Rust and older loaded JS.
The current binary is `a2ca3d02efda`; its identity at incident time is not proven.
No private message bodies were needed for this diagnosis.

## Recovery contract

Each newly claimed wake has a unique token. Only a matching completed-submission
report makes an unacknowledged claim retryable, after a bounded delay and while
the session is idle. Completion does not advance the delivery cursor. A prompt
still queued behind a menu remains single-flight, however long it waits.

The JS bridge retains failed outcome reports and retries them while polls observe.
Duplicate reports are idempotent. Stale and tokenless reports cannot make a newer
tokenized claim retryable. Only the existing hook acknowledgement consumes messages.
A failed acknowledgement after output permits at-least-once redelivery of that
batch; duplicate context is possible and preferable to suppressing all future wakes.

Completed-but-unacknowledged health describes recovery, rather than suggesting a
menu. Outstanding submissions still describe possible queued prompts; stopped
polling and SQLite contention keep their existing distinct health states.

This protocol needs the new loaded JS as well as the new binary. Updating only
the binary beneath an existing session remains conservative and cannot repair
legacy tokenless outcomes. Tokenless outcomes for a new claim surface as
`mod-outdated` with a reload diagnostic, without rearming or suppressing a
newer identified submission. No live session, binary pin, daemon or settings were
changed during this work. An operator-controlled future reload/restart is needed
to load the new module. A previously ambiguous legacy claim may additionally
need a normal user prompt to make hook progress; merely loading the new module
does not discard such a claim.

An abandoned poll result before JS receives the wake, or a module/process loss
before its completion report is persisted, remains ambiguous. Such claims are
not expired blindly: Claude could still hold the original prompt behind a menu.
This bounded fix addresses the proven completed-submission/failed-ack incident,
not a general exactly-once prompt protocol.

## Validation

The isolated regression holds a real SQLite writer lock over the hook ack,
proves the cursor remains unchanged, records completion, and requires a new wake.
The original implementation fails that assertion. Additional regressions cover
outcome report loss, token fencing, bounded retries, menu single-flight, and
stale endpoint replacement after a failed poll startup. Runtime fixtures use
temporary databases and home directories.

Final local results:

- `cargo test --locked --no-fail-fast`, from the physical `.superset` worktree
  with temporary `HCOM_DIR`: 2,976 passed, 21 ignored, no failures.
- `cargo test --locked --bin hcom hooks::claude_mod::tests`: 28 passed.
- `cargo clippy --all-targets --locked -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- All 28 `register.test.ts` cases passed through an isolated Node test/expect
  shim. Against the baseline JS, six new regressions failed. This is not a test
  of Claude's proprietary runtime.
- An isolated CLI smoke test exercised actual child processes: BUSY completion
  report/retry with wake IDs; killed poller leaving a stale endpoint; locked
  respawn returning 75; subsequent respawn binding, accepting a TCP wake and
  removing its own endpoint. Both scenarios passed with unchanged cursor.

Before/after logs, the diagnostic-only incident excerpt, and local rerunnable
Node/CLI harnesses are in `target/issue12-evidence/` in this workspace. The first
full-suite log is retained: it passed all 2,823 unit tests but failed an unrelated
bundled-script assertion comparing `.paradev` against Python's canonical
`.superset` path. Running from the physical path passed the same test unchanged.
No real model/provider tests were enabled or tools installed.
