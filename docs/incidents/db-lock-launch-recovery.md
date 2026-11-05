# Launch persistence and heartbeat SQLite contention

Tracking: [hcom #21](https://github.com/tsavo-at-pieces/hcom/issues/21).

This patch makes launch persistence atomic, retries short-lived SQLite BUSY, and
avoids repeated predicate compilation during resume replay. It is not proof that
the live rem incident is fixed. No live rem database was opened,
queried, checkpointed, vacuumed, scanned, or configured during this work.
No deployment, pin update, model call, push, merge, or session restart was done.

## Evidence and limits

The incident brief reports a roughly 463 MB DB, 458 MB WAL, 137–145 open holders,
empty `launch_args`, PTY persistence failures with SQLite code 5, and heartbeat
failures. Those observations do not identify the writer holding the lock.
FTS5 automerge and checkpoint stalls remain unproven hypotheses.

Source inspection found a direct partial-registration path: the launcher created
an instance and process binding separately, then wrote arguments through
`update_instance_position`, which logs and suppresses write errors. Spawning
continued after an argument write failed. The signed tests-first commit is
`13291ca`; its new launch registration, cleanup, and heartbeat regressions fail
on the unchanged implementation.

An isolated diagnostic uses the real `initialize_launch_instance` resume path,
128 saved subscriptions, and 1,000 queued status events. Ordinary nonmatching
predicates avoid notifications or model calls. The holder contains no injected
sleep. It restores subscriptions and replays history under `BEGIN IMMEDIATE`.
A second connection attempts a KV write with a 5,000 ms busy timeout.

Before predicate preparation caching, the first measurement on the loaded Studio
took **52,213 ms** including scope completion/logging (**52,186 ms** measured
writer ownership); the contender failed with SQLite BUSY
after **5,273 ms**. This proves this production path can exceed the default
busy timeout with the specified synthetic workload. It does not establish that
these subscription counts or this writer caused the live incident. Absolute
numbers depend on host load and the unoptimized test build.

The distinct-predicate baseline used 128 different ordinary predicates against
1,000 events: **41,743 ms** scope elapsed / **41,742 ms** writer ownership; its
contender failed BUSY after **5,249 ms**. An optional 512-identical-subscription
baseline completed with **185,068 ms** measured writer ownership. It is not a
claim about live subscription counts.

## Bounded replay optimization

The initial experiment used `prepare_cached` for each predicate. Observed scope
times improved to 8,022 ms for identical predicates but remained 32,046 ms for
128 distinct predicates, exceeding the launch budget. That approach is not the
shipped implementation.

The final implementation retains prepared predicates for one replay invocation,
keyed by the complete SQL after collision-filter upgrades. It retains at most
256 statements and 256 KiB of SQL key text. Overflow queries execute normally
without evicting admitted entries. This limits retained work without changing
any connection-wide cache capacity or live room setting. Compiled statements
have additional memory overhead; the text cap is not a total-memory claim.

Every evaluation binds the current event ID and all parameters. Preparation or
query failures remain nonmatches. Subscription records/results are not cached:
each event rereads current KV rows, including once deletion, request-watch
transitions and SQL upgrades. Saved cursors, event order, and deferred reactions
are unchanged. Reset statements are dropped before reactions are dispatched.
Replay stays in the same write transaction, preserving pre-spawn unread behavior.
The 1,000-event replay cap is unchanged. Predicates beyond retention bounds and
other work in the transaction can still produce long holds.

Final observed scope durations on the loaded Studio:

| 128 subscriptions × 1,000 events | Baseline | Ordinary cache experiment | Replay-local cache |
| --- | ---: | ---: | ---: |
| Identical predicates | 52,213 ms | 8,022 ms | 11,736 ms |
| Distinct predicates | 41,743 ms | 32,046 ms | 25,637 ms |

The final measured writer ownership was 11,716 ms / 25,634 ms respectively.
Five-second competitors still failed BUSY (5,230 ms / 5,185 ms). These are
single observations under changing host load, not controlled benchmarks or
latency promises. Deterministic tests separately establish statement reuse.

**The distinct fixture still exceeds the 15-second launch admission budget.**
The optimization removes repeated explicit predicate preparation within its
bounds and materially shortens the reproduced hold, but does not eliminate
contention or guarantee that launch/heartbeat admission will succeed. It is not
a complete live-incident resolution claim. A follow-up should profile remaining
subscription loading/JSON work and SQLite reprepare counts, then evaluate a
bounded replay-state strategy with explicit invalidation for transitions, once
removal and cursor writes. That is a broader semantic change than prepared SQL
reuse; this patch deliberately retains fresh state reads and transaction scope.

## Behavior

- Launch arguments are the existing pre-injection snapshot, serialized and
  committed atomically with registration, cursor, launch token and binding.
  A required write error prevents spawning. No provenance check is weakened.
- Launch persistence/cleanup admission has a 15-second monotonic budget;
  heartbeat admission has eight seconds. SQLite waits use slices of at most
  250 ms; retry backoff grows from 10 to 100 ms. The original connection timeout
  is restored. Only BUSY, BUSY_RECOVERY and BUSY_TIMEOUT are retryable;
  LOCKED, BUSY_SNAPSHOT, constraints and I/O failures are not.
- Only `BEGIN IMMEDIATE` admission is retried. A callback executes once after
  admission. Nested transactions use savepoints without independent retries.
  **These are admission budgets, not complete-operation deadlines.** A writer
  holding the lock longer than the budget can still cause a clean failure.
- PID/context persistence follows the exact launch token through canonical
  renames, rejects missing ownership, and rolls back together. PTY persistence
  failure keeps the existing child/group termination and reap behavior.
  Windows still collects slow terminal metadata only after starting its reader.
- Cleanup deletes only the row still owned by the failed launch and its binding
  in one transaction. Cleanup and rollback failures are surfaced; a failed
  cleanup can leave a complete registration and is reported as such. New owners
  are not deleted or overwritten by late metadata.
- Heartbeat timestamps are captured before waiting. Stdio ownership and stopped
  state predicates are still checked within the UPDATE. Claude PR14/15/18 poll
  protocol retry behavior is unchanged.

## Measurement instrumentation

Slow common write scopes and the direct tool-checked binding transaction emit
`db.write_timing` to the existing rotating filesystem JSONL log after release.
Fields include PID, database path, operation, source caller, `wait_ms`, `held_ms`
and `released`. An unreleased transaction is labeled `unreleased_ms` instead.
Nested savepoints do not claim independent lock ownership. The threshold is
250 ms. Arguments and event bodies are not logged.

Autocommit event inserts emit `db.event_insert_timing` with
`statement_elapsed_ms`. This includes admission, trigger/FTS work and commit;
it must not be interpreted as measured lock ownership. These probes cover common
transaction helpers, tool binding and autocommit event insertion, not every raw
SQL statement in the repository. An in-progress or killed writer has no completed
scope timing record.

The timing regression separately measured a deliberately held fixture scope and
its waiter: 356 ms held/0 ms waiting, versus 0 ms held/355 ms waiting. This checks
the instrumentation, independently of the real subscription-replay diagnostic.

## Validation

All Rust runs use disposable HOME/HCOM_DIR, `CARGO_BUILD_JOBS=4`, and
`RUST_TEST_THREADS=4`; Cargo/Rustup homes point to the installed toolchain cache.
The tests-first baseline ran `cargo test --bin hcom launch_ -- --nocapture`
(184 passed, three new tests failed) and
`cargo test --bin hcom heartbeats_recover_after_busy_timeout -- --nocapture`
(one new test failed with code 5).

Focused final commands and results (each Rust filter uses
`cargo test --locked --bin hcom FILTER -- --nocapture`):

| Filter | Passed |
| --- | ---: |
| `launch_` | 189 |
| `db::contention::tests` | 4 |
| `db::instances::tests` | 22 |
| `db::subscriptions::tests` | 51 |
| `hooks::claude_mod::tests` | 28 |
| `instance_binding::tests::test_tool_checked` | 2 |
| `launcher::tests::stdio_tap_` | 7 |
| `pty::tests::pty_persistence` | 1 |
| `commands::listen::tests` | 6 |

Filters overlap; these counts are not a unique-test total. The 49 existing
subscription tests passed before the optimization. Added equivalence tests cover
interleaved parameterized filters, event-ID rebinding, once removal and cursors,
257 distinct predicates, bounded overflow, malformed SQL/parameter recovery, and
actual statement reuse without a timing assertion. Existing replay regressions
cover saved cursors, deferred reactions and request-watch transitions.

`cargo test --locked --test codex_stdio -- --nocapture` passed 20 fixture tests,
including protocol preservation, resume/canonical cleanup, cancellation, stale
launch-token protection, hook image retention and existing validation-cache
behavior. Fixtures do not call models. `cargo fmt --all -- --check` and
`git diff --check` passed. Windows was not executed on this macOS host.

The existing Claude mod JS suite passed 28 tests using Node's test/assert shim
for the `claude-code/testing` import. No polling code was changed. The shim is
saved with the logs; it replaces only the test framework import and the register
module path and uses the unchanged test bodies.

To repeat the isolated measurement, use the same disposable environment and run:

```sh
cargo test --locked --bin hcom diagnose_resume_subscription_replay_writer_duration -- --ignored --nocapture
HCOM_REPLAY_DISTINCT=1 cargo test --locked --bin hcom diagnose_resume_subscription_replay_writer_duration -- --ignored --nocapture
```

`HCOM_REPLAY_DISTINCT` is read only by the ignored test. These are measurements,
not assertions of a fixed latency/speedup. Evidence logs remain in the workspace's
ignored `target/db-lock-evidence/` directory. No Windows host was available;
Windows startup ordering was source-reviewed and its existing delayed metadata
path retained.


## Review handoff

Base: `723550bf63f934cc9d2def0f7d8831248306388c` (fetched main with PR17–20).
Tests-first commit: `13291ca`; containment/ownership commit: `4cff51b`.
Both signatures verified `G`. The final replay source blob tested throughout the
final checks is `af4c95fd72c0386875f05993fd863d7e9d10ad79`.
`tested-source-blobs.json` records every source blob; the final commit was checked
against it. Only this document changed after those checks. Read-only native
review found no remaining source blockers. Root review and any publication or
rollout remain separate; nothing has been pushed or deployed.
