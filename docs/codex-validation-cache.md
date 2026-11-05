# Shared Codex hook validation cache

Codex per-run preparation caches **only the six hcom-owned `sessionFlags` hook
keys, commands, and hashes returned by `hooks/list`**. Session identities,
databases, messages, delivery cursors, credentials, and caller trust decisions
remain outside this cache. Each launch still creates its own hook injection and
merges its own `hooks.state` overrides. A cache hit is not a general assertion
that Codex or a routing service is healthy.

The default location is `dirs::cache_dir()/hcom/codex-hooks/v1`: typically
`~/Library/Caches/hcom/codex-hooks/v1` on macOS, `$XDG_CACHE_HOME/hcom/codex-hooks/v1`
(or `~/.cache/...`) on Linux, and the user's local cache directory on Windows.
`HCOM_CODEX_CACHE_DIR` can specify an absolute alternative, particularly for
isolated tests. It is independent of `HCOM_DIR`. Old room-local cache entries
are not imported: their weaker identity is not sufficient for shared reuse.

The key covers the cache schema, hcom version and running image, PATH-resolved
hcom hook image, selected and canonical Codex image paths and contents, Codex
version output, hook declarations, config override flags and values, working
directory configuration/trust, relevant setup environment, and local Codex
configuration/trust files. File contents are hashed, including absence; config text and environment
values are not written to the cache. Native pin retargets invalidate even when
version strings and image contents match. Failed version checks fail preparation.
Runtime `CODEX_SESSION_ID`, `CODEX_THREAD_ID`, `CODEX_TOOL_CALL_ID`, and
`CODEX_SQLITE_HOME` are not setup identity. Producers can explicitly name any
additional environment inputs that actually affect their setup.

Local inputs include home config and named profile TOML, `hooks.json`, requirements,
managed config, rules, ancestor project `.codex` files, cwd `config.toml`, and
platform system config. Relative configuration dependencies and dynamic/remote
policy cannot be inferred generally. Without a producer manifest, native launches
retain their Codex home and raw working-directory identities, and Git worktree
layouts with an external
trust root bypass reuse. An opaque script launcher always bypasses reuse unless
it supplies the contract below. This includes npm/TeamRouter wrappers: the wrapper
version string or script digest alone does not identify its downstream image.

## Launcher input contract

Set `HCOM_CODEX_CACHE_INPUTS` in the environment of the **hcom preparation process**
to an absolute JSON file path:

```json
{
  "version": 1,
  "native_binary": "/absolute/selected/native/pin/bin/codex",
  "files": [
    "/absolute/launcher/runtime.js",
    "/absolute/source/config.toml",
    "/absolute/setup-input-snapshot.json"
  ],
  "environment": ["A_SETUP_SELECTOR"]
}
```

All paths are absolute. `native_binary` must be a readable native image, not
another script. `files` is nonempty and contains readable files. `environment`
is optional. Unknown fields or versions, invalid manifests, and missing/unreadable
inputs disable reuse and run the normal preflight. The manifest's **content** is
hashed; its temporary file location is not. Use stable serialization, dependency
ordering, and stable source paths to share results across private data dirs.
Every dependency's selected path, resolved path, and contents enter the key.

The producer asserts that this manifest completely describes the downstream
image and setup used by the inspection. It must enumerate transitive local
launcher modules, interpreter/package setup inputs, native pin, source configuration,
trust inputs, external/relative config dependencies, and policy/materialization
inputs that hcom cannot discover. Rebuild the manifest before each hcom invocation;
include directory membership, missing-file state, effective external/managed policy,
and other derived inputs in a private snapshot file when necessary. Do not put
credentials, per-run IDs, SQLite paths, leases, or session/delivery data into it.
Keep files available until preparation finishes. Publish producer files atomically.
A previously supplied manifest is not proof that today's dependencies are unchanged.

With this complete contract, empty per-panel bundle directories do not contribute
their names or ancestry depth to the key. hcom rescans existing ancestor `.codex`
directories by canonical location and contents for both the launch cwd and the
inspection process cwd. Creating, deleting, or changing project configuration
changes that inventory; a cwd-local `config.toml` also contributes its path and
contents. The producer must still prove effective trust and inventory relative or
external configuration dependencies, including when config is absent. It must
regenerate its manifest for each invocation; dropping cwd identity does not make
an incomplete manifest sufficient.

Unknown or path-bearing caller `-c` overrides (including relative instruction-file
paths and caller hook commands) conservatively retain raw cwd in the key. Only
known cwd-independent scalar settings, boolean feature flags, named profiles, and
whole `projects` trust maps with absolute project keys allow cwd omission. Relative
PATH entries or setup-directory/config selectors also retain raw cwd. These guards
avoid inferring equivalence for paths whose resolution depends on the bundle.

Providing the manifest only to the final Codex child is too late. TeamRouter needs
a paired producer change on its launcher-to-hcom path. This hcom change alone
leaves the current opaque TeamRouter launcher uncached. It does not install or
activate that producer, alter global pins, or bypass its runtime checks. hcom's
existing inspection still uses ambient executable/environment resolution with
an explicit effective `CODEX_HOME`; producers must describe that actual invocation.

## Simultaneous launches and failures

A stable lock file per key coordinates processes. Waiters recheck the cache and
input fingerprint after acquiring the lock. The lock wait is capped at five
seconds; a busy/unavailable/unwritable cache falls back to a full preflight.
Different keys do not block each other. Lock files are never unlinked while
waiters could refer to their inode, and the OS releases locks on process death.

Fresh and cached results require the exact set of expected commands, unique
nonempty keys, and nonempty hashes. Truncated, malformed, wrong-schema, wrong-key,
or incomplete records trigger a new preflight. Failed inspections never publish
success. If inputs change while waiting or inspecting, preparation fails with a
retry message rather than publishing or using mismatched trust. Publication uses
a sibling temporary file, fsync, and atomic rename; a failed cache write does not
invalidate a successful fresh preflight.

Permission setup and legacy cleanup still happen on each launch. If they change
an input after validation (for example first creation of `rules/hcom.rules`), the
next launch intentionally revalidates once. Entries and lock files are disposable
user cache data; there is no automatic eviction in this change.
