# Claude pipe clients

On Unix, `hcom claude --stdio --go -- -p ...` runs one foreground Claude
print process with the caller's stdin, stdout and stderr. This supports clients
such as the Claude Agent SDK, whose JSON control channel cannot run in a PTY.
The client must supply its own native Claude flags, including `-p`/`--print`.
hcom preserves the native arguments and does not add stream format defaults.

With `--output-format stream-json`, the SDK protocol, hcom forwards Claude's
stdout to the caller byte-for-byte: it never adds, alters or reorders output,
and a caller that closes stdout gives Claude the same broken pipe as before.
This lets hcom see the `result` frame that ends every turn. Other output
formats are inherited directly.

hcom prepares its usual per-launch hooks, reserves a fresh process identity,
and remains the native child's parent. The child runs the executable a lookup
like hcom's install check resolves, made absolute before the child changes to
its working directory, with the launched environment minus a parent Claude's
instance state (`CLAUDE_CODE_CHILD_SESSION` and the messaging socket and token
belong to that parent and would stop the new session's transcript). Its hooks
name the canonical path of the hcom image that launched it, and its batch and
launch-failure events carry the launcher and batch that hcom resolved for it,
not the environment of the hcom process itself. It does not create a terminal, print a
launch banner, or redirect output to a background log. Native `SessionStart`
binds the real session ID. Signals reach the child's process group; hcom returns
its exit status and cleans up its registration if `SessionEnd` was not emitted.

The client owns turn completion and idle input. `Stop` drains messages already
queued, then returns immediately when none remain. Messages arriving while the
client is idle are delivered by the next native prompt or tool/Stop hook.
hcom does not inject synthetic SDK prompts. Pending delivery is acknowledged
only after hook output has been flushed, as in the other native modes.

A turn the client cancels runs no `Stop` hook. When its `result` frame reports
an aborted turn, hcom marks the session `listening` with context `interrupted`
(another turn that ends without `Stop` is marked `listening` too, or with a
`failure:<reason>` context for an error). hcom reads the session's state before
it forwards the frame and records the change just after, so it never overwrites
a later prompt, a stopped session or another session's row. Queued messages
still wait for the next native prompt.

`--stdio` supports one local Claude print or Codex app-server process. It
cannot be combined with multiple agents, an unsupported tool, a remote device,
a terminal preset, `--headless`, `--json`, or `--no-run-here`. It is a launch
transport; Claude clients resume the native conversation by passing
`--resume <session-id>` under a fresh `hcom claude --stdio` launch. Codex clients
use `hcom codex --stdio --run-here --go -- app-server` and choose or resume
their thread through the native JSON-RPC protocol. Existing interactive and
detached print modes retain their behavior.
