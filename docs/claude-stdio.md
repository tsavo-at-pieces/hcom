# Claude pipe clients

On Unix, `hcom claude --stdio --go -- -p ...` runs one foreground Claude
print process with the caller's stdin, stdout and stderr. This supports clients
such as the Claude Agent SDK, whose JSON control channel cannot run in a PTY.
The client must supply its own native Claude flags, including `-p`/`--print`.
hcom preserves the native arguments and does not add stream format defaults.

With `--output-format stream-json`, the SDK protocol, Claude writes stdout
into a pipe that hcom copies to the caller byte for byte: each chunk is
written as soon as it is read, and nothing is added, changed, reordered or
held back for a whole line. hcom reads the copied lines only to find the
frames that end a turn. A caller that closes stdout gives Claude the broken
pipe it would otherwise get. Everything Claude wrote before it exited reaches
a caller that keeps stdout open, however slowly it reads, before hcom exits,
with two exceptions that stop hcom without the output the caller has not
taken, as they would have stopped a Claude still blocked writing to the
caller: SIGHUP, SIGINT or SIGTERM reaching hcom once Claude has exited stops
it at once, and after hcom has forwarded one of them to Claude (or killed
Claude after a failed launch), it stops once Claude has exited and one write
to the caller has waited 5 seconds. If a descendant of Claude still holds the
pipe, hcom stops copying once it has been empty for 5 seconds or, while the
descendant keeps writing, the next time it is empty after 30 seconds. stdin
and stderr are inherited, and so is stdout for any other output format and
for Codex.

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
For a stream-json launch, hcom refreshes the session's heartbeat every 5
seconds until the child is reaped, so an idle pipe client is not reported
stale. A single statement sets only the heartbeat, on the row that this
launch's process binding, launch token and row incarnation still name: a
stopped row, or one another launch replaced or took over, is never touched.
Other output formats get no heartbeat, so their idle rows read as stale after
10 seconds.

The client owns turn completion and idle input. `Stop` drains messages already
queued, then returns immediately when none remain. Messages arriving while the
client is idle are delivered by the next native prompt or tool/Stop hook.
hcom does not inject synthetic SDK prompts. Pending delivery is acknowledged
only after hook output has been flushed, as in the other native modes.

A turn the client cancels runs no `Stop` hook. When its `result` frame reports
an aborted turn, hcom marks the session `listening` with context `interrupted`
(another turn that ends without `Stop` is marked `listening` too, or with a
`failure:<reason>` context for an error). A `session_state_changed` frame with
state `idle`, which Claude writes after each result when the client enables
session state events, marks a session that is still busy `listening`. A result
followed by a queued turn, or written during a shutdown, changes nothing.
hcom records these off the stdout copy, so a busy database never delays
output; one that another writer's lock keeps out past the 5 second busy
timeout is tried again while Claude runs, before anything read after it,
until it is recorded or refused. It is recorded only while the row is still
this launch's, in the frame's session and `active` or `blocked`, with no
prompt or prompt delivery recorded since the frame may have been written
(the last time hcom saw the pipe empty before reading it). When hcom cannot
tell, for example because it read a whole turn at once, the status stays as
the hooks left it. Nothing on stdout stops a session or delivers a message:
queued messages still wait for the next native prompt.

`--stdio` supports one local Claude print or Codex app-server process. It
cannot be combined with multiple agents, an unsupported tool, a remote device,
a terminal preset, `--headless`, `--json`, or `--no-run-here`. It is a launch
transport; Claude clients resume the native conversation by passing
`--resume <session-id>` under a fresh `hcom claude --stdio` launch. Codex clients
use `hcom codex --stdio --run-here --go -- app-server` and choose or resume
their thread through the native JSON-RPC protocol. Existing interactive and
detached print modes retain their behavior.
