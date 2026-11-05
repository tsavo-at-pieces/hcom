// The binary owns identity, the queue, and acknowledgement. This module only wakes Claude.
let timer;
let probing = false;
let submitting = false;
let submittingSession;
let submissionToken;
let pendingOutcome;
let reportingOutcome;
let outcomeBackoff = 0;
let generation = 0;
let registration = 0;
let command;
// Consecutive failed polls. A single failure is usually host sleep: the poll's
// timeout keeps counting while the process is suspended, so it aborts on wake.
let failures = 0;
const FAILURES_BEFORE_REPORT = 3;
const BUSY_WINDOW_MS = 60000;
let busySince;
let busySession;
let busyReported = false;
let elapsedNow = () => performance.now();
// Ticks to skip after a failed poll, so a broken hcom is not respawned 4x/s.
let backoff = 0;
// process.run starts in the session's working directory by default. Claude
// keeps that directory after a Bash command deletes it, and every spawn then
// fails with ENOENT until the command ends. hcom needs no particular directory.
const CWD = '/';

async function report($, session, event, detail, busy = false, wakeId) {
  try {
    const result = await $.process.run(
      [...command, 'claude-mod-poll', '--session-id', session, '--event', event, '--detail', detail,
        ...(busy ? ['--busy'] : []), ...(wakeId ? ['--wake-id', wakeId] : [])],
      { cwd: CWD, timeoutMs: 10000 },
    );
    if (result.exitCode !== 0) throw new Error(result.stderr || result.stdout);
    return true;
  } catch (error) {
    // $.ui.log is fire-and-forget: it returns nothing, so it has no .catch.
    $.ui.log('hcom mod could not record ' + event + ': ' + String(error));
    return false;
  }
}

async function flushOutcome($, outcome) {
  reportingOutcome = outcome;
  const recorded = await report($, outcome.session, outcome.event, outcome.detail, false, outcome.wakeId);
  // A reload may have started a new submission while this report was running.
  if (pendingOutcome === outcome && submissionToken === outcome.token) {
    if (recorded) {
      pendingOutcome = undefined;
      submitting = false;
    } else {
      outcomeBackoff = 20;
    }
  }
  if (reportingOutcome === outcome) reportingOutcome = undefined;
}

async function completeSubmission($, token, session, event, detail, wakeId) {
  if (submissionToken !== token) return;
  const outcome = { token, session, event, detail, wakeId };
  pendingOutcome = outcome;
  await flushOutcome($, outcome);
}

async function poll($) {
  // Outcome recording can fail after Claude completed a wake. Keep observing
  // until that exact outcome is recorded, and retry even during a long poll.
  if (pendingOutcome && !reportingOutcome) {
    if (outcomeBackoff > 0) outcomeBackoff--;
    else void flushOutcome($, pendingOutcome);
  }
  if (probing) return;
  if (backoff > 0) {
    backoff--;
    return;
  }
  probing = true;
  const currentGeneration = generation;
  const currentRegistration = registration;
  let session;
  try {
    // session.start does not fire after /clear or /resume.
    session = await $.session.id();
    if (currentRegistration !== registration) return;
    const result = await $.process.run(
      [...command, 'claude-mod-poll', '--session-id', session, ...(submitting ? ['--observe'] : [])],
      { cwd: CWD, timeoutMs: 30000 },
    );
    if (currentRegistration !== registration) return;
    if (result.exitCode === 75) {
      backoff = 20;
      const now = elapsedNow();
      if (busySince === undefined || busySession !== session) {
        busySince = now;
        busySession = session;
        busyReported = false;
      }
      if (now - busySince >= BUSY_WINDOW_MS) {
        const detail = result.stderr || result.stdout || 'SQLite contention';
        await report($, session, 'poll-failed', detail, true);
        if (currentRegistration !== registration) return;
        if (!busyReported) {
          $.ui.log('hcom mod poll delayed by SQLite contention; messages remain queued: ' + detail);
          busyReported = true;
        }
      }
      return;
    }
    busySince = undefined;
    busySession = undefined;
    busyReported = false;
    if (result.exitCode !== 0) throw new Error(result.stderr || result.stdout || 'hcom poll failed');
    const state = JSON.parse(result.stdout);
    failures = 0;
    if (currentGeneration !== generation || !state.wake || submitting) return;
    submitting = true;
    submittingSession = session;
    const token = {};
    submissionToken = token;
    // A queued submission must not block polling or a lifecycle hook. Polls
    // only observe until its outcome is reported, so hcom judges the outcome
    // against this wake's claim, never against a newer one.
    try {
      void $.prompt.submit({ text: '<hcom>', asUser: true }).then(
        async (result) => {
          // A hook may drop the wake; hcom decides whether that was a failure.
          if (result?.drop) {
            await completeSubmission($, token, session, 'submission-failed', 'wake dropped: ' + result.drop, state.wake_id);
          } else {
            await completeSubmission($, token, session, 'submitted', 'Claude completed the native wake submission', state.wake_id);
          }
        },
        async (error) => {
          await completeSubmission($, token, session, 'submission-failed', String(error), state.wake_id);
          $.ui.log('hcom mod wake failed; messages remain queued: ' + String(error));
        },
      );
    } catch (error) {
      await completeSubmission($, token, session, 'submission-failed', String(error), state.wake_id);
    }
  } catch (error) {
    if (currentRegistration !== registration) return;
    busySince = undefined;
    busySession = undefined;
    busyReported = false;
    failures++;
    backoff = 20;
    // Record every failure past the threshold: hcom clears the error after one
    // good cycle, so a repeat failure must set it again. Backoff limits this to
    // one per ~5s.
    if (failures >= FAILURES_BEFORE_REPORT) {
      if (session) await report($, session, 'poll-failed', String(error));
      if (currentRegistration !== registration) return;
      if (failures === FAILURES_BEFORE_REPORT) {
        $.ui.log('hcom mod poll failed; messages remain queued: ' + String(error));
      }
    }
  } finally {
    if (currentRegistration === registration) probing = false;
  }
}

export function register(on, monotonicNow = () => performance.now()) {
  registration++;
  elapsedNow = monotonicNow;
  timer?.cancel();
  timer = undefined;
  probing = false;
  submitting = false;
  submittingSession = undefined;
  submissionToken = undefined;
  pendingOutcome = undefined;
  reportingOutcome = undefined;
  outcomeBackoff = 0;
  generation = 0;
  failures = 0;
  busySince = undefined;
  busySession = undefined;
  busyReported = false;
  backoff = 0;
  on('session.start', async ($, e, next) => {
    if (!e.isInteractive || !await $.env.get('HCOM_PROCESS_ID') || await $.env.get('HCOM_PTY_MODE') !== '1'
        || await $.env.get('HCOM_CLAUDE_DELIVERY') !== 'native') return next(e);
    try {
      command = JSON.parse(await $.env.get('HCOM_MOD_COMMAND'));
      if (!Array.isArray(command) || command.length === 0
          || command.some(value => typeof value !== 'string' || !value)) {
        throw new Error('expected a nonempty command argument array');
      }
    } catch (error) {
      $.ui.log('hcom mod cannot start: invalid HCOM_MOD_COMMAND: ' + String(error));
      return next(e);
    }
    timer?.cancel();
    timer = $.clock.every(250, () => poll($));
    return next(e);
  });

  on('prompt.submit', async ($, e, next) => {
    if (e.text === '<hcom>' && e.origin?.kind === 'plugin'
        && (e.origin.name === 'hcom' || e.origin.name === 'hcom@inline')
        && submittingSession && await $.session.id() !== submittingSession) {
      return { drop: 'hcom wake belongs to a previous session' };
    }
    return next(e);
  });

  on('session.end', async ($, e, next) => {
    generation++;
    if (e.reason !== 'clear' && e.reason !== 'resume') timer?.cancel();
    return next(e);
  });
}
