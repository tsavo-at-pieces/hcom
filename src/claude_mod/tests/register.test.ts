import { expect, test } from 'claude-code/testing'
import { register } from '../hooks/register.js'

function harness() {
  const handlers = {} as Record<string, Function>
  let tick: Function
  let cancelled = false
  const prompts = [] as any[]
  let response = { wake: true }
  let resolvePrompt: Function
  let now = 1000
  let monotonic = 1000
  const api = {
    env: { get: async (key: string) => key === 'HCOM_PROCESS_ID' ? 'process-1' : key === 'HCOM_PTY_MODE' ? '1' : key === 'HCOM_CLAUDE_DELIVERY' ? 'native' : '["hcom"]' },
    clock: { now: async () => now, every: (_: number, fn: Function) => { tick = fn; return { cancel: () => { cancelled = true } } } },
    session: { id: async () => 'session-1' },
    process: { run: async () => ({ exitCode: 0, stdout: JSON.stringify(response) }) },
    prompt: { submit: (value: any) => { prompts.push(value); return new Promise(resolve => { resolvePrompt = resolve }) } },
    // Claude's $.ui.log is synchronous and returns undefined.
    ui: { log: (_: string) => {} },
  }
  register((event: string, fn: Function) => { handlers[event] = fn }, () => monotonic)
  return {
    api, handlers, prompts,
    tick: () => tick(api),
    cancelled: () => cancelled,
    response: (value: any) => { response = value },
    finishPrompt: () => resolvePrompt({ text: '<hcom>', asUser: true }),
    advance: (ms: number) => { now += ms; monotonic += ms },
    stepWall: (ms: number) => { now += ms },
  }
}
const next = async (event: any) => event
const interactive = { isInteractive: true, surface: 'terminal', cwd: '/work' }

test('one pending submission while the bridge keeps polling', async () => {
  const h = harness()
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  await h.tick()
  expect(h.prompts.length).toBe(1)
  expect(h.prompts[0]).toEqual({ text: '<hcom>', asUser: true })
  h.finishPrompt()
  await Promise.resolve()
  h.response({ wake: false })
  await h.tick()
  expect(h.prompts.length).toBe(1)
})

test('an old poll cannot submit after a session switch', async () => {
  const h = harness()
  let finish: Function
  h.api.process.run = () => new Promise(resolve => { finish = resolve }) as any
  await h.handlers['session.start'](h.api, interactive, next)
  const pending = h.tick()
  for (let i = 0; i < 5; i++) await Promise.resolve()
  await h.handlers['session.end'](h.api, { reason: 'resume' }, next)
  finish!({ exitCode: 0, stdout: '{"wake":true}' })
  await pending
  expect(h.prompts.length).toBe(0)
  expect(h.cancelled()).toBe(false)
})

test('headless sessions do not start polling', async () => {
  const h = harness()
  await h.handlers['session.start'](h.api, { ...interactive, isInteractive: false }, next)
  expect(h.prompts.length).toBe(0)
})

test('session exit cancels polling', async () => {
  const h = harness()
  await h.handlers['session.start'](h.api, interactive, next)
  await h.handlers['session.end'](h.api, { reason: 'prompt_input_exit' }, next)
  expect(h.cancelled()).toBe(true)
})

test('a queued mod wake cannot enter a different session', async () => {
  const h = harness()
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  h.api.session.id = async () => 'session-2'
  const result = await h.handlers['prompt.submit'](h.api, {
    text: '<hcom>', origin: { kind: 'plugin', name: 'hcom' },
  }, next)
  expect(result.drop).toBe('hcom wake belongs to a previous session')
  h.finishPrompt()
})

test('failed submission is reported without switching transport', async () => {
  const h = harness()
  const reports = [] as string[][]
  h.api.prompt.submit = () => Promise.reject(new Error('submission denied'))
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) reports.push(argv)
    return { exitCode: 0, stdout: '{"wake":true}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  for (let i = 0; i < 20; i++) await Promise.resolve()
  expect(h.cancelled()).toBe(false)
  expect(reports.length).toBe(1)
  expect(reports[0].includes('submission-failed')).toBe(true)
  expect(reports[0].includes('Error: submission denied')).toBe(true)
})

test('disabled experimental mode does not start polling', async () => {
  const h = harness()
  const get = h.api.env.get
  h.api.env.get = async (key: string) => key === 'HCOM_CLAUDE_DELIVERY' ? 'inject' : get(key)
  await h.handlers['session.start'](h.api, interactive, next)
  expect(h.prompts.length).toBe(0)
})

test('polling observes without claiming while a submission is pending', async () => {
  const h = harness()
  const calls = [] as string[][]
  h.api.process.run = async (argv: string[]) => {
    calls.push(argv)
    return { exitCode: 0, stdout: '{"wake":true}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  await h.tick()
  expect(calls[0].includes('--observe')).toBe(false)
  expect(calls[1].includes('--observe')).toBe(true)
  expect(h.prompts.length).toBe(1)
  h.finishPrompt()
})

test('invalid mod command reports startup failure without blocking the hook', async () => {
  for (const value of [undefined, '{broken', '[]', '"hcom"', '[1]']) {
    const h = harness()
    const logs = [] as string[]
    const get = h.api.env.get
    h.api.env.get = async (key: string) => key === 'HCOM_MOD_COMMAND' ? value as any : get(key)
    h.api.ui.log = (text: string) => { logs.push(text) }
    const result = await h.handlers['session.start'](h.api, interactive, next)
    expect(result).toEqual(interactive)
    expect(logs.length).toBe(1)
    expect(logs[0].includes('invalid HCOM_MOD_COMMAND')).toBe(true)
    expect(h.prompts.length).toBe(0)
  }
})

test('a dropped wake is reported as a failure, not a submission', async () => {
  const h = harness()
  const reports = [] as string[][]
  h.api.prompt.submit = () => Promise.resolve({ drop: 'blocked by another plugin' }) as any
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) reports.push(argv)
    return { exitCode: 0, stdout: '{"wake":true}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  for (let i = 0; i < 20; i++) await Promise.resolve()
  expect(reports.length).toBe(1)
  expect(reports[0].includes('submission-failed')).toBe(true)
  expect(reports[0].includes('wake dropped: blocked by another plugin')).toBe(true)
})

test('polls keep observing until a finished wake is reported', async () => {
  const h = harness()
  const calls = [] as string[][]
  let finishReport: Function
  h.api.process.run = (argv: string[]) => {
    calls.push(argv)
    if (argv.includes('--event')) return new Promise(resolve => { finishReport = resolve }) as any
    return Promise.resolve({ exitCode: 0, stdout: '{"wake":true}' }) as any
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  h.finishPrompt()
  for (let i = 0; i < 5; i++) await Promise.resolve()
  // The outcome report is still running: a poll now must not claim a wake.
  await h.tick()
  expect(calls.at(-1)!.includes('--observe')).toBe(true)
  expect(h.prompts.length).toBe(1)
  finishReport!({ exitCode: 0, stdout: '{"wake":false}' })
  for (let i = 0; i < 5; i++) await Promise.resolve()
  await h.tick()
  expect(calls.at(-1)!.includes('--observe')).toBe(false)
  expect(h.prompts.length).toBe(2)
  h.finishPrompt()
})

test('a completed wake retries a BUSY outcome report before releasing observe mode', async () => {
  const h = harness()
  const calls = [] as string[][]
  let reports = 0
  let polls = 0
  h.api.process.run = async (argv: string[]) => {
    calls.push(argv)
    if (argv.includes('--event')) {
      reports++
      return reports === 1
        ? { exitCode: 75, stdout: '{"busy":true}' }
        : { exitCode: 0, stdout: '{}' }
    }
    polls++
    return { exitCode: 0, stdout: JSON.stringify({ wake: polls === 1, wake_id: 'wake-1' }) }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  h.finishPrompt()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  for (let i = 0; i < 20; i++) await h.tick()
  expect(reports).toBe(1)
  expect(calls.filter(c => !c.includes('--event')).slice(1).every(c => c.includes('--observe'))).toBe(true)
  expect(h.prompts.length).toBe(1)
  await h.tick()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  expect(reports).toBe(2)
  const outcomes = calls.filter(c => c.includes('--event'))
  expect(outcomes.every(c => c.includes('submitted') && c[c.indexOf('--wake-id') + 1] === 'wake-1')).toBe(true)
  await h.tick()
  expect(calls.at(-1)!.includes('--observe')).toBe(false)
  expect(h.prompts.length).toBe(1)
})

test('a dropped wake retains and retries its exact failure outcome', async () => {
  const h = harness()
  const reports = [] as string[][]
  const polls = [] as string[][]
  h.api.prompt.submit = () => Promise.resolve({ drop: 'submission denied' }) as any
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) {
      reports.push(argv)
      if (reports.length === 1) throw new Error('report process timed out')
      return { exitCode: 0, stdout: '{}' }
    }
    polls.push(argv)
    return { exitCode: 0, stdout: JSON.stringify({ wake: polls.length === 1, wake_id: 'wake-2' }) }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  for (let i = 0; i < 20; i++) await h.tick()
  expect(reports.length).toBe(1)
  expect(polls.slice(1).every(c => c.includes('--observe'))).toBe(true)
  await h.tick()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  expect(reports.length).toBe(2)
  expect(reports[1]).toEqual(reports[0])
  expect(reports[1].includes('submission-failed')).toBe(true)
  expect(reports[1][reports[1].indexOf('--wake-id') + 1]).toBe('wake-2')
  await h.tick()
  expect(polls.at(-1)!.includes('--observe')).toBe(false)
})

test('outcome retries continue while an observe poll is still running', async () => {
  const h = harness()
  let reports = 0
  let polls = 0
  let finishPoll: Function
  h.api.process.run = (argv: string[]) => {
    if (argv.includes('--event')) {
      reports++
      return Promise.resolve({ exitCode: reports === 1 ? 75 : 0, stdout: '{}' }) as any
    }
    polls++
    if (polls === 1) return Promise.resolve({ exitCode: 0, stdout: '{"wake":true,"wake_id":"wake-3"}' }) as any
    return new Promise(resolve => { finishPoll = resolve }) as any
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  const pending = h.tick()
  h.finishPrompt()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  for (let i = 0; i < 21; i++) await h.tick()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  expect(reports).toBe(2)
  expect(polls).toBe(2)
  expect(h.prompts.length).toBe(1)
  finishPoll!({ exitCode: 0, stdout: '{"wake":false}' })
  await pending
})

test('an unresolved menu submission is never retried or reported as completed', async () => {
  const h = harness()
  const calls = [] as string[][]
  h.api.process.run = async (argv: string[]) => {
    calls.push(argv)
    return { exitCode: 0, stdout: '{"wake":true,"wake_id":"menu-wake"}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  h.advance(35 * 60 * 1000)
  for (let i = 0; i < 100; i++) await h.tick()
  expect(h.prompts.length).toBe(1)
  expect(calls.slice(1).every(c => c.includes('--observe') && !c.includes('--event'))).toBe(true)
})

test('a completion from before module reload cannot release a newer submission', async () => {
  const old = harness()
  const oldReports = [] as string[][]
  old.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) oldReports.push(argv)
    return { exitCode: 0, stdout: '{"wake":true,"wake_id":"old-wake"}' }
  }
  await old.handlers['session.start'](old.api, interactive, next)
  await old.tick()
  const current = harness()
  const calls = [] as string[][]
  current.api.process.run = async (argv: string[]) => {
    calls.push(argv)
    return { exitCode: 0, stdout: '{"wake":true,"wake_id":"new-wake"}' }
  }
  await current.handlers['session.start'](current.api, interactive, next)
  await current.tick()
  old.finishPrompt()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  await current.tick()
  expect(oldReports.length).toBe(0)
  expect(current.prompts.length).toBe(1)
  expect(calls.at(-1)!.includes('--observe')).toBe(true)
})

test('an in-flight report from before reload cannot release a newer submission', async () => {
  const old = harness()
  let finishReport: Function
  old.api.process.run = (argv: string[]) => {
    if (argv.includes('--event')) return new Promise(resolve => { finishReport = resolve }) as any
    return Promise.resolve({ exitCode: 0, stdout: '{"wake":true,"wake_id":"old-wake"}' }) as any
  }
  await old.handlers['session.start'](old.api, interactive, next)
  await old.tick()
  old.finishPrompt()
  for (let i = 0; i < 10; i++) await Promise.resolve()
  const current = harness()
  const calls = [] as string[][]
  current.api.process.run = async (argv: string[]) => {
    calls.push(argv)
    return { exitCode: 0, stdout: '{"wake":true,"wake_id":"new-wake"}' }
  }
  await current.handlers['session.start'](current.api, interactive, next)
  await current.tick()
  finishReport!({ exitCode: 0, stdout: '{}' })
  for (let i = 0; i < 10; i++) await Promise.resolve()
  await current.tick()
  expect(current.prompts.length).toBe(1)
  expect(calls.at(-1)!.includes('--observe')).toBe(true)
})

test('an old in-flight poll cannot submit or release a newer probe after reload', async () => {
  const old = harness()
  let finishOld: Function
  old.api.process.run = () => new Promise(resolve => { finishOld = resolve }) as any
  await old.handlers['session.start'](old.api, interactive, next)
  const pendingOld = old.tick()
  for (let i = 0; i < 5; i++) await Promise.resolve()
  const current = harness()
  let finishCurrent: Function
  let polls = 0
  current.api.process.run = () => {
    polls++
    return new Promise(resolve => { finishCurrent = resolve }) as any
  }
  await current.handlers['session.start'](current.api, interactive, next)
  const pendingCurrent = current.tick()
  for (let i = 0; i < 5; i++) await Promise.resolve()
  finishOld!({ exitCode: 0, stdout: '{"wake":true,"wake_id":"old-wake"}' })
  await pendingOld
  expect(old.prompts.length).toBe(0)
  await current.tick()
  expect(polls).toBe(1)
  finishCurrent!({ exitCode: 0, stdout: '{"wake":true,"wake_id":"new-wake"}' })
  await pendingCurrent
  expect(current.prompts.length).toBe(1)
})

test('a failed poll backs off instead of respawning every tick', async () => {
  const h = harness()
  let polls = 0
  h.api.process.run = async (argv: string[]) => {
    if (!argv.includes('--event')) polls++
    return { exitCode: 1, stdout: '', stderr: 'database is locked' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  for (let i = 0; i < 21; i++) await h.tick()
  expect(polls).toBe(1)
  await h.tick()
  expect(polls).toBe(2)
})

// One failure is a tick plus the 20 backoff ticks that follow it.
async function fail(h: ReturnType<typeof harness>, n: number) {
  for (let i = 0; i < n * 21; i++) await h.tick()
}

test('transient BUSY stays quiet inside its window and keeps the backoff cadence', async () => {
  const h = harness()
  const reports = [] as string[][]
  const logs = [] as string[]
  let polls = 0
  h.api.ui.log = text => { logs.push(text) }
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{}' } }
    polls++
    return { exitCode: 75, stdout: '{"busy":true,"error":"database is locked"}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 4)
  expect(polls).toBe(4)
  expect(reports.length).toBe(0)
  expect(logs.length).toBe(0)
  h.api.process.run = async () => ({ exitCode: 0, stdout: '{"wake":true}' })
  await h.tick()
  expect(h.prompts.length).toBe(1)
  h.finishPrompt()
})

test('BUSY past sixty seconds reports each retry with transient context and logs once', async () => {
  const h = harness()
  const reports = [] as string[][]
  const logs = [] as string[]
  let busy = true
  h.api.ui.log = text => { logs.push(text) }
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{}' } }
    return busy ? { exitCode: 75, stdout: '{"busy":true}' } : { exitCode: 0, stdout: '{"wake":false}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 3)
  expect(reports.length).toBe(0)
  h.advance(60001)
  await fail(h, 2)
  expect(reports.length).toBe(2)
  expect(reports.every(r => r.includes('poll-failed') && r.includes('--busy'))).toBe(true)
  expect(logs.length).toBe(1)
  busy = false
  await h.tick()
  busy = true
  await fail(h, 4)
  expect(reports.length).toBe(2)
})

test('BUSY elapsed threshold ignores forward and backward wall-clock steps', async () => {
  const h = harness()
  const reports = [] as string[][]
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{}' } }
    return { exitCode: 75, stdout: '{"busy":true}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 1)
  h.advance(5000)
  h.stepWall(3600000)
  await fail(h, 1)
  expect(reports.length).toBe(0)
  h.stepWall(-7200000)
  h.advance(55000)
  await fail(h, 1)
  expect(reports.length).toBe(1)
})

test('BUSY switches session identity and reports at exactly sixty elapsed seconds', async () => {
  const h = harness()
  const reports = [] as string[][]
  let session = 'session-1'
  h.api.session.id = async () => session
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{}' } }
    return { exitCode: 75, stdout: '{"busy":true}' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 1)
  h.advance(60000)
  await fail(h, 1)
  expect(reports.length).toBe(1)
  session = 'session-2'
  await fail(h, 1)
  expect(reports.length).toBe(1)
  h.advance(59999)
  await fail(h, 1)
  expect(reports.length).toBe(1)
  h.advance(1)
  await fail(h, 1)
  expect(reports.length).toBe(2)
  expect(reports[1][reports[1].indexOf('--session-id') + 1]).toBe('session-2')
})

test('BUSY does not count toward the ordinary consecutive failure threshold', async () => {
  const h = harness()
  const reports = [] as string[][]
  let busy = true
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{}' } }
    return busy ? { exitCode: 75, stdout: '{"busy":true}' }
      : { exitCode: 1, stdout: '', stderr: 'notify bind failed' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 4)
  busy = false
  await fail(h, 2)
  expect(reports.length).toBe(0)
  await fail(h, 1)
  expect(reports.length).toBe(1)
  expect(reports[0].includes('--busy')).toBe(false)
  expect(reports[0].includes('Error: notify bind failed')).toBe(true)
})

test('an isolated failed poll, as after host sleep, stays silent', async () => {
  const h = harness()
  const reports = [] as string[][]
  const logs = [] as string[]
  let ok = false
  h.api.ui.log = (text: string) => { logs.push(text) }
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{"wake":false}' } }
    return ok ? { exitCode: 0, stdout: '{"wake":false}' } : { exitCode: 1, stdout: '', stderr: 'aborted: still running after 30000ms' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  for (let round = 0; round < 3; round++) {
    ok = false
    await fail(h, 2)
    ok = true
    await h.tick()
  }
  expect(reports.length).toBe(0)
  expect(logs.length).toBe(0)
})

test('repeated failed polls are reported every time and logged once', async () => {
  const h = harness()
  const reports = [] as string[][]
  const logs = [] as string[]
  let ok = false
  h.api.ui.log = (text: string) => { logs.push(text) }
  h.api.process.run = async (argv: string[]) => {
    if (argv.includes('--event')) { reports.push(argv); return { exitCode: 0, stdout: '{"wake":false}' } }
    return ok ? { exitCode: 0, stdout: '{"wake":false}' } : { exitCode: 1, stdout: '', stderr: 'notify bind failed' }
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 4)
  // hcom clears the error after one good cycle, so each repeat must set it again.
  expect(reports.length).toBe(2)
  expect(reports.every(r => r.includes('poll-failed'))).toBe(true)
  expect(logs.length).toBe(1)
  expect(logs[0].includes('notify bind failed')).toBe(true)
  ok = true
  await h.tick()
  ok = false
  await fail(h, 3)
  expect(reports.length).toBe(3)
  expect(logs.length).toBe(2)
})

test('hcom runs outside the session directory, which a Bash command may delete', async () => {
  const h = harness()
  const inits = [] as any[]
  h.api.process.run = async (argv: string[], init?: any) => {
    inits.push(init)
    if (argv.includes('--event')) return { exitCode: 0, stdout: '{}' }
    return { exitCode: 0, stdout: '{"wake":true}' }
  }
  h.api.prompt.submit = () => Promise.resolve(undefined) as any
  await h.handlers['session.start'](h.api, interactive, next)
  await h.tick()
  for (let i = 0; i < 20; i++) await Promise.resolve()
  expect(inits.length).toBe(2)
  expect(inits.every(init => init.cwd === '/')).toBe(true)
})

test('a poll that cannot record its failure still logs and backs off', async () => {
  const h = harness()
  const logs = [] as string[]
  let polls = 0
  h.api.ui.log = (text: string) => { logs.push(text) }
  h.api.process.run = async (argv: string[]) => {
    if (!argv.includes('--event')) polls++
    throw new Error('failed to start: ENOENT')
  }
  await h.handlers['session.start'](h.api, interactive, next)
  await fail(h, 3)
  expect(polls).toBe(3)
  expect(logs.length).toBe(2)
  expect(logs[0].includes('could not record poll-failed')).toBe(true)
  expect(logs[1].includes('poll failed; messages remain queued')).toBe(true)
})
