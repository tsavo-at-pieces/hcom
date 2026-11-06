#!/usr/bin/env node
// Bounded contract check; external TeamRouter checkout is read only.
import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync } from 'node:crypto';
import { chmodSync, existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

const [binaryArg, verifierArg] = process.argv.slice(2);
assert.ok(binaryArg && verifierArg, 'usage: node scripts/check-receipt-interop.mjs HCOM_BINARY TEAMROUTER_VERIFIER_JS');
const binary = resolve(binaryArg);
const { verifyTranscriptReceipt, matchReceiptOccurrence } = await import(pathToFileURL(resolve(verifierArg)).href);
const root = mkdtempSync(join(tmpdir(), 'hcom-receipt-interop-'));
try {
  const { privateKey, publicKey } = generateKeyPairSync('ed25519');
  const keyPath = join(root, 'test-key.pem');
  writeFileSync(keyPath, privateKey.export({ format: 'pem', type: 'pkcs8' }), { mode: 0o600 });
  const sink = join(root, 'receipts');
  mkdirSync(sink, { mode: 0o700 });
  const home = join(root, 'home');
  mkdirSync(home, { mode: 0o700 });
  const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => !/^(HCOM_|CLAUDE|CODEX_|SUPERSET_|TEAMROUTER_)/.test(k)));
  Object.assign(env, {
    HOME: home, HCOM_DIR: join(root, 'hcom'), HCOM_PROVENANCE_DIR: sink,
    HCOM_PROVENANCE_KEY_FILE: keyPath, HCOM_PROVENANCE_ISSUER: 'hcom-interop-test',
  });
  const text = 'exact native text: é 😀\n';
  const payload = {
    submissionId: 'submission-interop', provider: 'codex', nativeSessionId: 'native-session-interop',
    agentId: 'main', messageId: 'native-message-interop', turnId: 'turn-interop',
    contentHash: createHash('sha256').update(text, 'utf8').digest('hex'), actor: 'agent', origin: 'hcom',
    state: 'matched', issuedAt: '2026-11-06T20:00:00.000Z',
  };
  const run = (value, extra = {}) => spawnSync(binary, ['receipt-sign'], {
    env: { ...env, ...extra }, input: JSON.stringify(value), encoding: 'utf8', timeout: 10_000,
  });
  const result = run(payload);
  assert.equal(result.status, 0, result.stderr || String(result.error));
  const envelope = JSON.parse(result.stdout);
  const issuers = [{
    id: 'hcom-interop-test', publicKey: publicKey.export({ format: 'pem', type: 'spki' }),
    allowedTypes: ['complete'], allowedActors: ['agent', 'system'], allowedOrigins: ['hcom', 'automation'],
  }];
  const verified = verifyTranscriptReceipt(envelope, { issuers });
  assert.equal(verified.valid, true, JSON.stringify(verified));
  const event = { ...payload, kind: 'input', text };
  assert.equal(matchReceiptOccurrence(verified.receipt, event), true);
  assert.equal(matchReceiptOccurrence(verified.receipt, { ...event, messageId: 'different-occurrence' }), false);
  assert.equal(matchReceiptOccurrence(verified.receipt, { ...event, text: text.trim() }), false);
  const tampered = structuredClone(envelope);
  tampered.payload.messageId = 'tampered';
  assert.equal(verifyTranscriptReceipt(tampered, { issuers }).valid, false);
  const lines = readFileSync(join(sink, 'receipts.jsonl'), 'utf8').trim().split('\n');
  assert.equal(lines.length, 1);
  assert.deepEqual(JSON.parse(lines[0]), envelope);
  const answer = { ...payload, submissionId: 'answer-interop', messageId: 'answer-message',
    actor: 'system', origin: 'automation', eventKind: 'tool_result', questionId: 'native-question', state: 'written' };
  const answerResult = run(answer);
  assert.equal(answerResult.status, 0, answerResult.stderr);
  const answerVerified = verifyTranscriptReceipt(JSON.parse(answerResult.stdout), { issuers });
  assert.equal(answerVerified.valid, true, JSON.stringify(answerVerified));
  assert.equal(matchReceiptOccurrence(answerVerified.receipt, { ...answer, kind: 'tool_result', text }), true);
  assert.equal(matchReceiptOccurrence(answerVerified.receipt, { ...answer, kind: 'tool_result', questionId: 'another-question', text }), false);
  for (const bad of [{ ...payload, actor: 'owner' }, { ...payload, messageId: '' }, { ...payload, from: 'bigboss' }]) {
    const rejected = run(bad);
    assert.notEqual(rejected.status, 0);
    assert.equal(rejected.stdout, '');
  }
  assert.notEqual(run({ ...answer, questionId: '' }).status, 0);
  assert.notEqual(run(payload, { HCOM_PROVENANCE_DIR: '' }).status, 0);
  chmodSync(keyPath, 0o644);
  assert.notEqual(run(payload).status, 0);
  assert.equal(existsSync(join(root, 'hcom', 'hcom.db')), false, 'signer opened live messaging DB');
  console.log('receipt interoperability: signature, exact occurrence, persistence, rejection, and private-key checks passed');
} finally {
  rmSync(root, { recursive: true, force: true });
}
