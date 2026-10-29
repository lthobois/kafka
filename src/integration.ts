import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createTopics, producer, readRecords, send } from './bus.ts';
import { makeEvent, openDb } from './domain.ts';
import { runWorker } from './worker.ts';
import { queueWorker } from './queues.ts';

export async function integration(): Promise<void> {
  const id = randomUUID().slice(0, 8), topic = `dev.lab.integration-${id}.v1`, dlq = `${topic}.dlq`, crashTopic = `${topic}.crash`;
  await createTopics([topic, dlq, crashTopic].map(topic => ({ topic, numPartitions: 3, replicationFactor: 1 })));
  const p = await producer();
  const directory = mkdtempSync(join(tmpdir(), 'kafka-integration-'));
  try {
    const events = [[makeEvent(1), 0], [makeEvent(1), 0], [makeEvent(2), 2], [makeEvent(3), 0], [makeEvent(4), 9]] as const;
    events[3][0].data.quantity = -1;
    for (const [event, failures] of events) await send(p, topic, event.data.ticketId, event, { 'lab-failures': String(failures) });
    const db = join(directory, 'safe.db');
    const metrics = await runWorker({ topic, dlqTopic: dlq, group: `safe-${id}`, db, count: 5 });
    assert.deepEqual(metrics, { records: 5, accepted: 2, duplicate: 1, retry: 4, dlq: 2 });
    const state = openDb(db);
    try { assert.equal(state.prepare('SELECT COUNT(*) AS n FROM tickets_pdf').get()!.n, 2); }
    finally { state.close(); }
    const quarantine = await readRecords({ topic: dlq, group: `dlq-${id}`, count: 2 });
    assert.equal(quarantine.length, 2);
    assert.deepEqual(quarantine.map(r => (r.value as { error_type: string }).error_type).sort(), ['PermanentError', 'TransientError']);
    const replay = await runWorker({ topic, dlqTopic: dlq, group: `replay-${id}`, db, count: 5 });
    assert.equal(replay.accepted, 0); assert.equal(replay.duplicate, 3);
    const naive = await runWorker({ topic, dlqTopic: dlq, group: `naive-${id}`, db: join(directory, 'naive.db'), count: 5, mode: 'naive' });
    assert.equal(naive.accepted, 3); assert.equal(naive.duplicate, 0);
    await send(p, crashTopic, 'TKT-099', makeEvent(99));
    const options = { topic: crashTopic, dlqTopic: dlq, group: `crash-${id}`, db: join(directory, 'crash.db'), count: 1 };
    await assert.rejects(runWorker({ ...options, crashAfterEffect: true }), /Incident simulé/);
    const resumed = await runWorker(options);
    assert.equal(resumed.accepted, 0); assert.equal(resumed.duplicate, 1);
    const qTopic = `${topic}.queues`, qRetry = `${qTopic}.retry`, qDlq = `${qTopic}.dlq`;
    await createTopics([qTopic, qRetry, qDlq].map(topic => ({ topic, numPartitions: 3, replicationFactor: 1 })));
    for (const [event, failures] of events) await send(p, qTopic, event.data.ticketId, event, { 'lab-failures': String(failures) });
    const qOptions = { retryTopic: qRetry, dlqTopic: qDlq, db: join(directory, 'queues.db') };
    const initial = await queueWorker({ ...qOptions, topic: qTopic, group: `qs-${id}`, count: 5 });
    assert.deepEqual(initial, { records: 5, accepted: 1, duplicate: 1, retry_queued: 2, dlq: 1 });
    const retried = await queueWorker({ ...qOptions, topic: qRetry, group: `qr-${id}`, count: 4 });
    assert.deepEqual(retried, { records: 4, accepted: 1, duplicate: 0, retry_queued: 2, dlq: 1 });
    console.log('INTEGRATION OK : contrat, livraison, retry, DLQ, rejeu, incident avant commit');
  } finally { await p.disconnect(); rmSync(directory, { recursive: true }); }
}
