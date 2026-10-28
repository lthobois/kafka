import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createTopics, producer, readRecords, send } from './bus.ts';

export async function transactions(): Promise<void> {
  const id = randomUUID().slice(0, 8), topic = `dev.lab.transactions-${id}.v1`;
  await createTopics([{ topic, numPartitions: 1, replicationFactor: 1 }]);
  const p = await producer(`tx-${id}`);
  try {
    let tx = await p.transaction();
    await send(tx, topic, 'ORD-X', { status: 'aborted' }); await tx.abort();
    tx = await p.transaction();
    await send(tx, topic, 'ORD-Y', { status: 'committed' }); await tx.commit();
    const all = await readRecords({ topic, group: `all-${id}`, count: 2, timeout: 30 });
    const visible = await readRecords({ topic, group: `committed-${id}`, count: 2, timeout: 10, readCommitted: true });
    assert.equal(all.length, 2);
    assert.deepEqual(visible.map(r => (r.value as { status: string }).status), ['committed']);
    console.log('TRANSACTIONS OK : read_uncommitted=2, read_committed=1');
  } finally { await p.disconnect(); }
}
