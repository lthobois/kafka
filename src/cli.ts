import { parseArgs } from 'node:util';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { checkCatalog, makeEvent, ROOT, validate } from './domain.ts';
import { shutdownTelemetry } from './telemetry.ts';

const { values: flags, positionals } = parseArgs({ allowPositionals: true, options: {
  topic: { type: 'string' }, group: { type: 'string' }, count: { type: 'string' }, timeout: { type: 'string' },
  delay: { type: 'string' }, db: { type: 'string' }, mode: { type: 'string' }, isolation: { type: 'string' },
  'dlq-topic': { type: 'string' }, 'retry-topic': { type: 'string' }, 'solution': { type: 'boolean' },
  'no-commit': { type: 'boolean' }, 'crash-after-effect': { type: 'boolean' }
} });

async function main(): Promise<void> {
  const [command, argument] = positionals;
  if (command === 'test' || command === 'exercise-tests' || command === 'check') {
    const args = command === 'check' ? [resolve(ROOT, 'node_modules/typescript/bin/tsc'), '--noEmit']
      : ['--test', resolve(ROOT, command === 'test' ? 'tests/domain.test.ts' : 'tests/exercise.test.ts')];
    const result = spawnSync(process.execPath, args, { cwd: ROOT, stdio: 'inherit' });
    process.exitCode = result.status ?? 1;
    if (result.status === 0) console.log(command === 'check' ? 'CHECK OK' : 'TESTS OK');
    return;
  }
  if (command === 'catalog') {
    const errors = checkCatalog(resolve(ROOT, argument ?? 'exercises/catalog.json'));
    console.log(errors.length ? errors.join('\n') : 'CATALOG OK');
    process.exitCode = errors.length ? 1 : 0;
    return;
  }
  if (command === 'status') {
    const db = new DatabaseSync(flags.db ?? '/state/shipping.db', { readOnly: true });
    try {
      console.log('shipments=' + db.prepare('SELECT COUNT(*) AS n FROM shipments').get()!.n);
      console.table(db.prepare('SELECT event_id, order_id, quantity FROM shipments ORDER BY id').all());
    } finally { db.close(); }
    return;
  }
  if (command === 'integration') { await (await import('./integration.ts')).integration(); return; }
  if (command === 'transactions') { await (await import('./experiments.ts')).transactions(); return; }
  if (command === 'supervise') {
    await (await import('./supervision.ts')).supervise(flags.topic ?? 'dev.shop.order-created.v1', flags.group ?? 'shipping-v1'); return;
  }
  if (command === 'queue-worker') {
    await (await import('./queues.ts')).queueWorker({ topic: flags.topic ?? 'dev.lab.queues.v1',
      retryTopic: flags['retry-topic'] ?? 'dev.lab.queues.retry.v1', dlqTopic: flags['dlq-topic'] ?? 'dev.lab.queues.dlq.v1',
      group: flags.group ?? 'queues-source-v1', db: flags.db ?? '/state/queues.db',
      count: Number(flags.count ?? 5), timeout: Number(flags.timeout ?? 40) }); return;
  }
  const { initTopics, MAIN, producer, readRecords, send } = await import('./bus.ts');
  if (command === 'init') { await initTopics(); return; }
  if (command === 'read') {
    await readRecords({ topic: flags.topic ?? 'legacy-events', group: flags.group ?? 'audit-v1',
      count: Number(flags.count ?? 4), timeout: Number(flags.timeout ?? 15), delay: Number(flags.delay ?? 0),
      commit: !flags['no-commit'], readCommitted: flags.isolation === 'read_committed' });
    return;
  }
  if (command === 'worker') {
    if (flags.mode && !['safe', 'naive'].includes(flags.mode)) throw new Error('mode attendu : safe ou naive');
    await (await import('./worker.ts')).runWorker({ group: flags.group ?? 'shipping-v1', db: flags.db ?? '/state/shipping.db',
      count: Number(flags.count ?? 5), mode: flags.mode as 'safe' | 'naive' | undefined,
      timeout: Number(flags.timeout ?? 40), topic: flags.topic, dlqTopic: flags['dlq-topic'],
      crashAfterEffect: flags['crash-after-effect'] });
    return;
  }
  if (command !== 'seed') throw new Error('Commandes : init, seed, read, worker, status, catalog, test, exercise-tests, check, integration, transactions');
  const p = await producer();
  try {
    if (argument === 'legacy') {
      for (const event of JSON.parse(readFileSync(resolve(ROOT, 'data/legacy.json'), 'utf8')))
        await send(p, 'legacy-events', null, event);
    } else if (argument === 'groups') {
      await p.send({ topic: 'dev.lab.groups.v1', messages: Array.from({ length: 30 }, (_, i) =>
        ({ partition: i % 3, key: `ORD-${i % 6}`, value: JSON.stringify({ index: i }) })) });
    } else if (argument === 'orders') {
      const { eventKey } = flags.solution ? await import('../solutions/routing.ts') : await import('../exercises/routing.ts');
      for (let i = 1; i <= 3; i++) {
        const event = makeEvent(i); validate(event, flags.solution ? 'contracts/order-created.v1.json' : 'exercises/order-created.v1.json');
        await send(p, flags.topic ?? MAIN, eventKey(event), event);
      }
    } else if (argument === 'resilience') {
      const events = [[makeEvent(1), 0], [makeEvent(1), 0], [makeEvent(2), 2], [makeEvent(3), 0], [makeEvent(4), 9]] as const;
      events[3][0].data.quantity = -1;
      for (const [event, failures] of events)
        await send(p, flags.topic ?? MAIN, event.data.order_id, event, { 'lab-failures': String(failures) });
    } else if (argument === 'crash') {
      await send(p, flags.topic ?? MAIN, 'ORD-099', makeEvent(99));
    } else throw new Error('Jeux : legacy, groups, orders, resilience, crash');
    console.log(`SEEDED ${argument}`);
  } finally { await p.disconnect(); }
}

main().catch(error => { console.error(error); process.exitCode = 1; }).finally(shutdownTelemetry);
