import { apply, openDb, PermanentError, TransientError, validate, withRetry } from './domain.ts';
import { commit, consume, DLQ, MAIN, producer, send, sleep } from './bus.ts';

export interface WorkerOptions {
  group: string; db: string; count: number; mode?: 'safe' | 'naive'; timeout?: number;
  crashAfterEffect?: boolean; topic?: string; dlqTopic?: string;
}
export async function runWorker(options: WorkerOptions) {
  const db = openDb(options.db), p = await producer();
  const metrics = { records: 0, accepted: 0, duplicate: 0, retry: 0, dlq: 0 };
  try {
    await consume({ ...options, topic: options.topic ?? MAIN }, async (payload, c) => {
      let attempts = 0, event: unknown = null;
      try {
        try { event = JSON.parse(payload.message.value?.toString() ?? 'null'); }
        catch { throw new PermanentError('JSON invalide'); }
        validate(event);
        const order = event;
        const status = await withRetry(async attempt => {
          attempts = attempt;
          if (attempt > 1) metrics.retry++;
          // Injection de panne de laboratoire ; ce header ne fait pas partie du contrat métier.
          const failures = Number(payload.message.headers?.['lab-failures']?.toString() ?? 0);
          if (attempt <= failures) throw new TransientError('Génération PDF temporairement indisponible (simulation)');
          return apply(db, order, options.mode);
        }, sleep);
        metrics[status]++;
      } catch (error) {
        if (!(error instanceof PermanentError || error instanceof TransientError)) throw error;
        const envelope = { source_topic: payload.topic, source_partition: payload.partition,
          source_offset: payload.message.offset, error_type: error.constructor.name, error: error.message,
          attempts, original: event, raw: payload.message.value?.toString() ?? '' };
        await send(p, options.dlqTopic ?? DLQ, payload.message.key, envelope);
        metrics.dlq++;
      }
      if (options.crashAfterEffect && metrics.accepted > 0) throw new Error('Incident simulé après effet SQLite et avant commit Kafka');
      await commit(c, payload);
      metrics.records++;
      console.log(JSON.stringify(metrics));
    });
  } finally { db.close(); await p.disconnect(); }
  console.log('METRICS ' + JSON.stringify(metrics));
  if (metrics.records !== options.count) throw new Error(`Délai atteint : ${metrics.records}/${options.count} records`);
  return metrics;
}
