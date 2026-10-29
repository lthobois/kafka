import { apply, openDb, PermanentError, TransientError, validate } from './domain.ts';
import { commit, consume, producer, send, sleep } from './bus.ts';

export async function queueWorker(options: { topic: string; retryTopic: string; dlqTopic: string;
  group: string; db: string; count: number; timeout?: number }) {
  const db = openDb(options.db), p = await producer();
  const metrics = { records: 0, accepted: 0, duplicate: 0, retry_queued: 0, dlq: 0 };
  try {
    await consume(options, async (payload, c) => {
      let event: unknown = null;
      const attempt = Number(payload.message.headers?.attempt?.toString() ?? 1);
      const failures = Number(payload.message.headers?.['lab-failures']?.toString() ?? 0);
      if (!Number.isInteger(attempt) || attempt < 1) throw new Error('Header attempt invalide');
      // Délai court uniquement pédagogique : un topic Kafka n'est pas un ordonnanceur différé.
      if (attempt > 1) await sleep(200);
      try {
        try { event = JSON.parse(payload.message.value?.toString() ?? 'null'); }
        catch { throw new PermanentError('JSON invalide'); }
        validate(event);
        if (attempt <= failures) throw new TransientError('Génération PDF indisponible (simulation)');
        metrics[apply(db, event)]++;
      } catch (error) {
        if (!(error instanceof PermanentError || error instanceof TransientError)) throw error;
        if (error instanceof TransientError && attempt < 3) {
          await send(p, options.retryTopic, payload.message.key, event,
            { attempt: String(attempt + 1), 'lab-failures': String(failures) });
          metrics.retry_queued++;
        } else {
          await send(p, options.dlqTopic, payload.message.key, {
            source_topic: payload.topic, source_partition: payload.partition, source_offset: payload.message.offset,
            error_type: error.constructor.name, error: error.message, attempts: attempt,
            original: event, raw: payload.message.value?.toString() ?? '' });
          metrics.dlq++;
        }
      }
      // La sortie est confirmée AVANT le commit. Un incident entre les deux peut dupliquer la sortie.
      await commit(c, payload);
      metrics.records++;
      console.log(JSON.stringify(metrics));
    });
  } finally { db.close(); await p.disconnect(); }
  if (metrics.records !== options.count) throw new Error(`Délai atteint : ${metrics.records}/${options.count}`);
  console.log('QUEUE METRICS ' + JSON.stringify(metrics));
  return metrics;
}
