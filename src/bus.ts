import { KafkaJS } from '@confluentinc/kafka-javascript';
import type { KafkaJS as K } from '@confluentinc/kafka-javascript';
import { traced, traceHeaders } from './telemetry.ts';

export type Consumer = K.Consumer;
export type EachMessagePayload = K.EachMessagePayload;
export type Producer = K.Producer;

export const MAIN = 'dev.shop.order-created.v1';
export const DLQ = 'dev.shop.order-created.dlq.v1';
// API compatible KafkaJS : les options KafkaJS vont dans le bloc kafkaJS, les propriétés librdkafka à côté.
function connection() {
  if (process.env.KAFKA_PROFILE !== 'aiven') return { kafkaJS: {}, native: {} };
  const required = (key: string) => { const value = process.env[key]; if (!value) throw new Error(`${key} requis pour Aiven`); return value; };
  const ca = { 'ssl.ca.location': required('KAFKA_CA_FILE') };
  if (process.env.KAFKA_AUTH === 'mtls') return { kafkaJS: { ssl: true }, native: { ...ca,
    'ssl.certificate.location': required('KAFKA_CERT_FILE'), 'ssl.key.location': required('KAFKA_KEY_FILE') } };
  return { kafkaJS: { ssl: true, sasl: { mechanism: 'scram-sha-256' as const,
    username: required('KAFKA_USERNAME'), password: required('KAFKA_PASSWORD') } }, native: ca };
}
const { kafkaJS: secured, native } = connection();
export const kafka = new KafkaJS.Kafka({ ...native, kafkaJS: { clientId: 'formation',
  brokers: (process.env.BOOTSTRAP_SERVERS ?? 'kafka:9092').split(','), ...secured,
  logLevel: KafkaJS.logLevel.ERROR, retry: { retries: 5 } } });
export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

export async function producer(transactionalId?: string): Promise<Producer> {
  // acks se règle sur le producer, pas à chaque send. Partitionnement par défaut : murmur2 sur la clé, comme Java.
  // transactionalId n'est transmis que s'il existe : sa seule présence rend le producer transactionnel.
  const p = kafka.producer({ kafkaJS: { idempotent: true, maxInFlightRequests: 1, acks: -1,
    ...(transactionalId ? { transactionalId } : {}) } });
  await p.connect();
  return p;
}

export async function send(p: Pick<Producer, 'send'>, topic: string, key: string | Buffer | null,
  value: unknown, headers?: K.IHeaders): Promise<void> {
  // send rejette en cas d'erreur : l'appelant ne doit pas committer l'entrée dans ce cas.
  await traced('kafka.publish', topic, async () => {
    await p.send({ topic, messages: [{ key, value: JSON.stringify(value), headers: { ...headers, ...traceHeaders() } }] });
  });
}

export async function createTopics(topics: K.ITopicConfig[]): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try { await admin.createTopics({ topics }); }
  finally { await admin.disconnect(); }
}

export async function initTopics(): Promise<void> {
  const topics: [string, number][] = [['legacy-events', 1], ['dev.lab.groups.v1', 3], [MAIN, 3], [DLQ, 3],
    ['dev.lab.resilience.v1', 3], ['dev.lab.resilience.dlq.v1', 3], ['dev.lab.crash.v1', 1],
    ['dev.lab.queues.v1', 3], ['dev.lab.queues.retry.v1', 3], ['dev.lab.queues.dlq.v1', 3]];
  await createTopics(topics.map(([topic, numPartitions]) => ({ topic, numPartitions,
    replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] })));
  console.log('TOPICS OK', topics.map(([name]) => name));
}

export interface ConsumeOptions {
  topic: string; group: string; count: number; timeout?: number; readCommitted?: boolean;
}

export async function consume(options: ConsumeOptions,
  handle: (payload: EachMessagePayload, consumer: Consumer) => Promise<void>): Promise<number> {
  // fromBeginning et autoCommit se règlent sur le consumer ; les heartbeats sont gérés par librdkafka.
  const c = kafka.consumer({ kafkaJS: { groupId: options.group, readUncommitted: !options.readCommitted,
    fromBeginning: true, autoCommit: false, sessionTimeout: 10000, heartbeatInterval: 2000 } });
  let done = false, processed = 0, fatal: unknown;
  let finish!: () => void;
  const completion = new Promise<void>(resolve => { finish = resolve; });
  const timer = setTimeout(() => { done = true; finish(); }, (options.timeout ?? 40) * 1000);
  try {
    await c.connect();
    await c.subscribe({ topics: [options.topic] });
    await c.run({ partitionsConsumedConcurrently: 1, eachMessage: async payload => {
      if (done) return;
      try {
        const carrier = Object.fromEntries(Object.entries(payload.message.headers ?? {}).map(([k, v]) => [k, v?.toString() ?? '']));
        await traced('kafka.process', payload.topic, () => handle(payload, c), carrier);
        processed++;
        if (processed >= options.count) { done = true; finish(); }
      } catch (error) { fatal = error; done = true; finish(); }
    } });
    await completion;
    if (fatal) throw fatal;
    return processed;
  } finally { clearTimeout(timer); await c.disconnect(); }
}

export async function commit(c: Consumer, payload: EachMessagePayload): Promise<void> {
  await c.commitOffsets([{ topic: payload.topic, partition: payload.partition,
    offset: (BigInt(payload.message.offset) + 1n).toString() }]);
}

export async function readRecords(options: ConsumeOptions & { commit?: boolean; delay?: number }) {
  const records: { partition: number; offset: string; key: string | null; value: unknown }[] = [];
  await consume(options, async (payload, c) => {
    const record = { partition: payload.partition, offset: payload.message.offset,
      key: payload.message.key?.toString() ?? null, value: JSON.parse(payload.message.value!.toString()) as unknown };
    records.push(record);
    console.log(JSON.stringify(record));
    if (options.commit !== false) await commit(c, payload);
    if (options.delay) await sleep(options.delay * 1000);
  });
  console.log(`READ records=${records.length}`);
  return records;
}
