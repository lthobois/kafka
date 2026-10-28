import { kafka } from './bus.ts';

export async function supervise(topic: string, group: string): Promise<void> {
  const admin = kafka.admin(); await admin.connect();
  try {
    const ends = await admin.fetchTopicOffsets(topic);
    const committed = await admin.fetchOffsets({ groupId: group, topics: [topic] });
    const positions = committed.find(t => t.topic === topic)?.partitions ?? [];
    console.table(ends.map(end => {
      const offset = positions.find(p => p.partition === end.partition)?.offset ?? '-1';
      return { topic, partition: end.partition, retained_from: end.low, log_end: end.high,
        committed_next: offset, lag: offset === '-1' ? 'offset absent' : String(BigInt(end.high) - BigInt(offset)) };
    }));
  } finally { await admin.disconnect(); }
}
