import { createHash } from 'node:crypto';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createRequire } from 'node:module';
import type { ValidateFunction } from 'ajv';

const require = createRequire(import.meta.url);
const Ajv2020 = require('ajv/dist/2020').default;
const addFormats = require('ajv-formats');
const ajv = new Ajv2020({ allErrors: true });
addFormats(ajv);
export const ROOT = fileURLToPath(new URL('../', import.meta.url));
const validators = new Map<string, ValidateFunction>();

export interface OrderEvent {
  event_id: string;
  event_type: 'OrderCreated';
  schema_version: 1;
  occurred_at: string;
  correlation_id: string;
  data: { order_id: string; quantity: number; currency: string; total_cents: number; channel?: string };
}
export class PermanentError extends Error {}
export class TransientError extends Error {}

export function validate(event: unknown, schemaPath = 'contracts/order-created.v1.json'): asserts event is OrderEvent {
  let validator = validators.get(schemaPath);
  if (!validator) {
    validator = ajv.compile(JSON.parse(readFileSync(resolve(ROOT, schemaPath), 'utf8'))) as ValidateFunction;
    validators.set(schemaPath, validator);
  }
  if (!validator(event)) throw new PermanentError(ajv.errorsText(validator.errors));
}

export function openDb(path: string): DatabaseSync {
  const db = new DatabaseSync(path);
  db.exec('CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY, digest TEXT NOT NULL)');
  db.exec('CREATE TABLE IF NOT EXISTS shipments (id INTEGER PRIMARY KEY, event_id TEXT, order_id TEXT, quantity INTEGER)');
  return db;
}

export function apply(db: DatabaseSync, event: OrderEvent, mode: 'safe' | 'naive' = 'safe'): 'accepted' | 'duplicate' {
  // Empreinte sur les champs métier normalisés : les espaces et l'ordre JSON ne changent pas l'identité.
  const normalized = [event.event_type, event.schema_version, event.occurred_at, event.correlation_id,
    event.data.order_id, event.data.quantity, event.data.currency, event.data.total_cents, event.data.channel ?? null];
  const digest = createHash('sha256').update(JSON.stringify(normalized)).digest('hex');
  db.exec('BEGIN IMMEDIATE');
  try {
    if (mode === 'safe') {
      const row = db.prepare('SELECT digest FROM seen WHERE event_id=?').get(event.event_id);
      if (row) {
        if (row.digest !== digest) throw new PermanentError('event_id réutilisé avec un contenu métier différent');
        db.exec('COMMIT');
        return 'duplicate';
      }
      db.prepare('INSERT INTO seen VALUES (?, ?)').run(event.event_id, digest);
    }
    db.prepare('INSERT INTO shipments(event_id, order_id, quantity) VALUES (?, ?, ?)')
      .run(event.event_id, event.data.order_id, event.data.quantity);
    db.exec('COMMIT');
    return 'accepted';
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

export async function withRetry<T>(operation: (attempt: number) => Promise<T>,
  sleep: (ms: number) => Promise<void> = async () => {}, maxAttempts = 3): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try { return await operation(attempt); }
    catch (error) {
      if (!(error instanceof TransientError) || attempt >= maxAttempts) throw error;
      await sleep(200 * attempt);
    }
  }
}

export function makeEvent(index = 1, channel?: string): OrderEvent {
  return { event_id: `evt-${String(index).padStart(3, '0')}`, event_type: 'OrderCreated', schema_version: 1,
    occurred_at: '2026-01-15T09:00:00Z', correlation_id: `checkout-${index}`,
    data: { order_id: `ORD-${String(index).padStart(3, '0')}`, quantity: 1, currency: 'EUR', total_cents: 2500,
      ...(channel ? { channel } : {}) } };
}

export function checkCatalog(path: string): string[] {
  const topics = JSON.parse(readFileSync(path, 'utf8')) as Record<string, unknown>[];
  const errors: string[] = [], names = new Set<string>();
  for (const item of topics) {
    const name = String(item.name ?? '');
    if (names.has(name)) errors.push(`${name}: nom dupliqué`);
    names.add(name);
    if (!/^(dev|test|prod)\.shop\.order-created(\.dlq)?\.v1$/.test(name)) errors.push(`${name}: nom non conforme`);
    for (const field of ['owner', 'purpose', 'key', 'schema', 'consumers', 'classification', 'incident_contact']) {
      if (!item[field] || (Array.isArray(item[field]) && item[field].length === 0)) errors.push(`${name}: ${field} manquant`);
    }
    if (item.key !== 'order_id') errors.push(`${name}: clé attendue order_id`);
    if (!Number.isInteger(item.partitions) || Number(item.partitions) < 1) errors.push(`${name}: partitions invalides`);
    if (!Number.isInteger(item.retention_ms) || Number(item.retention_ms) < 86400000) errors.push(`${name}: rétention inférieure à un jour`);
    if (item.schema && !existsSync(resolve(ROOT, String(item.schema)))) errors.push(`${name}: contrat absent`);
  }
  return errors;
}
