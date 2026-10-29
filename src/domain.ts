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

export interface TicketEvent {
  event_id: string;
  event_type: 'ticket.sold';
  schema_version: 1;
  occurred_at: string;
  correlation_id: string;
  data: { ticketId: string; eventId: string; orderId: string; insuranceSelected: boolean;
    quantity: number; currency: string; total_cents: number; channel?: string };
}
export class PermanentError extends Error {}
export class TransientError extends Error {}

export function validate(event: unknown, schemaPath = 'contracts/ticket-sold.v1.json'): asserts event is TicketEvent {
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
  db.exec('CREATE TABLE IF NOT EXISTS tickets_pdf (id INTEGER PRIMARY KEY, event_id TEXT, ticketId TEXT, quantity INTEGER)');
  return db;
}

export function apply(db: DatabaseSync, event: TicketEvent, mode: 'safe' | 'naive' = 'safe'): 'accepted' | 'duplicate' {
  // Empreinte sur les champs métier normalisés : les espaces et l'ordre JSON ne changent pas l'identité.
  const normalized = [event.event_type, event.schema_version, event.occurred_at, event.correlation_id,
    event.data.ticketId, event.data.eventId, event.data.orderId, event.data.insuranceSelected,
    event.data.quantity, event.data.currency, event.data.total_cents, event.data.channel ?? null];
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
    db.prepare('INSERT INTO tickets_pdf(event_id, ticketId, quantity) VALUES (?, ?, ?)')
      .run(event.event_id, event.data.ticketId, event.data.quantity);
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

export function makeEvent(index = 1, channel?: string): TicketEvent {
  return { event_id: `evt-${String(index).padStart(3, '0')}`, event_type: 'ticket.sold', schema_version: 1,
    occurred_at: '2026-01-15T09:00:00Z', correlation_id: `checkout-${index}`,
    data: { ticketId: `TKT-${String(index).padStart(3, '0')}`, eventId: 'CONCERT-2026',
      orderId: `ORDER-${index}`, insuranceSelected: index % 2 === 0,
      quantity: 1, currency: 'EUR', total_cents: 2500,
      ...(channel ? { channel } : {}) } };
}

export function checkCatalog(path: string): string[] {
  const topics = JSON.parse(readFileSync(path, 'utf8')) as Record<string, unknown>[];
  const errors: string[] = [], names = new Set<string>();
  for (const item of topics) {
    const name = String(item.name ?? '');
    if (names.has(name)) errors.push(`${name}: nom dupliqué`);
    names.add(name);
    if (!/^(dev|test|prod)\.ticketing\.ticket\.sold(\.dlq)?\.v1$/.test(name)) errors.push(`${name}: nom non conforme`);
    for (const field of ['owner', 'purpose', 'key', 'schema', 'consumers', 'classification', 'incident_contact']) {
      if (!item[field] || (Array.isArray(item[field]) && item[field].length === 0)) errors.push(`${name}: ${field} manquant`);
    }
    if (item.key !== 'ticketId') errors.push(`${name}: clé attendue ticketId`);
    if (!Number.isInteger(item.partitions) || Number(item.partitions) < 1) errors.push(`${name}: partitions invalides`);
    if (!Number.isInteger(item.retention_ms) || Number(item.retention_ms) < 86400000) errors.push(`${name}: rétention inférieure à un jour`);
    if (item.schema && !existsSync(resolve(ROOT, String(item.schema)))) errors.push(`${name}: contrat absent`);
  }
  return errors;
}
