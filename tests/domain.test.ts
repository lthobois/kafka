import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply, checkCatalog, makeEvent, openDb, PermanentError, ROOT, TransientError, validate, withRetry } from '../src/domain.ts';

test('contrat : ancien événement et ajout facultatif', () => {
  validate(makeEvent()); validate(makeEvent(1, 'web'));
});
test('contrat : valeurs métier et date invalides rejetées', () => {
  for (const change of [(e: any) => delete e.data.currency, (e: any) => e.data.quantity = -1,
    (e: any) => e.occurred_at = 'hier']) {
    const event = makeEvent(); change(event); assert.throws(() => validate(event), PermanentError);
  }
});
test('catalogue corrigé conforme', () => assert.deepEqual(checkCatalog(join(ROOT, 'solutions/catalog.json')), []));
test('déduplication persistée et collision de contenu détectée', () => {
  const directory = mkdtempSync(join(tmpdir(), 'kafka-test-')), path = join(directory, 'state.db');
  let db = openDb(path);
  try {
    assert.equal(apply(db, makeEvent()), 'accepted'); db.close(); db = openDb(path);
    assert.equal(apply(db, makeEvent()), 'duplicate');
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM shipments').get()!.n, 1);
    const changed = makeEvent(); changed.data.quantity = 3;
    assert.throws(() => apply(db, changed), PermanentError);
  } finally { db.close(); rmSync(directory, { recursive: true }); }
});
test('mode naïf : deux effets pour le même fait', () => {
  const db = openDb(':memory:');
  try { apply(db, makeEvent(), 'naive'); apply(db, makeEvent(), 'naive');
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM shipments').get()!.n, 2);
  } finally { db.close(); }
});
test('échec de stockage : le marqueur seen est annulé avec l’effet', () => {
  const db = openDb(':memory:');
  try {
    db.exec("CREATE TRIGGER fail BEFORE INSERT ON shipments BEGIN SELECT RAISE(ABORT, 'failure'); END");
    assert.throws(() => apply(db, makeEvent()));
    assert.equal(db.prepare('SELECT COUNT(*) AS n FROM seen').get()!.n, 0);
  } finally { db.close(); }
});
test('erreur transitoire résolue à la troisième tentative', async () => {
  const calls: number[] = [];
  assert.equal(await withRetry(async attempt => { calls.push(attempt); if (attempt < 3) throw new TransientError(); return 'ok'; }), 'ok');
  assert.deepEqual(calls, [1, 2, 3]);
});
test('erreur permanente : une seule tentative', async () => {
  const calls: number[] = [];
  await assert.rejects(withRetry(async attempt => { calls.push(attempt); throw new PermanentError(); }), PermanentError);
  assert.deepEqual(calls, [1]);
});
test('erreur transitoire persistante : trois tentatives maximum', async () => {
  const calls: number[] = [];
  await assert.rejects(withRetry(async attempt => { calls.push(attempt); throw new TransientError(); }), TransientError);
  assert.deepEqual(calls, [1, 2, 3]);
});
