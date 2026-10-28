import { test } from 'node:test';
import assert from 'node:assert/strict';
import { eventKey } from '../exercises/routing.ts';
import { makeEvent, PermanentError, validate } from '../src/domain.ts';
const schema = 'exercises/order-created.v1.json';
test('clé = order_id', () => assert.equal(eventKey(makeEvent()), 'ORD-001'));
test('ancien événement et ajout facultatif', () => { validate(makeEvent(), schema); validate(makeEvent(1, 'mobile'), schema); });
test('champs métier obligatoires', () => {
  for (const field of ['quantity', 'currency', 'total_cents']) {
    const event: any = makeEvent(); delete event.data[field];
    assert.throws(() => validate(event, schema), PermanentError, field);
  }
});
test('valeurs métier contrôlées', () => {
  for (const [field, value] of [['quantity', 0], ['quantity', '1'], ['currency', '???'], ['total_cents', -1]]) {
    const event: any = makeEvent(); event.data[String(field)] = value;
    assert.throws(() => validate(event, schema), PermanentError, String(field));
  }
});
