import type { OrderEvent } from '../src/domain.ts';
export function eventKey(event: OrderEvent): string {
  // LAB-02 : choisir une clé stable pour regrouper les faits d'une commande.
  return event.event_id;
}
