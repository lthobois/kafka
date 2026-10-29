import type { TicketEvent } from '../src/domain.ts';
export function eventKey(event: TicketEvent): string {
  // LAB-02 : choisir une clé stable pour regrouper les faits d'une vente.
  return event.event_id;
}
