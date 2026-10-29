import type { TicketEvent } from '../src/domain.ts';
export function eventKey(event: TicketEvent): string {
  return event.data.ticketId;
}
