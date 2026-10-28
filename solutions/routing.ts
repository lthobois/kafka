import type { OrderEvent } from '../src/domain.ts';
export function eventKey(event: OrderEvent): string {
  return event.data.order_id;
}
