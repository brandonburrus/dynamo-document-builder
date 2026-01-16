import type { AddExpression, ValueReference } from '@/updates'
import { $add, $set } from '@/updates/update-symbols'

/**
 * Update function to add a number to an attribute.
 *
 * @param value - The number or reference to add.
 */
export function add(value: number | ValueReference<number>): AddExpression {
  return {
    type: $set,
    op: $add,
    value,
  }
}
