import type { SubtractExpression, ValueReference } from '@/updates'
import { $set, $subtract } from '@/updates/update-symbols'

/**
 * Update function to subtract a number from an attribute.
 *
 * @param value - The number or reference to subtract.
 */
export function subtract(value: number | ValueReference<number>): SubtractExpression {
  return {
    type: $set,
    op: $subtract,
    value,
  }
}
