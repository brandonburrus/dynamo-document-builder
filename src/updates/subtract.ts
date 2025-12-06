import type { SubtractExpression, ValueReference } from '@/updates/update-types'
import { $set, $subtract } from '@/updates/update-symbols'

export function subtract(value: number | ValueReference<number>): SubtractExpression {
  return {
    type: $set,
    op: $subtract,
    value,
  }
}
