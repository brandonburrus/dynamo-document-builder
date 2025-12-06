import type { AddExpression, ValueReference } from '@/updates/update-types'
import { $add, $set } from '@/updates/update-symbols'

export function add(value: number | ValueReference<number>): AddExpression {
  return {
    type: $set,
    op: $add,
    value,
  }
}
