import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { PrependExpression, ValueReference } from '@/updates'
import { $set, $prepend } from '@/updates/update-symbols'

/**
 * Update function to prepend values to a list attribute.
 *
 * @param values - The values or reference to prepend.
 */
export function prepend(values: NativeAttributeValue[] | ValueReference): PrependExpression {
  return {
    type: $set,
    op: $prepend,
    values,
  }
}
