import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { AppendExpression, ValueReference } from '@/updates'
import { $set, $append } from '@/updates/update-symbols'

/**
 * Update function to append values to a list attribute.
 *
 * @param values - The values or reference to append.
 */
export function append(values: NativeAttributeValue[] | ValueReference): AppendExpression {
  return {
    type: $set,
    op: $append,
    values,
  }
}
