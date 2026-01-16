import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { DeleteExpression, ValueReference } from '@/updates'
import { $delete } from '@/updates/update-symbols'

/**
 * Update function to remove values from a set attribute.
 *
 * @param values - The values or reference to remove.
 */
export function removeFromSet(values: NativeAttributeValue[] | ValueReference): DeleteExpression {
  return {
    type: $delete,
    values,
  }
}
