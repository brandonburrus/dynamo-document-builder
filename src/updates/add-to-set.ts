import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { AddToSetExpression, ValueReference } from '@/updates'
import { $add, $addToSet } from '@/updates/update-symbols'

/**
 * Update function to add values to a set attribute.
 *
 * @param values - The values or reference to add to the set.
 */
export function addToSet(values: NativeAttributeValue[] | ValueReference): AddToSetExpression {
  return {
    type: $add,
    op: $addToSet,
    values,
  }
}
