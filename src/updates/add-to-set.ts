import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { AddToSetExpression, ValueReference } from '@/updates/update-types'
import { $add, $addToSet } from '@/updates/update-symbols'

export function addToSet(values: NativeAttributeValue[] | ValueReference): AddToSetExpression {
  return {
    type: $add,
    op: $addToSet,
    values,
  }
}
