import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { DeleteExpression, ValueReference } from '@/updates/update-types'
import { $delete } from '@/updates/update-symbols'

export function removeFromSet(values: NativeAttributeValue[] | ValueReference): DeleteExpression {
  return {
    type: $delete,
    values,
  }
}
