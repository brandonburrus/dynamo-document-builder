import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { PrependExpression, ValueReference } from '@/updates/update-types'
import { $set, $prepend } from '@/updates/update-symbols'

export function prepend(values: NativeAttributeValue[] | ValueReference): PrependExpression {
  return {
    type: $set,
    op: $prepend,
    values,
  }
}
