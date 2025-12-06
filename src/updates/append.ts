import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { AppendExpression, ValueReference } from '@/updates/update-types'
import { $set, $append } from '@/updates/update-symbols'

export function append(values: NativeAttributeValue[] | ValueReference): AppendExpression {
  return {
    type: $set,
    op: $append,
    values,
  }
}
