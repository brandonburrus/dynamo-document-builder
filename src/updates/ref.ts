import type { ValueReference } from '@/updates/update-types'
import { $ref } from '@/updates/update-symbols'
import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'

export function ref(attributePath: string, defaultTo?: NativeAttributeValue): ValueReference {
  return {
    type: $ref,
    to: attributePath,
    default: defaultTo,
  }
}
