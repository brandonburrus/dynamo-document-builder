import type { ValueReference } from '@/updates'
import { $ref } from '@/updates/update-symbols'
import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'

/**
 * Creates a reference to another attribute's value.
 *
 * @param attributePath - The path of the attribute to reference.
 * @param defaultTo - An optional default value if the referenced attribute does not exist.
 */
export function ref(attributePath: string, defaultTo?: NativeAttributeValue): ValueReference {
  return {
    type: $ref,
    to: attributePath,
    default: defaultTo,
  }
}
