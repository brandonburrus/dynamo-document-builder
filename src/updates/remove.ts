import type { RemoveExpression } from '@/updates'
import { $remove } from '@/updates/update-symbols'

/**
 * Update function to remove an attribute.
 */
export function remove(): RemoveExpression {
  return {
    type: $remove,
  }
}
