import type { RemoveExpression } from '@/updates/update-types'
import { $remove } from '@/updates/update-symbols'

export function remove(): RemoveExpression {
  return {
    type: $remove,
  }
}
