import { $size } from '@/conditions/condition-symbols'
import type { SizeExpression } from '@/conditions/condition-types'

export function size(attributeNameOrPath: string): SizeExpression {
  return {
    type: $size,
    attribute: attributeNameOrPath,
  }
}
