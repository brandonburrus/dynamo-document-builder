import { $type } from '@/conditions/condition-symbols'
import type { DynamoAttributeType, TypeCheckExpressionTemplate } from '@/conditions/condition-types'

export function typeIs(type: DynamoAttributeType): TypeCheckExpressionTemplate {
  return {
    type: $type,
    attributeType: type,
  }
}
