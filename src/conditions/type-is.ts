import { $type } from '@/conditions/condition-symbols'
import type { DynamoAttributeType, TypeCheckExpressionTemplate } from '@/conditions'

/**
 * Creates a type check expression template for the specified DynamoDB attribute type.
 */
export function typeIs(type: DynamoAttributeType): TypeCheckExpressionTemplate {
  return {
    type: $type,
    attributeType: type,
  }
}
