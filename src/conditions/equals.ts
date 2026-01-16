import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions'

/**
 * Creates an EQUALS comparison expression template.
 */
export function equals(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '=',
    value,
  }
}
