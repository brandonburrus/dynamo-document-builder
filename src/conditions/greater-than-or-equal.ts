import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions'

/**
 * Creates a "greater than or equal to" `>=` comparison expression template.
 */
export function greaterThanOrEqual(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '>=',
    value,
  }
}
