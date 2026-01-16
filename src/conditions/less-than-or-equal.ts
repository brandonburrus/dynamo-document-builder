import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions'

/**
 * Creates a "less than or equal to" `<=` comparison expression template.
 */
export function lessThanOrEqual(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '<=',
    value,
  }
}
