import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions'

/**
 * Creates a "less than" `<` comparison expression template.
 */
export function lessThan(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '<',
    value,
  }
}
