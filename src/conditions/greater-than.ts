import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions'

/**
 * Creates a "greater than" `>` comparison expression template.
 */
export function greaterThan(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '>',
    value,
  }
}
