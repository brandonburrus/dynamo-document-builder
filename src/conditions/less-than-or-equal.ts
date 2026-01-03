import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions/condition-types'

export function lessThanOrEqual(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '<=',
    value,
  }
}
