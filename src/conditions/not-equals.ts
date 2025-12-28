import { $comparison } from '@/conditions/condition-symbols'
import type { ComparisonExpressionTemplate, ValueExpression } from '@/conditions/condition-types'

export function notEquals(value: ValueExpression): ComparisonExpressionTemplate {
  return {
    type: $comparison,
    operator: '<>',
    value,
  }
}
