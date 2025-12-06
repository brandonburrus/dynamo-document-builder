import { $in } from '@/conditions/condition-symbols'
import type { InExpressionTemplate, ValueExpression } from '@/conditions/condition-types'

export function isIn(
  firstValue: ValueExpression,
  secondValue: ValueExpression,
  ...additionalValues: ValueExpression[]
): InExpressionTemplate {
  return {
    type: $in,
    values: [firstValue, secondValue, ...additionalValues],
  }
}
