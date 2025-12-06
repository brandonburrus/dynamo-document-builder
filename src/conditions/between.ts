import { $between } from '@/conditions/condition-symbols'
import type { BetweenExpressionTemplate, ValueExpression } from '@/conditions/condition-types'

export function between(
  lowerBound: ValueExpression,
  upperBound: ValueExpression,
): BetweenExpressionTemplate {
  return {
    type: $between,
    lowerValue: lowerBound,
    upperValue: upperBound,
  }
}
