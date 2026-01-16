import { $between } from '@/conditions/condition-symbols'
import type { BetweenExpressionTemplate, ValueExpression } from '@/conditions'

/**
 * Creates a BETWEEN expression template. Lower and upper bounds are *inclusive*.
 */
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
