import { $not } from '@/conditions/condition-symbols'
import type { ConditionExpressionTemplate, NotExpression } from '@/conditions'

/**
 * Creates a NOT expression template that negates a given condition expression.
 */
export function not(condition: ConditionExpressionTemplate): NotExpression {
  return {
    type: $not,
    condition,
  }
}
