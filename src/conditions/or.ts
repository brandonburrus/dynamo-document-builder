import { $logical } from '@/conditions/condition-symbols'
import type { ConditionExpressionTemplate, LogicalExpression } from '@/conditions'

/**
 * Creates an OR logical expression template that combines multiple condition expressions.
 */
export function or(
  firstCondition: ConditionExpressionTemplate,
  secondCondition: ConditionExpressionTemplate,
  ...additionalConditions: ConditionExpressionTemplate[]
): LogicalExpression {
  return {
    type: $logical,
    operator: 'OR',
    subConditions: [firstCondition, secondCondition, ...additionalConditions],
  }
}
