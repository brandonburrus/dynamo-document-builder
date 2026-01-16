import { $logical } from '@/conditions/condition-symbols'
import type { ConditionExpressionTemplate, LogicalExpression } from '@/conditions'

/**
 * Combines multiple condition expressions using the logical AND operator.
 */
export function and(
  firstCondition: ConditionExpressionTemplate,
  secondCondition: ConditionExpressionTemplate,
  ...additionalConditions: ConditionExpressionTemplate[]
): LogicalExpression {
  return {
    type: $logical,
    operator: 'AND',
    subConditions: [firstCondition, secondCondition, ...additionalConditions],
  }
}
