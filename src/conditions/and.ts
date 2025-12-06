import { $logical } from '@/conditions/condition-symbols'
import type { ConditionExpressionTemplate, LogicalExpression } from '@/conditions/condition-types'

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
