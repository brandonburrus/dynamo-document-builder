import { $logical } from '@/conditions/condition-symbols'
import type { ConditionExpressionTemplate, LogicalExpression } from '@/conditions/condition-types'

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
