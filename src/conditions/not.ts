import { $not } from '@/conditions/condition-symbols'
import type { ConditionExpressionTemplate, NotExpression } from '@/conditions/condition-types'

export function not(condition: ConditionExpressionTemplate): NotExpression {
  return {
    type: $not,
    condition,
  }
}
