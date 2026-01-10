import { $size } from '@/conditions/condition-symbols'
import type {
  ComparisonExpressionTemplate,
  SizeConditionExpressionTemplate,
} from '@/conditions/condition-types'

export function size(
  valueOrComparison: number | ComparisonExpressionTemplate,
): SizeConditionExpressionTemplate {
  if (typeof valueOrComparison === 'number') {
    return {
      type: $size,
      operator: '=',
      value: valueOrComparison,
    }
  }

  return {
    type: $size,
    operator: valueOrComparison.operator,
    value: valueOrComparison.value,
  }
}
