import { $exists } from '@/conditions/condition-symbols'
import type { ExistsExpressionTemplate } from '@/conditions/condition-types'

export function notExists(): ExistsExpressionTemplate {
  return {
    type: $exists,
    not: true,
  }
}
