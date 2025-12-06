import { $exists } from '@/conditions/condition-symbols'
import type { ExistsExpressionTemplate } from '@/conditions/condition-types'

export function exists(): ExistsExpressionTemplate {
  return {
    type: $exists,
  }
}
