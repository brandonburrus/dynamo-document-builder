import { $beginsWith } from '@/conditions/condition-symbols'
import type { BeginsWithExpressionTemplate } from '@/conditions/condition-types'

export function beginsWith(prefix: string): BeginsWithExpressionTemplate {
  return {
    type: $beginsWith,
    prefix,
  }
}
