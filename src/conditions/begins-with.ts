import { $beginsWith } from '@/conditions/condition-symbols'
import type { BeginsWithExpressionTemplate } from '@/conditions'

/**
 * Creates a BEGINS WITH expression template.
 */
export function beginsWith(prefix: string): BeginsWithExpressionTemplate {
  return {
    type: $beginsWith,
    prefix,
  }
}
