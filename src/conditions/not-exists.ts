import { $exists } from '@/conditions/condition-symbols'
import type { ExistsExpressionTemplate } from '@/conditions'

/**
 * Creates a NOT EXISTS expression template.
 */
export function notExists(): ExistsExpressionTemplate {
  return {
    type: $exists,
    not: true,
  }
}
