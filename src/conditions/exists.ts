import { $exists } from '@/conditions/condition-symbols'
import type { ExistsExpressionTemplate } from '@/conditions'

/**
 * Creates an EXISTS expression template.
 */
export function exists(): ExistsExpressionTemplate {
  return {
    type: $exists,
  }
}
