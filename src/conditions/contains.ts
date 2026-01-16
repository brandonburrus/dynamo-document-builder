import { $contains } from '@/conditions/condition-symbols'
import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb'
import type { ContainsExpressionTemplate } from '@/conditions'

/**
 * Creates a CONTAINS expression template.
 */
export function contains(substringOrElement: NativeAttributeValue): ContainsExpressionTemplate {
  return {
    type: $contains,
    substringOrElement,
  }
}
