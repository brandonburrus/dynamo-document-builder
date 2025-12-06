import { $contains } from '@/conditions/condition-symbols'
import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb'
import type { ContainsExpressionTemplate } from '@/conditions/condition-types'

export function contains(substringOrElement: NativeAttributeValue): ContainsExpressionTemplate {
  return {
    type: $contains,
    substringOrElement,
  }
}
