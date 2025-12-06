import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type { DynamoAttributeExpression } from '@/attributes'

export interface AttributeBuilderInput {
  names?: Record<string, string>
  values?: Record<string, NativeAttributeValue>
}

export function buildAttributeExpression(input: AttributeBuilderInput): DynamoAttributeExpression {
  const result: DynamoAttributeExpression = {}
  if (input.names && Object.keys(input.names).length > 0) {
    result.ExpressionAttributeNames = input.names
  }
  if (input.values && Object.keys(input.values).length > 0) {
    result.ExpressionAttributeValues = input.values
  }
  return result
}
