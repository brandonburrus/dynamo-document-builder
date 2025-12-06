import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'

export type ExpressionAttributeValues = Record<string, NativeAttributeValue> | undefined
export type ExpressionAttributeNames = Record<string, string> | undefined

export interface DynamoAttributeExpression {
  ExpressionAttributeValues?: ExpressionAttributeValues
  ExpressionAttributeNames?: ExpressionAttributeNames
}
