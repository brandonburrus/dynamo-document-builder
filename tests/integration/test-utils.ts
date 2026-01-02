import { customAlphabet } from 'nanoid'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'

export const id = customAlphabet(
  'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
  6,
)
export const ttl = () => Math.floor(Date.now() / 1000 + 10 * 60) // 10 min
export const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

export const dynamodbClient = new DynamoDBClient({
  profile: process.env.AWS_RPFOILE,
  region: 'us-east-1',
})
export const documentClient = DynamoDBDocumentClient.from(dynamodbClient)
