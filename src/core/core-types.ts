import type { output as ZodOutput, input as ZodInput, ZodObject } from 'zod/v4'
import type { DynamoEntity } from '@/core/entity'
import type {
  ConditionCheck,
  Delete,
  Put,
  TransactWriteItem,
  Update,
} from '@aws-sdk/client-dynamodb'
import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'

// This is the actual type of the entity items dervied from the zod schema
export type EntitySchema<SchemaDef extends ZodObject> = ZodOutput<SchemaDef>
export type EncodedEntitySchema<SchemaDef extends ZodObject> = ZodInput<SchemaDef>

// External type exposed to module users for getting the schema type from a defined entity
// biome-ignore lint/suspicious/noExplicitAny: Schema is the output type, not the input; any is ok
export type Entity<E extends DynamoEntity<any>> = ZodOutput<E['schema']>
// biome-ignore lint/suspicious/noExplicitAny: Schema is the output type, not the input; any is ok
export type EncodedEntity<E extends DynamoEntity<any>> = ZodInput<E['schema']>

export type IndexName = string

export type GlobalSecondaryIndexKeyName = {
  partitionKey: string
  sortKey?: string
}

export type LocalSecondaryIndexKeyName = {
  sortKey: string
}

export type NamedGlobalSecondaryIndexKeyNames = Record<IndexName, GlobalSecondaryIndexKeyName>
export type NamedLocalSecondaryIndexKeyNames = Record<IndexName, LocalSecondaryIndexKeyName>

// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-dynamodb/TypeAlias/TransactWriteCommandInput/
export type TransactWriteOperation = Omit<
  TransactWriteItem,
  'ConditionCheck' | 'Put' | 'Delete' | 'Update'
> & {
  ConditionCheck?:
    | (Omit<ConditionCheck, 'Key' | 'ExpressionAttributeValues'> & {
        Key: Record<string, NativeAttributeValue> | undefined
        ExpressionAttributeValues?: Record<string, NativeAttributeValue> | undefined
      })
    | undefined
  Put?:
    | (Omit<Put, 'Item' | 'ExpressionAttributeValues'> & {
        Item: Record<string, NativeAttributeValue> | undefined
        ExpressionAttributeValues?: Record<string, NativeAttributeValue> | undefined
      })
    | undefined
  Delete?:
    | (Omit<Delete, 'Key' | 'ExpressionAttributeValues'> & {
        Key: Record<string, NativeAttributeValue> | undefined
        ExpressionAttributeValues?: Record<string, NativeAttributeValue> | undefined
      })
    | undefined
  Update?:
    | (Omit<Update, 'Key' | 'ExpressionAttributeValues'> & {
        Key: Record<string, NativeAttributeValue> | undefined
        ExpressionAttributeValues?: Record<string, NativeAttributeValue> | undefined
      })
    | undefined
}
