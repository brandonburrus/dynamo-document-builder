export * from './entity'
export * from './key'
export * from './table'

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

/**
 * Utility type used to derive the type from the Zod schema definition. Mainly for internal use.
 *
 * To get the schema type from an entity use [`Entity<E>`](/api-reference/type-aliases/entity) instead.
 */
export type EntitySchema<Schema extends ZodObject> = ZodOutput<Schema>

/**
 * Utility type used to derive the input type from the Zod schema definition. Mainly for internal use.
 *
 * To get the input schema type from an entity use [`EncodedEntity<E>`](/api-reference/type-aliases/encodedentity) instead.
 */
export type EncodedEntitySchema<Schema extends ZodObject> = ZodInput<Schema>

/**
 * Utility type to derive the type of a DynamoEntity's schema.
 *
 * This is the Zod *output* type of a Zod schema (the type after a codec has been applied).
 * It is the equivalent of `z.infer<typeof entity.schema>` or `z.output<typeof entity.schema>`.
 *
 * @example
 * ```ts
 * import { DynamoTable, DynamoEntity, type Entity } from 'dynamo-document-builder';
 * import { z } from 'zod';
 *
 * const table = new DynamoTable({
 *   tableName: 'ExampleTable',
 *   documentClient,
 * });
 *
 * const userEntity = new DynamoEntity({
 *   table,
 *   schema: z.object({
 *     id: z.string(),
 *     name: z.string(),
 *     age: z.number().optional(),
 *   }),
 *   partitionKey: user => key('USER', user.id),
 *   sortKey: user => 'USER',
 * });
 *
 * type User = Entity<typeof userEntity>;
 */
// biome-ignore lint/suspicious/noExplicitAny: Schema is the output type, not the input; any is ok
export type Entity<E extends DynamoEntity<any>> = ZodOutput<E['schema']>

/**
 * Utility type to derive the input type of a DynamoEntity's schema.
 *
 * This is the Zod *input* type of a Zod schema (the type before a codec has been applied).
 * It is the equivalent of `z.input<typeof entity.schema>`.
 *
 * @example
 * ```ts
 * import { DynamoTable, DynamoEntity, type EncodedEntity } from 'dynamo-document-builder';
 * import { z } from 'zod';
 *
 * const table = new DynamoTable({
 *   tableName: 'ExampleTable',
 *   documentClient,
 * });
 *
 * const isoDatetimeToDate = z.codec(z.iso.datetime(), z.date(), {
 *   decode: isoString => new Date(isoString),
 *   encode: date => date.toISOString(),
 * });
 *
 * const checkinEntity = new DynamoEntity({
 *  table,
 *  schema: z.object({
 *    id: z.string(),
 *    timestamp: isoDatetimeToDate,
 *  }),
 * });
 *
 * type EncodedUser = EncodedEntity<typeof userEntity>;
 *
 * // EncodedUser is:
 * // {
 * //   id: string;
 * //   timestamp: string; // ISO datetime string
 * // }
 * ```
 */
// biome-ignore lint/suspicious/noExplicitAny: Schema is the output type, not the input; any is ok
export type EncodedEntity<E extends DynamoEntity<any>> = ZodInput<E['schema']>

/**
 * Type used to indicate the name of a DynamoDB secondary index.
 */
export type IndexName = string

/**
 * Type representing the key names for a **global** secondary index.
 */
export type GlobalSecondaryIndexKeyName = {
  partitionKey: string
  sortKey?: string
}

/**
 * Type representing the key names for a **local** secondary index.
 */
export type LocalSecondaryIndexKeyName = {
  sortKey: string
}

/**
 * Type mapping index names to their corresponding global secondary index key names.
 */
export type NamedGlobalSecondaryIndexKeyNames = Record<IndexName, GlobalSecondaryIndexKeyName>

/**
 * Type mapping index names to their corresponding local secondary index key names.
 */
export type NamedLocalSecondaryIndexKeyNames = Record<IndexName, LocalSecondaryIndexKeyName>

/**
 * A modified version of TransactWriteItem that uses NativeAttributeValue for keys and items
 * (dynamodb-lib doesnt export this type for some reason).
 *
 * See: https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-lib-dynamodb/TypeAlias/TransactWriteCommandInput/
 */
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
