import type { TransactWriteOperation, ObjectLikeZodType } from '@/core'
import type { DynamoEntity } from '@/core/entity'
import type { DynamoTable } from '@/core/table'
import type { EntitySchema } from '@/core'
import type { DynamoKey } from '@/core/key'
import type { ConsumedCapacity, ReturnConsumedCapacity } from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'

/**
 * Interface for commands that can be prepared for use in a table-level batch write.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type BatchWritePreparable<Schema extends ObjectLikeZodType> = {
  readonly items?: Array<EntitySchema<Schema>>
  readonly deletes?: Array<Partial<EntitySchema<Schema>>>
}

/**
 * Interface for commands that can be prepared for use in a table-level batch get.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type BatchGetPreparable<Schema extends ObjectLikeZodType> = {
  readonly keys: Array<Partial<EntitySchema<Schema>>>
  readonly consistent?: boolean
}

/**
 * Interface-like type for command classes to extend from that defines the execute method.
 *
 * This is what enables the command-based pattern for DynamoDB operations that makes tree-shaking possible.
 *
 * @template Output The output type of the command's execute method.
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type BaseCommand<Output, Schema extends ObjectLikeZodType> = {
  execute(entity: DynamoEntity<Schema>): Promise<Output>
}

/**
 * Interface-like type for paginatable command classes to extend from that defines the executePaginated method.
 *
 * Commands should only implement this interface if they support pagination.
 *
 * @template Output The output type of the command's executePaginated method.
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type BasePaginatable<Output, Schema extends ObjectLikeZodType> = {
  executePaginated(entity: DynamoEntity<Schema>): AsyncGenerator<Output, void, unknown>
}

/**
 * Interface-like type for get commands that can be included in a table-level get transaction.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type GetTransactable<Schema extends ObjectLikeZodType> = {
  readonly keys: Array<Partial<EntitySchema<Schema>>>
}

/**
 * Interface-like type for write commands that can be included in a write transaction.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type WriteTransactable<Schema extends ObjectLikeZodType> = {
  prepareWriteTransaction(entity: DynamoEntity<Schema>): Promise<TransactWriteOperation>
}

/**
 * Interface-like type for table-level commands that execute directly against a DynamoTable.
 *
 * @template Output The output type of the command's execute method.
 */
export type TableCommand<Output> = {
  execute(table: DynamoTable): Promise<Output>
}

/**
 * Represents a set of write operations bound to a specific entity, ready to be included
 * in a table-level TransactWrite command.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type PreparedWriteTransaction<Schema extends ObjectLikeZodType> = {
  entity: DynamoEntity<Schema>
  writes: WriteTransactable<Schema>[]
}

/**
 * Represents a TransactGet command bound to a specific entity, ready to be included
 * in a table-level TransactGet command.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type PreparedGetTransaction<Schema extends ObjectLikeZodType> = {
  entity: DynamoEntity<Schema>
  keys: Array<{ TableName: string; Key: DynamoKey }>
  parseResults(
    items: unknown[],
    skipValidation: boolean,
  ): Promise<Array<EntitySchema<Schema> | undefined>>
}

/**
 * Represents a BatchWrite command bound to a specific entity, ready to be included
 * in a table-level TableBatchWrite command.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type PreparedBatchWrite<Schema extends ObjectLikeZodType> = {
  entity: DynamoEntity<Schema>
  buildRequests(
    skipValidation: boolean,
    abortSignal?: AbortSignal,
  ): Promise<
    Array<{ PutRequest: { Item: Record<string, unknown> } } | { DeleteRequest: { Key: DynamoKey } }>
  >
  matchUnprocessedPut(item: Record<string, unknown>): EntitySchema<Schema> | undefined
  matchUnprocessedDelete(key: DynamoKey): Partial<EntitySchema<Schema>> | undefined
}

/**
 * Represents a BatchGet command bound to a specific entity, ready to be included
 * in a table-level TableBatchGet command.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type PreparedBatchGet<Schema extends ObjectLikeZodType> = {
  entity: DynamoEntity<Schema>
  keys: Array<DynamoKey>
  consistent: boolean
  matchItem(item: Record<string, unknown>): boolean
  parseResults(items: unknown[], skipValidation: boolean): Promise<Array<EntitySchema<Schema>>>
  matchUnprocessedKey(key: DynamoKey): Partial<EntitySchema<Schema>> | undefined
}

/**
 * Base configuration options for DynamoDB commands.
 *
 * @property skipValidation - If true, skips schema validation of the returned item(s).
 * @property returnConsumedCapacity - Specifies the level of detail about provisioned throughput consumption that is returned in the response.
 * @property abortController - An AbortController to allow cancellation of the operation.
 * @property timeoutMs - The maximum time in milliseconds to wait for the operation to complete.
 */
export type BaseConfig = {
  skipValidation?: boolean
  returnConsumedCapacity?: ReturnConsumedCapacity
  abortController?: AbortController
  timeoutMs?: number
}

/**
 * Base result type for DynamoDB commands.
 *
 * @property responseMetadata - Metadata about the response from DynamoDB.
 * @property consumedCapacity - Information about the capacity units consumed by the operation.
 */
export type BaseResult = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
}

// Reads
export * from './get'
export * from './query'
export * from './scan'
export * from './projected-get'
export * from './projected-query'
export * from './projected-scan'
export * from './batch-get'
export * from './batch-projected-get'
export * from './transact-get'

// Writes
export * from './put'
export * from './update'
export * from './delete'
export * from './conditional-put'
export * from './conditional-update'
export * from './conditional-delete'
export * from './batch-write'
export * from './transact-write'
export * from './table-transact-write'
export * from './table-batch-write'

// Reads (table-level)
export * from './table-transact-get'
export * from './table-batch-get'

// Misc
export * from './condition-check'
