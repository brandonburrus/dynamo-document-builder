import type { TransactWriteOperation } from '@/core'
import type { DynamoEntity } from '@/core/entity'
import type { ConsumedCapacity, ReturnConsumedCapacity } from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'

/**
 * Interface-like type for command classes to extend from that defines the execute method.
 *
 * This is what enables the command-based pattern for DynamoDB operations that makes tree-shaking possible.
 *
 * @template Output The output type of the command's execute method.
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type BaseCommand<Output, Schema extends ZodObject> = {
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
export type BasePaginatable<Output, Schema extends ZodObject> = {
  executePaginated(entity: DynamoEntity<Schema>): AsyncGenerator<Output, void, unknown>
}

/**
 * Interface-like type for write commands that can be included in a write transaction.
 *
 * @template Schema The Zod schema type associated with the DynamoEntity.
 */
export type WriteTransactable<Schema extends ZodObject> = {
  prepareWriteTransaction(entity: DynamoEntity<Schema>): Promise<TransactWriteOperation>
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

// Misc
export * from './condition-check'
