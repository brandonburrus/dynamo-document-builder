import type { BaseConfig, TableCommand, PreparedBatchWrite } from '@/commands'
import type { DynamoTable } from '@/core/table'
import type { EntitySchema } from '@/core'
import type {
  ConsumedCapacity,
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
} from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'
import { DocumentBuilderError } from '@/errors'
import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb'
import { BATCH_WRITE_VALIDATION_CONCURRENCY } from '@/internal-constants'
import pMap from 'p-map'

/**
 * Extracts the entity schema type from a PreparedBatchWrite.
 */
// biome-ignore lint/suspicious/noExplicitAny: required for conditional type extraction
type ExtractSchema<T> = T extends PreparedBatchWrite<infer S> ? EntitySchema<S> : never

/**
 * Maps an array of PreparedBatchWrites to a tuple of their unprocessed put arrays.
 */
type TableBatchWriteUnprocessedPuts<Writes extends PreparedBatchWrite<ZodObject>[]> = {
  [K in keyof Writes]: Array<ExtractSchema<Writes[K]>> | undefined
}

/**
 * Maps an array of PreparedBatchWrites to a tuple of their unprocessed delete arrays.
 */
type TableBatchWriteUnprocessedDeletes<Writes extends PreparedBatchWrite<ZodObject>[]> = {
  [K in keyof Writes]: Array<Partial<ExtractSchema<Writes[K]>>> | undefined
}

/**
 * Configuration for the TableBatchWrite command.
 *
 * @template Writes - Tuple of PreparedBatchWrite types, one per entity group.
 */
// biome-ignore lint/suspicious/noExplicitAny: writes span multiple heterogeneous entity schemas
export type TableBatchWriteConfig<Writes extends PreparedBatchWrite<any>[]> = BaseConfig & {
  // biome-ignore lint/suspicious/noExplicitAny: writes span multiple heterogeneous entity schemas
  writes: [...Writes]
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

/**
 * Result of the TableBatchWrite command.
 *
 * @template Writes - Tuple of PreparedBatchWrite types, one per entity group.
 */
// biome-ignore lint/suspicious/noExplicitAny: writes span multiple heterogeneous entity schemas
export type TableBatchWriteResult<Writes extends PreparedBatchWrite<any>[]> = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
  unprocessedPuts: TableBatchWriteUnprocessedPuts<Writes>
  unprocessedDeletes: TableBatchWriteUnprocessedDeletes<Writes>
}

/**
 * Table-level command to perform a batch write across multiple entity types.
 *
 * Unlike `BatchWrite` which operates on a single entity type, this command accepts
 * write operations from multiple entities via `entity.prepare(new BatchWrite({ ... }))`
 * and executes them in a single DynamoDB BatchWrite request.
 *
 * Unprocessed puts and deletes are returned per entity in a tuple matching the input order.
 *
 * @example
 * ```typescript
 * const { unprocessedPuts, unprocessedDeletes } = await table.send(new TableBatchWrite({
 *   writes: [
 *     userEntity.prepare(new BatchWrite({
 *       items: [{ userId: 'u1', name: 'Alice' }],
 *       deletes: [{ userId: 'u2', name: 'Bob' }],
 *     })),
 *     orderEntity.prepare(new BatchWrite({
 *       items: [{ orderId: 'o1', status: 'pending', total: 99 }],
 *     })),
 *   ],
 * }))
 *
 * const [userUnprocessedPuts, orderUnprocessedPuts] = unprocessedPuts
 * const [userUnprocessedDeletes, orderUnprocessedDeletes] = unprocessedDeletes
 * ```
 */
// biome-ignore lint/suspicious/noExplicitAny: writes span multiple heterogeneous entity schemas
export class TableBatchWrite<Writes extends PreparedBatchWrite<any>[]>
  implements TableCommand<TableBatchWriteResult<Writes>>
{
  #config: TableBatchWriteConfig<Writes>

  constructor(config: TableBatchWriteConfig<Writes>) {
    this.#config = config
  }

  public async execute(table: DynamoTable): Promise<TableBatchWriteResult<Writes>> {
    for (const group of this.#config.writes) {
      if (group.entity.table !== table) {
        throw new DocumentBuilderError(
          `Entity table "${group.entity.table.tableName}" does not match the executing table "${table.tableName}"`,
        )
      }
    }

    // Build all requests across all entity groups, preserving group index for unprocessed mapping
    // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
    const allRequests = (
      await pMap(
        this.#config.writes,
        (group: PreparedBatchWrite<any>) =>
          group.buildRequests(
            this.#config.skipValidation ?? false,
            this.#config.abortController?.signal,
          ),
        {
          concurrency: BATCH_WRITE_VALIDATION_CONCURRENCY,
          signal: this.#config.abortController?.signal,
        },
      )
    ).flat()

    const batchWrite = new BatchWriteCommand({
      RequestItems: {
        [table.tableName]: allRequests,
      },
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
    })

    const result = await table.documentClient.send(batchWrite, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const unprocessedItems = result.UnprocessedItems?.[table.tableName] ?? []

    // Map unprocessed items back to their respective entity groups
    const unprocessedPuts: Array<Array<unknown> | undefined> = this.#config.writes.map(
      () => undefined,
    )
    const unprocessedDeletes: Array<Array<unknown> | undefined> = this.#config.writes.map(
      () => undefined,
    )

    for (const unprocessedItem of unprocessedItems) {
      if (unprocessedItem.PutRequest?.Item) {
        const item = unprocessedItem.PutRequest.Item as Record<string, unknown>
        for (let i = 0; i < this.#config.writes.length; i++) {
          // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
          const group = this.#config.writes[i] as PreparedBatchWrite<any>
          const matched = group.matchUnprocessedPut(item)
          if (matched !== undefined) {
            if (!unprocessedPuts[i]) unprocessedPuts[i] = []
            unprocessedPuts[i]!.push(matched)
            break
          }
        }
      } else if (unprocessedItem.DeleteRequest?.Key) {
        // biome-ignore lint/suspicious/noExplicitAny: DynamoDB SDK returns untyped key objects
        const key = unprocessedItem.DeleteRequest.Key as any
        for (let i = 0; i < this.#config.writes.length; i++) {
          // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
          const group = this.#config.writes[i] as PreparedBatchWrite<any>
          const matched = group.matchUnprocessedDelete(key)
          if (matched !== undefined) {
            if (!unprocessedDeletes[i]) unprocessedDeletes[i] = []
            unprocessedDeletes[i]!.push(matched)
            break
          }
        }
      }
    }

    return {
      responseMetadata: result.$metadata,
      consumedCapacity: result.ConsumedCapacity?.[0],
      itemCollectionMetrics: result.ItemCollectionMetrics,
      unprocessedPuts: unprocessedPuts as TableBatchWriteUnprocessedPuts<Writes>,
      unprocessedDeletes: unprocessedDeletes as TableBatchWriteUnprocessedDeletes<Writes>,
    }
  }
}
