import type { BaseConfig, TableCommand, PreparedBatchGet } from '@/commands'
import type { EntitySchema } from '@/core'
import type { DynamoTable } from '@/core/table'
import type { ConsumedCapacity } from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'
import { DocumentBuilderError } from '@/errors'
import { BatchGetCommand } from '@aws-sdk/lib-dynamodb'
import { BATCH_GET_VALIDATION_CONCURRENCY } from '@/internal-constants'
import pMap from 'p-map'

/**
 * Extracts the result item type from a PreparedBatchGet.
 */
// biome-ignore lint/suspicious/noExplicitAny: required for conditional type extraction
type ExtractSchema<T> = T extends PreparedBatchGet<infer S> ? EntitySchema<S> : never

/**
 * Maps an array of PreparedBatchGets to a tuple of their result item arrays.
 */
type TableBatchGetItems<Gets extends PreparedBatchGet<ZodObject>[]> = {
  [K in keyof Gets]: Array<ExtractSchema<Gets[K]>>
}

/**
 * Maps an array of PreparedBatchGets to a tuple of their unprocessed key arrays.
 */
type TableBatchGetUnprocessedKeys<Gets extends PreparedBatchGet<ZodObject>[]> = {
  [K in keyof Gets]: Array<Partial<ExtractSchema<Gets[K]>>> | undefined
}

/**
 * Configuration for the TableBatchGet command.
 *
 * @template Gets - Tuple of PreparedBatchGet types, one per entity group.
 */
// biome-ignore lint/suspicious/noExplicitAny: gets span multiple heterogeneous entity schemas
export type TableBatchGetConfig<Gets extends PreparedBatchGet<any>[]> = BaseConfig & {
  // biome-ignore lint/suspicious/noExplicitAny: gets span multiple heterogeneous entity schemas
  gets: [...Gets]
  /**
   * If set, overrides the `consistent` setting on all individual prepared groups.
   * When `true`, DynamoDB will use strongly consistent reads for all entity groups.
   * When `false`, DynamoDB will use eventually consistent reads for all entity groups.
   * When not set, the command falls back to the per-group `consistent` setting
   * (any group with `consistent: true` makes the entire request consistent).
   */
  consistent?: boolean
}

/**
 * Result of the TableBatchGet command.
 *
 * @template Gets - Tuple of PreparedBatchGet types, one per entity group.
 */
// biome-ignore lint/suspicious/noExplicitAny: gets span multiple heterogeneous entity schemas
export type TableBatchGetResult<Gets extends PreparedBatchGet<any>[]> = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity | undefined
  items: TableBatchGetItems<Gets>
  unprocessedKeys: TableBatchGetUnprocessedKeys<Gets>
}

/**
 * Table-level command to perform a batch get across multiple entity types.
 *
 * Unlike `BatchGet` which operates on a single entity type, this command accepts
 * get operations from multiple entities via `entity.prepare(new BatchGet({ keys: [...] }))`
 * and returns results grouped by entity in a tuple structure.
 *
 * Unprocessed keys are returned per entity in a tuple matching the input order.
 *
 * @example
 * ```typescript
 * const { items, unprocessedKeys } = await table.send(new TableBatchGet({
 *   gets: [
 *     userEntity.prepare(new BatchGet({
 *       keys: [{ userId: 'u1' }, { userId: 'u2' }],
 *     })),
 *     orderEntity.prepare(new BatchGet({
 *       keys: [{ orderId: 'o1' }],
 *     })),
 *   ],
 *   consistent: true, // Apply strongly consistent reads to all groups
 * }))
 *
 * const [users, orders] = items
 * // users: User[]
 * // orders: Order[]
 * ```
 */
// biome-ignore lint/suspicious/noExplicitAny: gets span multiple heterogeneous entity schemas
export class TableBatchGet<Gets extends PreparedBatchGet<any>[]>
  implements TableCommand<TableBatchGetResult<Gets>>
{
  #config: TableBatchGetConfig<Gets>

  constructor(config: TableBatchGetConfig<Gets>) {
    this.#config = config
  }

  public async execute(table: DynamoTable): Promise<TableBatchGetResult<Gets>> {
    for (const group of this.#config.gets) {
      if (group.entity.table !== table) {
        throw new DocumentBuilderError(
          `Entity table "${group.entity.table.tableName}" does not match the executing table "${table.tableName}"`,
        )
      }
    }

    // Command-level consistent overrides all group-level settings.
    // If not specified, fall back to the per-group OR logic (any group consistent → all consistent).
    // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
    const consistent =
      this.#config.consistent ?? this.#config.gets.some((g: PreparedBatchGet<any>) => g.consistent)

    // Aggregate all keys from every entity group into a single list
    // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
    const allKeys = this.#config.gets.flatMap((g: PreparedBatchGet<any>) => g.keys)

    const batchGet = new BatchGetCommand({
      RequestItems: {
        [table.tableName]: {
          Keys: allKeys,
          ConsistentRead: consistent,
        },
      },
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const result = await table.documentClient.send(batchGet, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const rawItems = (result.Responses?.[table.tableName] ?? []) as Record<string, unknown>[]

    // Match returned items back to their entity groups by comparing primary keys.
    // DynamoDB does not guarantee order in batch get responses, so we must match by key.
    const groupedItems: Array<unknown[]> = this.#config.gets.map(() => [])

    for (const item of rawItems) {
      for (let i = 0; i < this.#config.gets.length; i++) {
        // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
        const group = this.#config.gets[i] as PreparedBatchGet<any>
        if (group.matchItem(item)) {
          groupedItems[i]!.push(item)
          break
        }
      }
    }

    // Parse each group's matched items
    const parsedItems = await pMap(
      this.#config.gets,
      // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
      async (group: PreparedBatchGet<any>, i: number) =>
        group.parseResults(groupedItems[i]!, this.#config.skipValidation ?? false),
      {
        concurrency: BATCH_GET_VALIDATION_CONCURRENCY,
        signal: this.#config.abortController?.signal,
      },
    )

    // Map unprocessed keys back to per-entity groups
    const rawUnprocessedKeys =
      // biome-ignore lint/suspicious/noExplicitAny: DynamoDB SDK returns untyped key objects
      (result.UnprocessedKeys?.[table.tableName]?.Keys ?? []) as any[]

    const unprocessedKeys: Array<Array<unknown> | undefined> = this.#config.gets.map(
      () => undefined,
    )

    for (const key of rawUnprocessedKeys) {
      for (let i = 0; i < this.#config.gets.length; i++) {
        // biome-ignore lint/suspicious/noExplicitAny: heterogeneous group schemas require any
        const group = this.#config.gets[i] as PreparedBatchGet<any>
        const matched = group.matchUnprocessedKey(key)
        if (matched !== undefined) {
          if (!unprocessedKeys[i]) unprocessedKeys[i] = []
          unprocessedKeys[i]!.push(matched)
          break
        }
      }
    }

    return {
      responseMetadata: result.$metadata,
      consumedCapacity: result.ConsumedCapacity?.[0],
      items: parsedItems as TableBatchGetItems<Gets>,
      unprocessedKeys: unprocessedKeys as TableBatchGetUnprocessedKeys<Gets>,
    }
  }
}
