import type { BaseConfig, BaseResult, TableCommand, PreparedGetTransaction } from '@/commands'
import type { DynamoTable } from '@/core/table'
import type { EntitySchema, ObjectLikeZodType } from '@/core'
import { DocumentBuilderError } from '@/errors'
import { TransactGetCommand } from '@aws-sdk/lib-dynamodb'

/**
 * Extracts the result item type from a PreparedGetTransaction.
 */
// biome-ignore lint/suspicious/noExplicitAny: required for conditional type extraction
type ExtractSchema<T> = T extends PreparedGetTransaction<infer S> ? EntitySchema<S> : never

/**
 * Maps an array of PreparedGetTransactions to a tuple of their result item arrays.
 */
type TableTransactGetItems<Gets extends PreparedGetTransaction<ObjectLikeZodType>[]> = {
  [K in keyof Gets]: Array<ExtractSchema<Gets[K]> | undefined>
}

/**
 * Configuration for the TableTransactGet command.
 *
 * @template Gets - Tuple of PreparedGetTransaction types, one per entity group.
 */
export type TableTransactGetConfig<Gets extends PreparedGetTransaction<ObjectLikeZodType>[]> =
  BaseConfig & {
    gets: [...Gets]
  }

/**
 * Result of the TableTransactGet command.
 *
 * @template Gets - Tuple of PreparedGetTransaction types, one per entity group.
 */
export type TableTransactGetResult<Gets extends PreparedGetTransaction<ObjectLikeZodType>[]> =
  BaseResult & {
    items: TableTransactGetItems<Gets>
  }

/**
 * Table-level command to perform a transactional read across multiple entity types.
 *
 * Unlike `TransactGet` which operates on a single entity type, this command accepts
 * get operations from multiple entities via `entity.prepare(new TransactGet({ keys: [...] }))`
 * and returns results grouped by entity in a tuple structure.
 *
 * @example
 * ```typescript
 * const { items } = await table.send(new TableTransactGet({
 *   gets: [
 *     userEntity.prepare(new TransactGet({ keys: [{ userId: 'u1' }] })),
 *     orderEntity.prepare(new TransactGet({ keys: [{ orderId: 'o1' }, { orderId: 'o2' }] })),
 *   ],
 * }))
 *
 * const [users, orders] = items
 * // users: (User | undefined)[]
 * // orders: (Order | undefined)[]
 * ```
 */
export class TableTransactGet<Gets extends PreparedGetTransaction<ObjectLikeZodType>[]>
  implements TableCommand<TableTransactGetResult<Gets>>
{
  #config: TableTransactGetConfig<Gets>

  constructor(config: TableTransactGetConfig<Gets>) {
    this.#config = config
  }

  public async execute(table: DynamoTable): Promise<TableTransactGetResult<Gets>> {
    for (const group of this.#config.gets) {
      if (group.entity.table !== table) {
        throw new DocumentBuilderError(
          `Entity table "${group.entity.table.tableName}" does not match the executing table "${table.tableName}"`,
        )
      }
    }

    const transactItems = this.#config.gets.flatMap(group => group.keys.map(key => ({ Get: key })))

    const transactGet = new TransactGetCommand({
      TransactItems: transactItems,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
    })

    const result = await table.documentClient.send(transactGet, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    const rawResponses = result.Responses ?? []

    // Split raw responses back into per-entity groups and parse each group
    let offset = 0
    const groupedItems: Array<Array<unknown>> = []

    for (const group of this.#config.gets) {
      const count = group.keys.length
      const slice = rawResponses.slice(offset, offset + count).map(r => r.Item)
      const parsed = await group.parseResults(slice, this.#config.skipValidation ?? false)
      groupedItems.push(parsed)
      offset += count
    }

    return {
      items: groupedItems as TableTransactGetItems<Gets>,
      responseMetadata: result.$metadata,
      consumedCapacity: result.ConsumedCapacity?.[0],
    }
  }
}
