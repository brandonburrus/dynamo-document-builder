import type { BaseConfig, TableCommand, PreparedWriteTransaction } from '@/commands'
import type { DynamoTable } from '@/core/table'
import type {
  ConsumedCapacity,
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
} from '@aws-sdk/client-dynamodb'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'
import { DocumentBuilderError } from '@/errors'
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb'
import { TRANSACTION_WRITE_VALIDATION_CONCURRENCY } from '@/internal-constants'
import pMap from 'p-map'

/**
 * Configuration for the TableTransactWrite command.
 */
// biome-ignore lint/suspicious/noExplicitAny: transactions span multiple heterogeneous entity schemas
export type TableTransactWriteConfig = BaseConfig & {
  // biome-ignore lint/suspicious/noExplicitAny: transactions span multiple heterogeneous entity schemas
  transactions: PreparedWriteTransaction<any>[]
  idempotencyToken?: string
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

/**
 * Result of the TableTransactWrite command.
 */
export type TableTransactWriteResult = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity[] | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

/**
 * Table-level command to perform an atomic write transaction across multiple entity types.
 *
 * Unlike `TransactWrite` which operates on a single entity type, this command accepts
 * operations from multiple entities via `entity.prepare([...])` and executes them as
 * a single all-or-nothing DynamoDB transaction.
 *
 * @example
 * ```typescript
 * await table.send(new TableTransactWrite({
 *   transactions: [
 *     userEntity.prepare([
 *       new Put({ item: { userId: 'u1', name: 'Alice' } }),
 *       new Delete({ key: { userId: 'u2', name: 'Bob' } }),
 *     ]),
 *     orderEntity.prepare([
 *       new Update({ key: { orderId: 'o1' }, update: { status: 'shipped' } }),
 *     ]),
 *   ],
 * }))
 * ```
 */
export class TableTransactWrite implements TableCommand<TableTransactWriteResult> {
  #config: TableTransactWriteConfig

  constructor(config: TableTransactWriteConfig) {
    this.#config = config
  }

  public async execute(table: DynamoTable): Promise<TableTransactWriteResult> {
    for (const { entity } of this.#config.transactions) {
      if (entity.table !== table) {
        throw new DocumentBuilderError(
          `Entity table "${entity.table.tableName}" does not match the executing table "${table.tableName}"`,
        )
      }
    }

    const transactItems = (
      await pMap(
        this.#config.transactions,
        ({ entity, writes }: PreparedWriteTransaction<ZodObject>) =>
          pMap(writes, write => write.prepareWriteTransaction(entity), {
            concurrency: TRANSACTION_WRITE_VALIDATION_CONCURRENCY,
            signal: this.#config.abortController?.signal,
          }),
        {
          concurrency: TRANSACTION_WRITE_VALIDATION_CONCURRENCY,
          signal: this.#config.abortController?.signal,
        },
      )
    ).flat()

    const writeTransaction = new TransactWriteCommand({
      TransactItems: transactItems,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ClientRequestToken: this.#config.idempotencyToken,
    })

    const result = await table.documentClient.send(writeTransaction, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    return {
      responseMetadata: result.$metadata,
      consumedCapacity: result.ConsumedCapacity,
      itemCollectionMetrics: result.ItemCollectionMetrics,
    }
  }
}
