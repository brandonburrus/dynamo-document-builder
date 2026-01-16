import type { BaseConfig, BaseCommand, WriteTransactable } from '@/commands'
import type {
  ConsumedCapacity,
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
} from '@aws-sdk/client-dynamodb'
import type { DynamoEntity } from '@/core/entity'
import type { ResponseMetadata } from '@aws-sdk/types'
import type { ZodObject } from 'zod/v4'
import { TRANSACTION_WRITE_VALIDATION_CONCURRENCY } from '@/internal-constants'
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb'
import pMap from 'p-map'

/**
 * Configuration for the TransactWrite command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type TransactWriteConfig<Schema extends ZodObject> = BaseConfig & {
  writes: WriteTransactable<Schema>[]
  idempotencyToken?: string
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

/**
 * Result of the TransactWrite command.
 */
export type TransactWriteResult = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity[] | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

/**
 * Command to perform an atomic multi-item write transaction (all-or-nothing).
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, TransactWrite, Put, Update, Delete } from 'dynamo-document-builder';
 *
 * const table = new DynamoTable({
 *   tableName: 'ExampleTable',
 *   documentClient,
 * });
 *
 * const userEntity = new DynamoEntity({
 *   table,
 *   schema: z.object({
 *     userId: z.string(),
 *     name: z.string(),
 *     balance: z.number(),
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const transactWriteCommand = new TransactWrite({
 *   writes: [
 *     new Put({ item: { userId: 'user1', name: 'John', balance: 100 } }),
 *     new Update({ key: { userId: 'user2' }, update: { balance: add(50) } }),
 *     new Delete({ key: { userId: 'user3' } }),
 *   ],
 *   idempotencyToken: 'unique-token',
 * });
 *
 * await userEntity.send(transactWriteCommand);
 * ```
 */
export class TransactWrite<Schema extends ZodObject>
  implements BaseCommand<TransactWriteResult, Schema>
{
  #config: TransactWriteConfig<Schema>

  constructor(config: TransactWriteConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<TransactWriteResult> {
    const transactItems = await pMap(
      this.#config.writes,
      write => write.prepareWriteTransaction(entity),
      {
        concurrency: TRANSACTION_WRITE_VALIDATION_CONCURRENCY,
        signal: this.#config.abortController?.signal,
      },
    )
    const writeTransaction = new TransactWriteCommand({
      TransactItems: transactItems,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
      ClientRequestToken: this.#config.idempotencyToken,
    })
    const writeTransactionResult = await entity.table.documentClient.send(writeTransaction, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })
    return {
      responseMetadata: writeTransactionResult.$metadata,
      consumedCapacity: writeTransactionResult.ConsumedCapacity,
      itemCollectionMetrics: writeTransactionResult.ItemCollectionMetrics,
    }
  }
}
