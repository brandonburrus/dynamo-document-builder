import type { DynamoEntity } from '@/core/entity'
import type {
  ConsumedCapacity,
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
} from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb'
import type { BaseConfig, BaseCommand, WriteTransactable } from '@/commands/base-command'
import type { ResponseMetadata } from '@aws-sdk/types'
import pMap from 'p-map'
import { TRANSACTION_WRITE_VALIDATION_CONCURRENCY } from '@/internal-constants'

export type TransactWriteConfig<Schema extends ZodObject> = BaseConfig & {
  writes: WriteTransactable<Schema>[]
  idempotencyToken?: string
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

export type TransactWriteResult = {
  responseMetadata?: ResponseMetadata
  consumedCapacity?: ConsumedCapacity[] | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

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
