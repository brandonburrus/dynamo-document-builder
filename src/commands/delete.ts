import type { DynamoEntity } from '@/core/entity'
import type { EntitySchema, TransactWriteOperation } from '@/core'
import type {
  ItemCollectionMetrics,
  ReturnItemCollectionMetrics,
  ReturnValue,
} from '@aws-sdk/client-dynamodb'
import type { ZodObject } from 'zod/v4'
import { DeleteCommand } from '@aws-sdk/lib-dynamodb'
import type { BaseConfig, BaseCommand, BaseResult, WriteTransactable } from '@/commands'

/**
 * Configuration for the Delete command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type DeleteConfig<Schema extends ZodObject> = BaseConfig & {
  key: Partial<EntitySchema<Schema>>
  returnValues?: ReturnValue
  returnItemCollectionMetrics?: ReturnItemCollectionMetrics
}

/**
 * Result of the Delete command.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 */
export type DeleteResult<Schema extends ZodObject> = BaseResult & {
  deletedItem?: Partial<EntitySchema<Schema>> | undefined
  itemCollectionMetrics?: ItemCollectionMetrics
}

/**
 * Command to remove an item from a DynamoDB table.
 *
 * @template Schema - The Zod schema defining the structure of the entity.
 *
 * @example
 * ```typescript
 * import { DynamoTable, DynamoEntity, key, Delete } from 'dynamo-document-builder';
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
 *   }),
 *   partitionKey: user => key('USER', user.userId),
 *   sortKey: () => 'METADATA',
 * });
 *
 * const deleteCommand = new Delete({
 *   key: { userId: 'user123' },
 *   returnValues: 'ALL_OLD',
 * });
 *
 * const { deletedItem } = await userEntity.send(deleteCommand);
 * ```
 */
export class Delete<Schema extends ZodObject>
  implements BaseCommand<DeleteResult<Schema>, Schema>, WriteTransactable<Schema>
{
  #config: DeleteConfig<Schema>

  constructor(config: DeleteConfig<Schema>) {
    this.#config = config
  }

  public async execute(entity: DynamoEntity<Schema>): Promise<DeleteResult<Schema>> {
    const deleteCmd = new DeleteCommand({
      TableName: entity.table.tableName,
      Key: entity.buildPrimaryKey(this.#config.key),
      ReturnValues: this.#config.returnValues,
      ReturnConsumedCapacity: this.#config.returnConsumedCapacity,
      ReturnItemCollectionMetrics: this.#config.returnItemCollectionMetrics,
    })

    const deleteResult = await entity.table.documentClient.send(deleteCmd, {
      abortSignal: this.#config.abortController?.signal,
      requestTimeout: this.#config.timeoutMs,
    })

    let deletedItem: Partial<EntitySchema<Schema>> | undefined
    if (deleteResult.Attributes) {
      if (this.#config.skipValidation) {
        deletedItem = deleteResult.Attributes as Partial<EntitySchema<Schema>>
      } else {
        deletedItem = (await entity.schema
          .partial()
          .parseAsync(deleteResult.Attributes)) as Partial<EntitySchema<Schema>>
      }
    }

    return {
      deletedItem,
      responseMetadata: deleteResult.$metadata,
      consumedCapacity: deleteResult.ConsumedCapacity,
      itemCollectionMetrics: deleteResult.ItemCollectionMetrics,
    }
  }

  public async prepareWriteTransaction(
    entity: DynamoEntity<Schema>,
  ): Promise<TransactWriteOperation> {
    return {
      Delete: {
        TableName: entity.table.tableName,
        Key: entity.buildPrimaryKey(this.#config.key),
      },
    }
  }
}
