import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import type { NamedGlobalSecondaryIndexKeyNames, NamedLocalSecondaryIndexKeyNames } from '@/core'
import type { TableCommand } from '@/commands'

/**
 * Configuration type for creating a DynamoTable.
 *
 * The table is assumed to have a primary key composed of a partition key named `"PK"` and
 * a sort key named `"SK"` by default, unless overridden in the `keyNames` property.
 *
 * @template Schema - The Zod schema representing the entity's structure.
 *
 * @property tableName - The name of the DynamoDB table.
 * @property documentClient - The DynamoDB Document Client instance to use for operations.
 * @property keyNames - Key names object for the table's primary key and secondary indexes.
 */
export type DynamoTableConfig = {
  tableName: string
  documentClient?: DynamoDBDocumentClient
  keyNames?: {
    partitionKey: string
    sortKey?: string | null
    globalSecondaryIndexes?: NamedGlobalSecondaryIndexKeyNames
    localSecondaryIndexes?: NamedLocalSecondaryIndexKeyNames
  }
}

/**
 * Core class that represents a DynamoDB table.
 */
export class DynamoTable {
  #tableName: string
  #documentClient: DynamoDBDocumentClient | undefined

  #pk: string = 'PK'
  #sk: string | null = 'SK'

  #gsi: NamedGlobalSecondaryIndexKeyNames
  #lsi: NamedLocalSecondaryIndexKeyNames

  constructor(config: DynamoTableConfig) {
    this.#tableName = config.tableName
    this.#documentClient = config.documentClient
    if (config.keyNames?.partitionKey !== undefined) {
      this.#pk = config.keyNames.partitionKey
    }
    if (config.keyNames?.sortKey !== undefined) {
      this.#sk = config.keyNames.sortKey
    }
    this.#gsi = config.keyNames?.globalSecondaryIndexes ?? {}
    this.#lsi = config.keyNames?.localSecondaryIndexes ?? {}
  }

  /**
   * The name of the DynamoDB table.
   */
  public get tableName(): string {
    return this.#tableName
  }

  /**
   * The DynamoDB Document Client instance used for operations.
   * @throws Error if no client has been provided via the constructor or `initClient`.
   */
  public get documentClient(): DynamoDBDocumentClient {
    if (this.#documentClient === undefined) {
      throw new Error(
        'documentClient has not been set on this DynamoTable. ' +
          'Provide it in the constructor config or call table.initClient(client) before executing commands.',
      )
    }
    return this.#documentClient
  }

  /**
   * Sets the DynamoDB Document Client for this table, allowing the client
   * to be initialized after table construction.
   */
  public initClient(client: DynamoDBDocumentClient): void {
    this.#documentClient = client
  }

  /**
   * The name of the partition key for the table.
   */
  public get partitionKeyName(): string {
    return this.#pk
  }

  /**
   * The name of the sort key for the table, or `null` if the table does not have a sort key
   * (`null` would indicate a "simple" primary key).
   */
  public get sortKeyName(): string | null {
    return this.#sk
  }

  /**
   * The key names for the global secondary indexes defined on the table.
   */
  public get globalSecondaryIndexKeyNames(): NamedGlobalSecondaryIndexKeyNames {
    return this.#gsi
  }

  /**
   * The key names for the local secondary indexes defined on the table.
   */
  public get localSecondaryIndexKeyNames(): NamedLocalSecondaryIndexKeyNames {
    return this.#lsi
  }

  /**
   * Sends a table-level command to be executed against this table.
   */
  public async send<CommandOutput>(command: TableCommand<CommandOutput>): Promise<CommandOutput> {
    return await command.execute(this)
  }
}
