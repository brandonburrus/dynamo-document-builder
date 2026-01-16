import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import type { NamedGlobalSecondaryIndexKeyNames, NamedLocalSecondaryIndexKeyNames } from '@/core'

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
  documentClient: DynamoDBDocumentClient
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
  #documentClient: DynamoDBDocumentClient

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
   */
  public get documentClient(): DynamoDBDocumentClient {
    return this.#documentClient
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
}
