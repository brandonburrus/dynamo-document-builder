import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'

export interface DynamoTableConfig {
  tableName: string
  documentClient: DynamoDBDocumentClient
  keyNames?: {
    partitionKey: string
    sortKey?: string
  }
}

/**
 * TODO: Add documentation
 */
export class DynamoTable {
  #tableName: string
  #documentClient: DynamoDBDocumentClient
  #partitionKey: string
  #sortKey: string

  constructor(config: DynamoTableConfig) {
    this.#tableName = config.tableName
    this.#documentClient = config.documentClient
    this.#partitionKey ??= config.keyNames?.partitionKey ?? 'PK'
    this.#sortKey ??= config.keyNames?.sortKey ?? 'SK'
  }

  public get tableName(): string {
    return this.#tableName
  }

  public get documentClient(): DynamoDBDocumentClient {
    return this.#documentClient
  }

  public get partitionKeyName(): string {
    return this.#partitionKey
  }

  public get sortKeyName(): string {
    return this.#sortKey
  }
}
