import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import type {
  NamedGlobalSecondaryIndexKeyNames,
  NamedLocalSecondaryIndexKeyNames,
} from '@/core/core-types'

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

export class DynamoTable {
  #tableName: string
  #documentClient: DynamoDBDocumentClient

  #pk: string
  #sk: string | null

  #gsi: NamedGlobalSecondaryIndexKeyNames
  #lsi: NamedLocalSecondaryIndexKeyNames

  constructor(config: DynamoTableConfig) {
    this.#tableName = config.tableName
    this.#documentClient = config.documentClient
    this.#pk = config.keyNames?.partitionKey ?? 'PK'
    this.#sk = config.keyNames?.sortKey ?? 'SK'
    this.#gsi = config.keyNames?.globalSecondaryIndexes ?? {}
    this.#lsi = config.keyNames?.localSecondaryIndexes ?? {}
  }

  public get tableName(): string {
    return this.#tableName
  }

  public get documentClient(): DynamoDBDocumentClient {
    return this.#documentClient
  }

  public get partitionKeyName(): string {
    return this.#pk
  }

  public get sortKeyName(): string | null {
    return this.#sk
  }

  public get globalSecondaryIndexKeyNames(): NamedGlobalSecondaryIndexKeyNames {
    return this.#gsi
  }

  public get localSecondaryIndexKeyNames(): NamedLocalSecondaryIndexKeyNames {
    return this.#lsi
  }
}
