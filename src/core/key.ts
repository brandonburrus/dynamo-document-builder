import type { IndexName } from './core-types'

export type DynamoKeyableValue = string | number | Buffer
export type DynamoKeyBuilder<Item> = (item: Item) => DynamoKeyableValue

export type GlobalSecondaryIndexKeyBuilders<Item> = Record<
  IndexName,
  {
    partitionKey: DynamoKeyBuilder<Item>
    sortKey?: DynamoKeyBuilder<Item>
  }
>

export type LocalSecondaryIndexKeyBuilders<Item> = Record<
  IndexName,
  {
    sortKey: DynamoKeyBuilder<Item>
  }
>

export type DynamoKey = Record<string, DynamoKeyableValue>

export function key(...parts: DynamoKeyableValue[]): string {
  return parts.join('#')
}
