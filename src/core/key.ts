import { DocumentBuilderError } from '@/errors'
import type { IndexName } from '@/core'

/**
 * Represents a value that can be used as a DynamoDB primary key.
 */
export type DynamoKeyableValue = string | number | Buffer

/**
 * Represents a value that can be used as a DynamoDB secondary index key.
 */
export type DynamoIndexKeyableValue = string | number | Buffer | undefined

/**
 * A function that builds a DynamoDB primary key from an item.
 */
export type DynamoKeyBuilder<Item> = (item: Item) => DynamoKeyableValue

/**
 * A function that builds a DynamoDB secondary index key from an item.
 */
export type DynamoIndexKeyBuilder<Item> = (item: Item) => DynamoIndexKeyableValue

/**
 * A mapping type of global secondary index names to their key builders.
 */
export type GlobalSecondaryIndexKeyBuilders<Item> = Record<
  IndexName,
  {
    partitionKey: DynamoIndexKeyBuilder<Item>
    sortKey?: DynamoIndexKeyBuilder<Item>
  }
>

/**
 * A mapping type of local secondary index names to their key builders.
 */
export type LocalSecondaryIndexKeyBuilders<Item> = Record<
  IndexName,
  {
    sortKey: DynamoIndexKeyBuilder<Item>
  }
>

/**
 * Record type representing a DynamoDB primary key.
 */
export type DynamoKey = Record<string, DynamoKeyableValue>

/**
 * Record type representing a DynamoDB secondary index key.
 */
export type DynamoIndexKey = Record<string, DynamoIndexKeyableValue>

/**
 * Builds a DynamoDB key from the given parts by concatenating them with a `#` separator.
 * At least one part must be provided, or a DocumentBuilderError will be thrown.
 *
 * @param parts - The parts to combine into a key.
 * @returns The combined key as a string.
 *
 * @example
 * ```ts
 * const key1 = key('USER', 123) // 'USER#123'
 * const key2 = key('METADATA') // 'METADATA'
 * ```
 */
export function key(...parts: DynamoKeyableValue[]): string {
  if (!parts.length) {
    throw new DocumentBuilderError('At least one key part must be provided')
  }
  return parts.join('#')
}

/**
 * Builds a DynamoDB key from the given parts. This function is specifically for secondary index keys,
 * where any part can be undefined. If any part is undefined, the function returns undefined.
 *
 * For primary keys, use the `key` function instead.
 *
 * This is specifically useful for building keys for queries on secondary indexes, or for sparse indexes.
 *
 * @param parts - The parts to combine into a key.
 * @returns The combined key as a string, or undefined if any part is undefined.
 *
 * @example
 * ```ts
 * const key1 = indexKey('part1', 'part2', 'part3') // 'part1#part2#part3'
 * const key2 = indexKey('part1', undefined, 'part3') // undefined
 * const key3 = indexKey(undefined, 'part2', 'part3') // undefined
 * const key4 = indexKey('part1', 'part2', undefined) // undefined
 * ```
 */
export function indexKey(...parts: DynamoIndexKeyableValue[]): string | undefined {
  let key: string = ''
  for (const part of parts) {
    if (part === undefined) {
      return undefined
    }
    key += key ? `#${part}` : part
  }
  return key
}
