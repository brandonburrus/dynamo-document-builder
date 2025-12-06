export type DynamoKeyableValue = string | number | Buffer
export type DynamoKeyBuilder<Item> = (item: Item) => DynamoKeyableValue
export type DynamoPrimaryKey = Record<string, DynamoKeyableValue>

export function key(...parts: DynamoKeyableValue[]): string {
  return parts.join('#')
}
