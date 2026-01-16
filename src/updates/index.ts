import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type {
  $set,
  $remove,
  $delete,
  $add,
  $subtract,
  $append,
  $addToSet,
  $prepend,
  $ref,
} from '@/updates/update-symbols'

/**
 * Type representing the different kinds of update operations.
 */
export type UpdateType = 'SET' | 'REMOVE' | 'ADD' | 'DELETE'

/**
 * Type representing a reference to another attribute's value.
 */
export type ValueReference<ValueType extends NativeAttributeValue = NativeAttributeValue> = {
  type: typeof $ref
  to: string
  default?: ValueType
}

/**
 * Update expression from a REMOVE operation
 */
export type RemoveExpression = {
  type: typeof $remove
}

/**
 * Update expression from a DELETE operation
 */
export type DeleteExpression = {
  type: typeof $delete
  values: NativeAttributeValue[] | ValueReference
}

/**
 * Update expression from an ADD operation
 */
export type AddExpression = {
  type: typeof $set
  op: typeof $add
  value: number | ValueReference<number>
}

/**
 * Update expression from a SUBTRACT operation
 */
export type SubtractExpression = {
  type: typeof $set
  op: typeof $subtract
  value: number | ValueReference<number>
}

/**
 * Update expression from an APPEND operation
 */
export type AppendExpression = {
  type: typeof $set
  op: typeof $append
  values: NativeAttributeValue[] | ValueReference
}

/**
 * Update expression from a PREPEND operation
 */
export type PrependExpression = {
  type: typeof $set
  op: typeof $prepend
  values: NativeAttributeValue[] | ValueReference
}

/**
 * Update expression from an ADD TO SET operation
 */
export type AddToSetExpression = {
  type: typeof $add
  op: typeof $addToSet
  values: NativeAttributeValue[] | ValueReference
}

/**
 * Union type of all possible update expressions.
 */
export type UpdateExpression =
  | AddExpression
  | SubtractExpression
  | AppendExpression
  | PrependExpression
  | AddToSetExpression
  | DeleteExpression
  | RemoveExpression

/**
 * Type representing an update operation for an entity.
 */
export type UpdateValues = Record<string, NativeAttributeValue | UpdateExpression | ValueReference>

export * from './update-parser'

export * from './ref'
export * from './remove'
export * from './add'
export * from './subtract'
export * from './append'
export * from './prepend'
export * from './add-to-set'
export * from './remove-from-set'
