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

export type UpdateType = 'SET' | 'REMOVE' | 'ADD' | 'DELETE'

export type ValueReference<ValueType extends NativeAttributeValue = NativeAttributeValue> = {
  type: typeof $ref
  to: string
  default?: ValueType
}

export type RemoveExpression = {
  type: typeof $remove
}

// Delete from SET
export type DeleteExpression = {
  type: typeof $delete
  values: NativeAttributeValue[] | ValueReference
}

export type AddExpression = {
  type: typeof $set
  op: typeof $add
  value: number | ValueReference<number>
}

export type SubtractExpression = {
  type: typeof $set
  op: typeof $subtract
  value: number | ValueReference<number>
}

export type AppendExpression = {
  type: typeof $set
  op: typeof $append
  values: NativeAttributeValue[] | ValueReference
}

export type PrependExpression = {
  type: typeof $set
  op: typeof $prepend
  values: NativeAttributeValue[] | ValueReference
}

export type AddToSetExpression = {
  type: typeof $add
  op: typeof $addToSet
  values: NativeAttributeValue[] | ValueReference
}

export type UpdateExpression =
  | AddExpression
  | SubtractExpression
  | AppendExpression
  | PrependExpression
  | AddToSetExpression
  | DeleteExpression
  | RemoveExpression

export type UpdateValues = Record<string, NativeAttributeValue | UpdateExpression | ValueReference>
