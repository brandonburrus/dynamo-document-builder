/**
 * Internal symbols used to identify different types of conditions.
 */

export const $comparison = Symbol('comparison')
export const $logical = Symbol('logical')
export const $between = Symbol('between')
export const $in = Symbol('in')
export const $not = Symbol('not')
export const $exists = Symbol('exists')
export const $type = Symbol('type')
export const $beginsWith = Symbol('begins')
export const $contains = Symbol('contains')
export const $size = Symbol('size')

export function isConditionSymbol(value: unknown): boolean {
  return (
    typeof value === 'symbol' &&
    [
      $comparison,
      $logical,
      $between,
      $in,
      $not,
      $exists,
      $type,
      $beginsWith,
      $contains,
      $size,
    ].includes(value)
  )
}
