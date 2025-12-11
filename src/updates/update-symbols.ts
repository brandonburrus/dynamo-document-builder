/**
 * Internal symbols used to identify different types of updates.
 */

export const $set = Symbol('set')
export const $remove = Symbol('remove')
export const $delete = Symbol('delete')
export const $add = Symbol('add')
export const $subtract = Symbol('subtract')
export const $append = Symbol('append')
export const $prepend = Symbol('prepend')
export const $addToSet = Symbol('addToSet')
export const $ref = Symbol('ref')

export function isUpdateSymbol(value: unknown): boolean {
  return (
    typeof value === 'symbol' &&
    [$set, $remove, $delete, $add, $subtract, $append, $prepend, $addToSet, $ref].includes(value)
  )
}
