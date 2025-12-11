import { describe, expect, it } from 'vitest'
import {
  $set,
  $remove,
  $delete,
  $add,
  $subtract,
  $append,
  $prepend,
  $addToSet,
  $ref,
  isUpdateSymbol,
} from '@/updates/update-symbols'

describe('update-symbols', () => {
  describe('isUpdateSymbol', () => {
    it('should return true for $set symbol', () => {
      expect(isUpdateSymbol($set)).toBe(true)
    })

    it('should return true for $remove symbol', () => {
      expect(isUpdateSymbol($remove)).toBe(true)
    })

    it('should return true for $delete symbol', () => {
      expect(isUpdateSymbol($delete)).toBe(true)
    })

    it('should return true for $add symbol', () => {
      expect(isUpdateSymbol($add)).toBe(true)
    })

    it('should return true for $subtract symbol', () => {
      expect(isUpdateSymbol($subtract)).toBe(true)
    })

    it('should return true for $append symbol', () => {
      expect(isUpdateSymbol($append)).toBe(true)
    })

    it('should return true for $prepend symbol', () => {
      expect(isUpdateSymbol($prepend)).toBe(true)
    })

    it('should return true for $addToSet symbol', () => {
      expect(isUpdateSymbol($addToSet)).toBe(true)
    })

    it('should return true for $ref symbol', () => {
      expect(isUpdateSymbol($ref)).toBe(true)
    })

    it('should return false for a string', () => {
      expect(isUpdateSymbol('not a symbol')).toBe(false)
    })

    it('should return false for a number', () => {
      expect(isUpdateSymbol(123)).toBe(false)
    })

    it('should return false for null', () => {
      expect(isUpdateSymbol(null)).toBe(false)
    })

    it('should return false for undefined', () => {
      expect(isUpdateSymbol(undefined)).toBe(false)
    })

    it('should return false for an object', () => {
      expect(isUpdateSymbol({})).toBe(false)
    })

    it('should return false for an array', () => {
      expect(isUpdateSymbol([])).toBe(false)
    })

    it('should return false for a different symbol', () => {
      const differentSymbol = Symbol('different')
      expect(isUpdateSymbol(differentSymbol)).toBe(false)
    })
  })
})
