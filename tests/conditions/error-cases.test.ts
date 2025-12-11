import { describe, expect, it } from 'vitest'
import { InvalidConditionDocumentBuilderError, parseCondition } from '@/conditions/condition-parser'
import { $in, $logical } from '@/conditions/condition-symbols'

describe('condition parser error cases', () => {
  it('throws error for logical expression with no sub-conditions', () => {
    const condition = {
      type: $logical,
      operator: 'AND',
      subConditions: [],
    } as any

    expect(() => parseCondition(condition)).toThrow(InvalidConditionDocumentBuilderError)
    expect(() => parseCondition(condition)).toThrow(
      'Logical expression must have at least one sub-condition',
    )
  })

  it('throws error for OR expression with no sub-conditions', () => {
    const condition = {
      type: $logical,
      operator: 'OR',
      subConditions: [],
    } as any

    expect(() => parseCondition(condition)).toThrow(InvalidConditionDocumentBuilderError)
    expect(() => parseCondition(condition)).toThrow(
      'Logical expression must have at least one sub-condition',
    )
  })

  it('throws error for IN expression with no values', () => {
    const condition = {
      name: {
        type: $in,
        values: [],
      },
    }

    expect(() => parseCondition(condition)).toThrow(InvalidConditionDocumentBuilderError)
    expect(() => parseCondition(condition)).toThrow('IN expression must have at least one value')
  })

  it('handles condition template being treated as such (not error case)', () => {
    // Objects without 'type' property are treated as ConditionTemplate
    // This is actually valid and should work, not throw an error
    const condition = {
      name: 'Test',
      id: '123',
    }

    const result = parseCondition(condition)
    expect(result.conditionExpression).toContain('#name = :v1')
    expect(result.conditionExpression).toContain('#id = :v2')
    expect(result.conditionExpression).toContain(' AND ')
  })

  it('throws error for unknown expression type', () => {
    const condition = {
      name: {
        type: Symbol('unknown'),
      },
    } as any

    expect(() => parseCondition(condition)).toThrow(InvalidConditionDocumentBuilderError)
    expect(() => parseCondition(condition)).toThrow('Unknown expression type')
  })

  it('validates InvalidConditionDocumentBuilderError has correct name', () => {
    const error = new InvalidConditionDocumentBuilderError('test message')
    expect(error.name).toBe('InvalidConditionDocumentBuilderError')
    expect(error.message).toBe('Invalid Condition: test message')
  })

  it('handles isConditionTemplate with null value', () => {
    const condition = null as any

    expect(() => parseCondition(condition)).toThrow()
  })

  it('handles isConditionTemplate with non-object value', () => {
    const condition = 'not an object' as any

    expect(() => parseCondition(condition)).toThrow()
  })

  it('handles array with non-template expression', () => {
    // Test array with a direct expression (not template)
    const condition = [
      {
        type: $logical,
        operator: 'AND',
        subConditions: [{ name: 'Test' }],
      },
    ] as any

    const result = parseCondition(condition)
    expect(result.conditionExpression).toBeTruthy()
  })

  it('throws error for expression with non-symbol type', () => {
    // Create an object with a non-symbol type property
    // This bypasses isConditionTemplate check (line 215-216)
    // and hits parseConditionExpression which throws on unknown type
    const condition = {
      name: {
        type: 'not-a-symbol', // Not a symbol, so passes isConditionTemplate check
        operand: 'name',
        value: 'test',
      } as any,
    }

    // This should be treated as a template with value having type: 'not-a-symbol'
    // parseConditionExpression will throw "Unknown expression type"
    expect(() => parseCondition(condition)).toThrow(InvalidConditionDocumentBuilderError)
    expect(() => parseCondition(condition)).toThrow('Unknown expression type')
  })
})
