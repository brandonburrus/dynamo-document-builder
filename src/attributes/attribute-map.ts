import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'

export type AttributeValues = Record<string, NativeAttributeValue>
export type AttributeNames = Record<string, string>

export interface DynamoAttributeExpression {
  ExpressionAttributeValues?: AttributeValues | undefined
  ExpressionAttributeNames?: AttributeNames | undefined
}

export type AttributeValuePlaceholder = `:${string}`
export type AttributeNamePlaceholder = `#${string}`

export class AttributeExpressionMap {
  private attributeNames: Record<AttributeNamePlaceholder, string>
  private reverseLookupNames: Record<string, AttributeNamePlaceholder>
  private names: Set<string>

  private attributeValues: Record<AttributeValuePlaceholder, NativeAttributeValue>
  private reverseLookupValues: Map<NativeAttributeValue, AttributeValuePlaceholder>
  private values: Set<string>

  constructor() {
    this.attributeNames = {}
    this.reverseLookupNames = {}
    this.names = new Set<string>()
    this.attributeValues = {}
    this.reverseLookupValues = new Map<NativeAttributeValue, AttributeValuePlaceholder>()
    this.values = new Set<string>()
  }

  public addName(name: string): AttributeNamePlaceholder {
    if (this.names.has(name)) {
      return this.reverseLookupNames[name]!
    }
    const placeholder: AttributeNamePlaceholder = `#${name}`
    this.attributeNames[placeholder] = name
    this.reverseLookupNames[name] = placeholder
    this.names.add(name)
    return placeholder
  }

  public addValue(value: NativeAttributeValue): AttributeValuePlaceholder {
    if (this.values.has(value)) {
      return this.reverseLookupValues.get(value)!
    }
    const valuePlaceholder: AttributeValuePlaceholder = `:v${this.values.size + 1}`
    this.attributeValues[valuePlaceholder] = value
    this.reverseLookupValues.set(value, valuePlaceholder)
    this.values.add(value)
    return valuePlaceholder
  }

  public add(
    name: string,
    value: NativeAttributeValue,
  ): [AttributeNamePlaceholder, AttributeValuePlaceholder] {
    return [this.addName(name), this.addValue(value)]
  }

  public toDynamoAttributeExpression(): DynamoAttributeExpression {
    const expression: DynamoAttributeExpression = {}
    if (this.names.size > 0) {
      expression.ExpressionAttributeNames = this.attributeNames
    }
    if (this.values.size > 0) {
      expression.ExpressionAttributeValues = this.attributeValues
    }
    return expression
  }

  public hasName(name: string): boolean {
    return this.names.has(name)
  }

  public hasValue(value: NativeAttributeValue): boolean {
    return this.values.has(value)
  }

  public getPlaceholderFromName(name: string): AttributeNamePlaceholder | undefined {
    return this.reverseLookupNames[name]
  }

  public getPlaceholderFromValue(
    value: NativeAttributeValue,
  ): AttributeValuePlaceholder | undefined {
    return this.reverseLookupValues.get(value)
  }

  public getNameFromPlaceholder(placeholder: AttributeNamePlaceholder): string | undefined {
    return this.attributeNames[placeholder]
  }

  public getValueFromPlaceholder(
    placeholder: AttributeValuePlaceholder,
  ): NativeAttributeValue | undefined {
    return this.attributeValues[placeholder]
  }

  public getNameCount(): number {
    return this.names.size
  }

  public getValueCount(): number {
    return this.values.size
  }
}
