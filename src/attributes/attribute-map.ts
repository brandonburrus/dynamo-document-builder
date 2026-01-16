import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'

/**
 * Record type for DynamoDB attribute values
 */
export type AttributeValues = Record<string, NativeAttributeValue>

/**
 * Record type for DynamoDB attribute names
 */
export type AttributeNames = Record<string, string>

/**
 * Type representing DynamoDB attribute expression
 */
export type DynamoAttributeExpression = {
  ExpressionAttributeValues?: AttributeValues | undefined
  ExpressionAttributeNames?: AttributeNames | undefined
}

/**
 * Type representing a placeholder for an attribute expression value
 */
export type AttributeValuePlaceholder = `:${string}`

/**
 * Type representing a placeholder for an attribute expression name
 */
export type AttributeNamePlaceholder = `#${string}`

/**
 * Class to manage names and values for  DynamoDB attribute expressions
 */
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

  /**
   * Adds an attribute name to the map and returns its placeholder
   * @param name - The attribute name to add
   * @returns The placeholder for the attribute name
   */
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

  /**
   * Adds an attribute value to the map and returns its placeholder
   * @param value - The attribute value to add
   * @returns The placeholder for the attribute value
   */
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

  /**
   * Adds both an attribute name and value to the map and returns their placeholders
   * @param name - The attribute name to add
   * @param value - The attribute value to add
   * @returns A tuple containing the placeholders for the attribute name and value
   */
  public add(
    name: string,
    value: NativeAttributeValue,
  ): [AttributeNamePlaceholder, AttributeValuePlaceholder] {
    return [this.addName(name), this.addValue(value)]
  }

  /**
   * Converts the attribute names to DynamoDB format
   */
  public toDynamoAttributeNames(): AttributeNames {
    return this.attributeNames
  }

  /**
   * Converts the attribute values to DynamoDB format
   */
  public toDynamoAttributeValues(): AttributeValues {
    return this.attributeValues
  }

  /**
   * Converts both attribute names and values to DynamoDB format
   */
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

  /**
   * Checks if the map contains a specific attribute name
   */
  public hasName(name: string): boolean {
    return this.names.has(name)
  }

  /**
   * Checks if the map contains a specific attribute value
   */
  public hasValue(value: NativeAttributeValue): boolean {
    return this.values.has(value)
  }

  /**
   * Retrieves the placeholder for a given attribute name, or `undefined` if not found
   */
  public getPlaceholderFromName(name: string): AttributeNamePlaceholder | undefined {
    return this.reverseLookupNames[name]
  }

  /**
   * Retrieves the placeholder for a given attribute value, or `undefined` if not found
   */
  public getPlaceholderFromValue(
    value: NativeAttributeValue,
  ): AttributeValuePlaceholder | undefined {
    return this.reverseLookupValues.get(value)
  }

  /**
   * Retrieves the attribute name for a given placeholder, or `undefined` if not found
   */
  public getNameFromPlaceholder(placeholder: AttributeNamePlaceholder): string | undefined {
    return this.attributeNames[placeholder]
  }

  /**
   * Retrieves the attribute value for a given placeholder, or `undefined` if not found
   */
  public getValueFromPlaceholder(
    placeholder: AttributeValuePlaceholder,
  ): NativeAttributeValue | undefined {
    return this.attributeValues[placeholder]
  }

  /**
   * Gets the count of unique attribute names in the map
   */
  public getNameCount(): number {
    return this.names.size
  }

  /**
   * Gets the count of unique attribute values in the map
   */
  public getValueCount(): number {
    return this.values.size
  }
}
