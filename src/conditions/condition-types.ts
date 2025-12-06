import type { NativeAttributeValue } from '@aws-sdk/lib-dynamodb'
import type {
  $beginsWith,
  $between,
  $comparison,
  $contains,
  $exists,
  $in,
  $logical,
  $not,
  $size,
  $type,
} from '@/conditions/condition-symbols'

/**
 * These are the basic building block types for constructing DynamoDB condition expressions.
 *
 * https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
 */

/**
 * Represends a top-level attribute in a Dynamo item OR a nested attribute using dot notation.
 *
 * {@link https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax}
 */
export type Operand = string

// Basic comparison operators
export type ComparisonOperator = '=' | '<>' | '<' | '<=' | '>' | '>='

// Logical operators for combining conditions
export type LogicalOperator = 'AND' | 'OR'

// DynamoDB attribute data types
export type DynamoAttributeType = 'B' | 'BOOL' | 'BS' | 'L' | 'M' | 'N' | 'NS' | 'NULL' | 'S' | 'SS'

export type ValueExpression = NativeAttributeValue | SizeExpression

// <operand> <comparison-operator> <value>
export type ComparisonExpression = {
  type: typeof $comparison
  operand: Operand
  operator: ComparisonOperator
  value: ValueExpression
}
export type ComparisonExpressionTemplate = Omit<ComparisonExpression, 'operand'>

// <condition1> AND|OR <condition2> AND|OR ...
export type LogicalExpression = {
  type: typeof $logical
  operator: LogicalOperator
  subConditions: ConditionExpressionTemplate[]
}

// <operand> BETWEEN <lowerValue> AND <upperValue>
export type BetweenExpression = {
  type: typeof $between
  operand: Operand
  lowerValue: ValueExpression
  upperValue: ValueExpression
}
export type BetweenExpressionTemplate = Omit<BetweenExpression, 'operand'>

// <operand> IN (<value1>, <value2>, ...)
export type InExpression = {
  type: typeof $in
  operand: Operand
  values: ValueExpression[]
}
export type InExpressionTemplate = Omit<InExpression, 'operand'>

// NOT <condition>
export type NotExpression = {
  type: typeof $not
  condition: ConditionExpressionTemplate
}

// attribute_exists(<operand>) | attribute_not_exists(<operand>)
export type ExistsExpression = {
  type: typeof $exists
  operand: Operand
  not?: true
}
export type ExistsExpressionTemplate = Omit<ExistsExpression, 'operand'>

// attribute_type(<operand>, <type>)
export type TypeCheckExpression = {
  type: typeof $type
  operand: Operand
  attributeType: DynamoAttributeType
}
export type TypeCheckExpressionTemplate = Omit<TypeCheckExpression, 'operand'>

// begins_with(<operand>, <prefix>)
export type BeginsWithExpression = {
  type: typeof $beginsWith
  operand: Operand
  prefix: string
}
export type BeginsWithExpressionTemplate = Omit<BeginsWithExpression, 'operand'>

// contains(<operand>, <substring>|<element>)
export type ContainsExpression = {
  type: typeof $contains
  operand: Operand
  substringOrElement: NativeAttributeValue
}
export type ContainsExpressionTemplate = Omit<ContainsExpression, 'operand'>

// size(<attribute>)
export type SizeExpression = {
  type: typeof $size
  attribute: Operand
}
// NOTE: This one isnt template-able since it always returns a number (just doesnt make sense API-wise)
// If you find a use-case for this, feel free to open an issue or PR!

// Functions that return boolean values
export type BooleanExpression =
  | ExistsExpression
  | TypeCheckExpression
  | BeginsWithExpression
  | ContainsExpression

// All functions
export type FunctionExpression = BooleanExpression | SizeExpression

// All expressions that can be used within a condition template
export type TemplateExpression = Omit<
  ComparisonExpression | BetweenExpression | InExpression | BooleanExpression,
  'operand'
>

/**
 * Implicit AND between all attributes in the template
 * This is an object where keys are operands and values are either values or template expressions
 */
export type ConditionTemplate = Record<Operand, ValueExpression | TemplateExpression>

// All expressions that result in a boolean
export type ConditionExpression =
  | ComparisonExpression
  | LogicalExpression
  | BetweenExpression
  | InExpression
  | NotExpression
  | BooleanExpression

export type ConditionExpressionTemplate = ConditionExpression | ConditionTemplate

export type Condition = ConditionExpressionTemplate | ConditionExpressionTemplate[]
