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
 * See: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
 */

/**
 * Represends a top-level attribute in a Dynamo item OR a nested attribute using dot notation.
 *
 * {@link https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html#Expressions.OperatorsAndFunctions.Syntax}
 */
export type Operand = string

/**
 * Type for representing comparison operators in DynamoDB condition expressions.
 */
export type ComparisonOperator = '=' | '<>' | '<' | '<=' | '>' | '>='

/**
 * Type for representing logical operators for combining two or more conditions in DynamoDB condition expressions.
 */
export type LogicalOperator = 'AND' | 'OR'

/**
 * Type for representing DynamoDB attribute type descriptors used to check attribute types in condition expressions.
 */
export type DynamoAttributeType = 'B' | 'BOOL' | 'BS' | 'L' | 'M' | 'N' | 'NS' | 'NULL' | 'S' | 'SS'

/**
 * Value expressions can be either native attribute values or size expressions in the context of a condition expression.
 */
export type ValueExpression = NativeAttributeValue | SizeExpression

/**
 * Represents a comparison between an operand and a value using a comparison operator.
 */
export type ComparisonExpression = {
  type: typeof $comparison
  operand: Operand
  operator: ComparisonOperator
  value: ValueExpression
}

/**
 * Same as ComparisonExpression but without the operand (for use in templates)
 */
export type ComparisonExpressionTemplate = Omit<ComparisonExpression, 'operand'>

/**
 * Represents a logical combination of multiple condition expressions using a logical operator.
 */
export type LogicalExpression = {
  type: typeof $logical
  operator: LogicalOperator
  subConditions: ConditionExpressionTemplate[]
}

/**
 * Represents a BETWEEN expression: `<operand>` BETWEEN `<lowerValue>` AND `<upperValue>`
 */
export type BetweenExpression = {
  type: typeof $between
  operand: Operand
  lowerValue: ValueExpression
  upperValue: ValueExpression
}

/**
 * Same as BetweenExpression but without the operand (for use in templates)
 */
export type BetweenExpressionTemplate = Omit<BetweenExpression, 'operand'>

/**
 * Represents an IN expression: `<operand>` IN (`<value1>`, `<value2>`, ...)
 */
export type InExpression = {
  type: typeof $in
  operand: Operand
  values: ValueExpression[]
}

/**
 * Same as InExpression but without the operand (for use in templates)
 */
export type InExpressionTemplate = Omit<InExpression, 'operand'>

/**
 * Represents a NOT expression that negates a condition expression.
 */
export type NotExpression = {
  type: typeof $not
  condition: ConditionExpressionTemplate
}

/**
 * Represents an EXISTS expression: attribute_exists(`<operand>`) or attribute_not_exists(`<operand>`)
 */
export type ExistsExpression = {
  type: typeof $exists
  operand: Operand
  not?: true
}

/**
 * Same as ExistsExpression but without the operand (for use in templates)
 */
export type ExistsExpressionTemplate = Omit<ExistsExpression, 'operand'>

/**
 * Represents a TYPE check expression: attribute_type(`<operand>`, `<attributeType>`)
 */
export type TypeCheckExpression = {
  type: typeof $type
  operand: Operand
  attributeType: DynamoAttributeType
}

/**
 * Same as TypeCheckExpression but without the operand (for use in templates)
 */
export type TypeCheckExpressionTemplate = Omit<TypeCheckExpression, 'operand'>

/**
 * Represents a BEGINS WITH expression: begins_with(`<operand>`, `<prefix>`)
 */
export type BeginsWithExpression = {
  type: typeof $beginsWith
  operand: Operand
  prefix: string
}

/**
 * Same as BeginsWithExpression but without the operand (for use in templates)
 */
export type BeginsWithExpressionTemplate = Omit<BeginsWithExpression, 'operand'>

/**
 * Represents a CONTAINS expression: contains(`<operand>`, `<substringOrElement>`)
 */
export type ContainsExpression = {
  type: typeof $contains
  operand: Operand
  substringOrElement: NativeAttributeValue
}

/**
 * Same as ContainsExpression but without the operand (for use in templates)
 */
export type ContainsExpressionTemplate = Omit<ContainsExpression, 'operand'>

/**
 * Represents a SIZE expression: size(`<operand>`)
 */
export type SizeExpression = {
  type: typeof $size
  attribute: Operand
}

/**
 * Represents a SIZE condition expression: size(`<operand>`) `<operator>` `<value>`
 */
export type SizeConditionExpression = {
  type: typeof $size
  operand: Operand
  operator: ComparisonOperator
  value: NativeAttributeValue
}

/**
 * Same as SizeConditionExpression but without the operand (for use in templates)
 */
export type SizeConditionExpressionTemplate = Omit<SizeConditionExpression, 'operand'>

/**
 * All functions that evaluate to a boolean value in condition expressions
 */
export type BooleanExpression =
  | ExistsExpression
  | TypeCheckExpression
  | BeginsWithExpression
  | ContainsExpression

/**
 * All function expressions that return a value
 */
export type FunctionExpression = BooleanExpression | SizeExpression

/**
 * All expressions that can be used within a condition template
 */
export type TemplateExpression = Omit<
  | ComparisonExpression
  | BetweenExpression
  | InExpression
  | BooleanExpression
  | SizeConditionExpression,
  'operand'
>

/**
 * Implicit AND between all attributes in the template
 * This is an object where keys are operands and values are either values or template expressions
 */
export type ConditionTemplate = Record<Operand, ValueExpression | TemplateExpression>

/**
 * All expressions that result in a boolean
 */
export type ConditionExpression =
  | ComparisonExpression
  | LogicalExpression
  | BetweenExpression
  | InExpression
  | NotExpression
  | BooleanExpression
  | SizeConditionExpression

/**
 * A condition template or a condition expression.
 * - A template is an object where keys are operands and values are either values or template expressions.
 * - An expression is a structured representation of a condition using the defined expression types.
 */
export type ConditionExpressionTemplate = ConditionExpression | ConditionTemplate

/**
 * A condition can be a single condition expression/template or an array of them (implicitly ANDed)
 */
export type Condition = ConditionExpressionTemplate | ConditionExpressionTemplate[]

export * from './condition-parser'

export * from './and'
export * from './begins-with'
export * from './between'
export * from './contains'
export * from './equals'
export * from './exists'
export * from './greater-than'
export * from './greater-than-or-equal'
export * from './is-in'
export * from './less-than'
export * from './less-than-or-equal'
export * from './not'
export * from './not-equals'
export * from './not-exists'
export * from './or'
export * from './size'
export * from './type-is'
