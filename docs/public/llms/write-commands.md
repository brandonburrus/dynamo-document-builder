# Write Commands

> All write commands are dispatched via `entity.send(command)`. Input items are validated against the entity's Zod schema before being written.

## Put

Create or replace an item.

```typescript
import { Put, ConditionalPut } from 'dynamo-document-builder';

await todoEntity.send(new Put({
  item: {
    userId: '123',
    todoId: '456',
    title: 'Take out trash',
    isComplete: false,
    createdAt: new Date().toISOString(),
  },
  returnValues: 'ALL_OLD', // optional: return the previous item if it existed
}));

// Conditional put — only write if condition passes
await todoEntity.send(new ConditionalPut({
  item: { /* ... */ },
  condition: { isComplete: false },
  returnValuesOnConditionCheckFailure: 'ALL_OLD',
}));
```

**`Put` options**: `item` (required), `returnValues`, `returnItemCollectionMetrics`, `skipValidation`, `returnConsumedCapacity`

**`ConditionalPut` additional options**: `condition` (required), `returnValuesOnConditionCheckFailure`

## Update

Modify specific attributes of an existing item without replacing it.

```typescript
import { Update, ConditionalUpdate } from 'dynamo-document-builder';
import {
  ref, remove, add, subtract,
  append, prepend, addToSet, removeFromSet,
} from 'dynamo-document-builder';

await todoEntity.send(new Update({
  key: { userId: '123', todoId: '456' },
  updates: {
    // Direct value assignment
    title: 'New title',
    'nested.attribute': 'value',   // dot-notation for nested paths

    // Reference another attribute's current value
    backup: ref('title'),
    valueWithDefault: ref('optional', 'fallback'),

    // Remove an attribute entirely
    obsolete: remove(),

    // Numeric operations
    counter: add(5),
    score: subtract(2),

    // List operations
    tags: append(['newTag']),
    priorities: prepend(['urgent']),

    // Set operations
    categories: addToSet(['category1', 'category2']),
    oldTags: removeFromSet(['deprecated']),
  },
  returnValues: 'ALL_NEW',
}));

// Conditional update
await todoEntity.send(new ConditionalUpdate({
  key: { userId: '123', todoId: '456' },
  updates: { isComplete: true },
  condition: { isComplete: false },
}));
```

### Update Operations Reference

| Operation | Description |
|---|---|
| `value` | Set attribute to a literal value |
| `ref(path, default?)` | Set to the current value of another attribute |
| `remove()` | Delete the attribute |
| `add(n)` | Increment a number by `n` |
| `subtract(n)` | Decrement a number by `n` |
| `append(items)` | Append elements to a list |
| `prepend(items)` | Prepend elements to a list |
| `addToSet(values)` | Add values to a DynamoDB set |
| `removeFromSet(values)` | Remove values from a DynamoDB set |

**`Update` options**: `key` (required), `updates` (required), `returnValues`, `skipValidation`, `returnConsumedCapacity`

**`ConditionalUpdate` additional options**: `condition` (required), `returnValuesOnConditionCheckFailure`

## Delete

Remove an item by primary key.

```typescript
import { Delete, ConditionalDelete } from 'dynamo-document-builder';

await todoEntity.send(new Delete({
  key: { userId: '123', todoId: '456' },
  returnValues: 'ALL_OLD', // optional: return the deleted item
}));

// Conditional delete
await todoEntity.send(new ConditionalDelete({
  key: { userId: '123', todoId: '456' },
  condition: { isComplete: true },
}));
```

**`Delete` options**: `key` (required), `returnValues`, `returnItemCollectionMetrics`, `skipValidation`, `returnConsumedCapacity`

**`ConditionalDelete` additional options**: `condition` (required), `returnValuesOnConditionCheckFailure`

## BatchWrite

Put and/or delete multiple items in one DynamoDB request. At least one of `items` or `deletes` is required.

```typescript
import { BatchWrite } from 'dynamo-document-builder';

const { unprocessedPuts, unprocessedDeletes } = await todoEntity.send(new BatchWrite({
  items: [
    { userId: '123', todoId: '456', title: 'Task 1', isComplete: false, createdAt: '...' },
    { userId: '789', todoId: '101', title: 'Task 2', isComplete: true, createdAt: '...' },
  ],
  deletes: [
    { userId: '111', todoId: '222' },
  ],
}));

// Retry unprocessed items with exponential backoff
```

**Options**: `items`, `deletes` (at least one required), `returnItemCollectionMetrics`, `skipValidation`, `returnConsumedCapacity`

## TransactWrite

Atomic, all-or-nothing multi-item write. Supports up to 100 operations across Put, Update, Delete, and ConditionCheck.

```typescript
import { TransactWrite, ConditionCheck, Put, Update, Delete } from 'dynamo-document-builder';

await todoEntity.send(new TransactWrite({
  writes: [
    new Put({ item: { /* ... */ } }),
    new Update({ key: { /* ... */ }, updates: { isComplete: true } }),
    new Delete({ key: { /* ... */ } }),
    new ConditionCheck({
      key: { userId: '123', todoId: '456' },
      condition: { isComplete: false },
    }),
  ],
  idempotencyToken: 'unique-idempotency-token', // optional: prevents duplicate execution
}));
```

**Supported write types**: `Put`, `ConditionalPut`, `Update`, `ConditionalUpdate`, `Delete`, `ConditionalDelete`, `ConditionCheck`

**Options**: `writes` (required), `idempotencyToken`, `returnItemCollectionMetrics`, `skipValidation`, `returnConsumedCapacity`

## Error Handling

Conditional commands throw `ConditionalCheckFailedException` when the condition is not satisfied.

```typescript
try {
  await todoEntity.send(new ConditionalPut({
    item: newItem,
    condition: { isComplete: false },
  }));
} catch (error) {
  if (error.name === 'ConditionalCheckFailedException') {
    // Handle condition failure
  }
}
```
