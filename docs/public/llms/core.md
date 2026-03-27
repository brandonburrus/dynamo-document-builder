# Core Concepts

> Tables and Entities are the two foundational primitives. A Table wraps a DynamoDB table; an Entity defines a typed, schema-validated data model that belongs to that table. Commands are dispatched through entities (or the table for multi-entity operations).

## Installation

```bash
npm i dynamo-document-builder zod @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

## Tables

A `DynamoTable` represents one DynamoDB table. Construct it once (outside Lambda handlers) and share it across all entities that belong to that table.

```typescript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoTable } from 'dynamo-document-builder';

const client = DynamoDBDocumentClient.from(new DynamoDBClient());

const myTable = new DynamoTable({
  tableName: 'MyTable',
  documentClient: client,
  keyNames: {
    partitionKey: 'PK',
    sortKey: 'SK',
  },
});
```

**`keyNames` options**:
- `partitionKey`: Partition key attribute name (default: `'PK'`)
- `sortKey`: Sort key attribute name (default: `'SK'`, set to `null` for simple key tables)
- `globalSecondaryIndexes`: Map of GSI name → `{ partitionKey, sortKey? }`
- `localSecondaryIndexes`: Map of LSI name → `{ sortKey }`

## Entities

A `DynamoEntity` defines a data model, its Zod schema, and optional computed key functions. The schema drives both runtime validation and TypeScript type inference.

```typescript
import { DynamoEntity, key, type Entity } from 'dynamo-document-builder';
import { z } from 'zod';

const todoEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    todoId: z.string(),
    userId: z.string(),
    title: z.string(),
    isComplete: z.boolean().default(false),
    createdAt: z.iso.datetime(),
  }),
  partitionKey: todo => key('USER', todo.userId, 'TODO', todo.todoId),
  sortKey: todo => key('CREATED_AT', todo.createdAt),
});

type Todo = Entity<typeof todoEntity>;
```

**Entity configuration**:
- `table`: Parent `DynamoTable` (required)
- `schema`: Zod object schema (required)
- `partitionKey`: `(item) => string` — computes the PK from item data (optional)
- `sortKey`: `(item) => string` — computes the SK from item data (optional)
- `globalSecondaryIndexes`: Map of GSI name → `{ partitionKey, sortKey? }` builder functions
- `localSecondaryIndexes`: Map of LSI name → `{ sortKey }` builder function

**Key behaviours**:
- `key('PREFIX', value, ...)` joins parts with `'#'` — e.g. `key('USER', '123')` → `'USER#123'`
- PK/SK attributes are automatically added on write and stripped on read (schema validation removes them)
- Schema validation runs on all read results by default; disable per-command with `skipValidation: true`

## Sparse Indexes

Return `undefined` from a GSI/LSI key builder to exclude an item from that index. Use `indexKey()` which automatically returns `undefined` if any input value is undefined.

```typescript
import { indexKey } from 'dynamo-document-builder';

const articleEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    id: z.string(),
    title: z.string(),
    publishedAt: z.string().optional(),
  }),
  partitionKey: item => key('ARTICLE', item.id),
  sortKey: () => 'METADATA',
  globalSecondaryIndexes: {
    PublishedIndex: {
      partitionKey: item => indexKey('PUBLISHED_AT', item.publishedAt),
      sortKey: item => indexKey('ARTICLE', item.id),
    },
  },
});
```

If the GSI partition key builder returns `undefined`, the sort key builder is not called.

## Dispatching Commands

```typescript
// Single-entity command
const { item } = await todoEntity.send(new Get({ key: { userId: '1', todoId: '2' } }));

// Multi-entity command — bind each command to its entity first, then send via the table
const result = await myTable.send(
  new TableBatchGet({
    gets: [
      todoEntity.prepare(new BatchGet({ keys: [{ userId: '1', todoId: '2' }] })),
      userEntity.prepare(new BatchGet({ keys: [{ userId: '1' }] })),
    ],
  }),
);
```

`entity.prepare(command)` binds a command to an entity without executing it, producing an input accepted by table-level commands (`TableBatchGet`, `TableBatchWrite`, `TableTransactGet`, `TableTransactWrite`).

## Type Utilities

```typescript
import { type Entity, type EncodedEntity } from 'dynamo-document-builder';

// Domain type — what your application works with
type Todo = Entity<typeof todoEntity>;

// Pre-codec type — what is actually stored in DynamoDB (relevant when using Zod codecs)
type EncodedTodo = EncodedEntity<typeof todoEntity>;
```
