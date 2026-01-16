# Dynamo Document Builder

DynamoDB single table design and data validation made easy using TypeScript and Zod ⚡️

Full documentation can be found at [dynamodocumentbuilder.com](https://dynamodocumentbuilder.com).

## Features

- Data integraty using [Zod](https://zod.dev/) for schema validation
- Follows [Single Table Design](https://www.alexdebrie.com/posts/dynamodb-single-table) principles
- Easy ergonomic API for a better DynamoDB developer experience
- Built from the group up to be type-safe
- Completely tree-shakable for minimal bundle sizes
- Extensively documented with guides, example code, and complete API reference

## Installation

```bash
npm i dynamo-document-builder zod @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

## Getting Started

```typescript
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { DynamoTable, DynamoEntity, type Entity, Put, Get } from 'dynamo-document-builder';
import { z } from 'zod';

// Define you DynamoDB clients
const dynamoDbClient = new DynamoDBClient();
const docClient = DynamoDBDocumentClient.from(dynamoDbClient);

// Define your table
const myTable = new DynamoTable({
  tableName: 'MyDynamoTable',
  documentClient: docClient,
  keyNames: {
    partitionKey: 'PK',
    sortKey: 'SK',
  },
});

// Define an entity that belongs to the table
const userEntity = new DynamoEntity({
  table: myTable,
  schema: z.object({
    id: z.string(),
    name: z.string(),
    email: z.string().email(),
  }),
  partitionKey: user => `USER#${user.id}`,
  sortKey: user => `EMAIL#${user.email}`,
});

// Infer the TypeScript type of the entity from the schema
type User = Entity<typeof userEntity>;

// Create a new item
const newUser: User = {
  id: '123',
  name: 'John Doe',
  email: 'johndoe@example.com',
};

// Put the item to DynamoDB
await userEntity.send(new Put({
  item: newUser, // Validated against the Zod schema
}))

// Get the item back from DynamoDB
const retrievedUser = await userEntity.send(new Get({
  // PK and SK auto-built from the key functions in the Entity
  key: {
    id: '123',
    email: 'johndoe@example.com',
  },
}));
```

## Examples

### Get

Retrieve a single item by primary key:

```typescript
import { Get } from 'dynamo-document-builder';

// Basic get
const { item } = await userEntity.send(new Get({
  key: {
    id: '123',
    email: 'johndoe@example.com',
  },
}));

// Strongly consistent read
const { item } = await userEntity.send(new Get({
  key: {
    id: '123',
    email: 'johndoe@example.com',
  },
  consistent: true,
}));
```

### Put

Create or replace an item:

```typescript
import { Put, ConditionalPut } from 'dynamo-document-builder';

// Basic put
await userEntity.send(new Put({
  item: {
    id: '123',
    name: 'John Doe',
    email: 'johndoe@example.com',
  },
}));

// Put with condition (only if item doesn't exist)
await userEntity.send(new ConditionalPut({
  item: {
    id: '456',
    name: 'Jane Smith',
    email: 'janesmith@example.com',
  },
  condition: { id: notExists() },
}));
```

### Update

Modify existing item attributes:

```typescript
import { Update } from 'dynamo-document-builder';
import { add, append } from 'dynamo-document-builder';

await userEntity.send(new Update({
  key: { id: '123', email: 'johndoe@example.com' },
  updates: {
    name: 'John D. Doe',
    loginCount: add(1),
    tags: append(['premium']),
  },
  returnValues: 'ALL_NEW',
}));
```

### Delete

Remove an item:

```typescript
import { Delete, ConditionalDelete } from 'dynamo-document-builder';

// Basic delete
await userEntity.send(new Delete({
  key: {
    id: '123',
    email: 'johndoe@example.com',
  },
}));

// Conditional delete
await userEntity.send(new ConditionalDelete({
  key: {
    id: '456',
    email: 'janesmith@example.com',
  },
  condition: {
    status: 'inactive',
  },
}));
```

### Query

Retrieve multiple items by partition key:

```typescript
import { Query } from 'dynamo-document-builder';
import { beginsWith, greaterThan } from 'dynamo-document-builder';

// Query all items with a specific partition key
const { items } = await userEntity.send(new Query({
  key: {
    id: '123',
  },
}));

// Query with sort key condition
const { items, lastEvaluatedKey } = await userEntity.send(new Query({
  key: {
    id: '123',
  },
  sortKeyCondition: {
    SK: beginsWith('EMAIL#'),
  },
  limit: 10,
}));

// Paginate through results
for await (const page of userEntity.paginate(new Query({
  key: {
    id: '123',
  },
  pageSize: 50,
}))) {
  console.log(`Found ${page.count} items`);
  processItems(page.items);
}
```

### Scan

Scan entire table (use sparingly, this an expensive operation):

```typescript
import { Scan } from 'dynamo-document-builder';

// Full table scan
const { items, scannedCount } = await userEntity.send(new Scan());

// Scan with filter and limit
const { items, scannedCount } = await userEntity.send(new Scan({
  filter: {
    status: 'active',
  },
  limit: 100,
}));

// Parallel scan
async function parallelScan(totalSegments: number) {
  const scanPromises = Array.from({ length: totalSegments }, (_, i) =>
    userEntity.send(new Scan({
      segment: i,
      totalSegments: totalSegments,
    }))
  );
  const results = await Promise.all(scanPromises);
  return results.flatMap(r => r.items);
}
```

### BatchGet

Retrieve multiple items by primary keys:

```typescript
import { BatchGet } from 'dynamo-document-builder';

const { items, unprocessedKeys } = await userEntity.send(new BatchGet({
  keys: [
    { id: '123', email: 'johndoe@example.com' },
    { id: '456', email: 'janesmith@example.com' },
    { id: '789', email: 'bobwilson@example.com' },
  ],
}));

// Handle unprocessed keys
if (unprocessedKeys?.length) {
  // Retry
  await sleep(100);
  const { items: retryItems } = await userEntity.send(new BatchGet({
    keys: unprocessedKeys,
  }));
}
```

### BatchWrite

Put and/or delete multiple items:

```typescript
import { BatchWrite } from 'dynamo-document-builder';

const { unprocessedPuts, unprocessedDeletes } = await userEntity.send(new BatchWrite({
  items: [
    { id: '123', name: 'User 1', email: 'user1@example.com' },
    { id: '456', name: 'User 2', email: 'user2@example.com' },
  ],
  deletes: [
    { id: '789', email: 'user3@example.com' },
  ],
}));
```

### TransactGet

Transactional read of multiple items:

```typescript
import { TransactGet } from 'dynamo-document-builder';

const { items } = await userEntity.send(new TransactGet({
  keys: [
    { id: '123', email: 'johndoe@example.com' },
    { id: '456', email: 'janesmith@example.com' },
  ],
}));

// Items array has same order as keys
// Undefined if item not found
if (items[0]) {
  console.log('First user:', items[0].name);
}
```

### TransactWrite

Atomic multi-item write transaction:

```typescript
import { TransactWrite, ConditionCheck } from 'dynamo-document-builder';
import { Put, Update, Delete } from 'dynamo-document-builder';
import { add, notExists } from 'dynamo-document-builder';

await userEntity.send(new TransactWrite({
  writes: [
    // Create new user
    new Put({ 
      item: { 
        id: '999', 
        name: 'New User', 
        email: 'newuser@example.com' 
      } 
    }),
    // Update existing user
    new Update({ 
      key: { id: '123', email: 'johndoe@example.com' },
      updates: { loginCount: add(1) }
    }),
    // Delete inactive user
    new Delete({ 
      key: { id: '456', email: 'janesmith@example.com' } 
    }),
    // Verify another user exists
    new ConditionCheck({
      key: { id: '789', email: 'admin@example.com' },
      condition: { role: 'admin' },
    }),
  ],
}));
```
