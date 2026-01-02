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
