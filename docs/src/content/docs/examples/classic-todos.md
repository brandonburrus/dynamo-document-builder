---
title: Classic To-dos
description: Quick example code of how basic CRUD operations would be implementing using the classic to-dos list as an example using Dynamo Document Builder
---

In this example we'll take a look at how you might implement basic [CRUD](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete) operations using the classic to-dos list example using Document Builder.
We'll examine how you might model your data and then retrieve and update that data with basic CRUD operations.

## Determine your Access Patterns

As with all DynamoDB applications, the first step is to identify the access patterns. For this example, we'll built the following:
- Create new to-do's for a given user
- Get an individual to-do by its ID
- Get all to-dos for a given user by the user's ID
    - Get all to-do's for a user by status (aka completed or not completed)
    - Retrieve user's to-do's either in ascending or descending order of creation date
- Update to-do's to mark them as completed or not completed by ID
- Update to-do's to change their title or description by ID
- Delete to-do's by their ID


## Define your Data Model

Based on our access patterns, we can assume the following simple data model:
```ts
interface Todo {
  todoId: string;
  userId: string;
  title: string;
  description?: string;
  createdAt: Date;
  isCompleted: boolean;
}
```

### Start with your Table

```ts
import { DynamoTable } from 'dynamo-document-builder';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'

const dynamodbClient = new DynamoDBClient({
  // Configure your DynamoDB client here
});

const todosTable = new DynamoTable({
  name: 'TodosTable',
  documentClient: DynamoDBDocumentClient.from(dynamodbClient, {
    // Configure your DynamoDB Document client here
    marshallOptions: {
      removeUndefinedValues: true, // Remove undefined values instead of inserting NULLs
    },
  }),
});
```

Document Builder follows the [Single Table](https://www.alexdebrie.com/posts/dynamodb-single-table-design/) design pattern, so in getting started we only need to define a single table for our to-dos application.

### Add your entities

```ts
import { DynamoEntity, key, isoDatetime, type Entity } from 'dynamo-document-builder';
import { z } from 'zod/v4';

const todoEntity = new DynamoEntity({
  table: todosTable,
  schema: z.object({
    todoId: z.string(),
    userId: z.string(),
    title: z.string(),
    description: z.string().optional(),
    createdAt: isoDatetime().default(() => new Date()),
    isCompleted: z.boolean().default(false),
  }),
  partitionKey: todo => key('USER_TODO', todo.userId),
  sortKey: todo => key('TODO', todo.todoId, 'CREATED_AT', todo.createdAt),
});
type Todo = Entity<typeof todoEntity>;
```

Key points to note here:
- Keys are defined using the built-in `key()` helper function to create composite keys.
  - By default, Document Builder defines the partition key name as `PK` and the sort key name as `SK`.
- The type `Todo` are inferred from the entity schema using the `Entity` utility type.

## Implement CRUD operations

### Create a new To-do

```ts
import { Put } from 'dynamo-document-builder';

async function createNewTodo(
  userId: string,
  title: string,
  description?: string
): Promise<Todo> {
  const putTodo = await todoEntity.send(new Put({
    item: {
      todoId: crypto.randomUUID(),
      userId,
      title,
      description,
    },
    returnValues: 'ALL_NEW',
  }));
  return putTodo.item;
}
```

### Get a To-do by ID

```ts
import { Get } from 'dynamo-document-builder';

async function getTodoById(
  userId: string,
  todoId: string
): Promise<Todo | undefined> {
  const result = await todoEntity.send(new Get({
    key: { userId, todoId },
  }));
  return result.item;
}
```

### Get all To-dos for a user

```ts
import { Query } from 'dynamo-document-builder';

async function getAllTodosForUser(userId: string): Promise<Todo[]> {
  const result = await todoEntity.send(new Query({
    key: { userId },
  }));
  return result.items;
}
```

### Get all To-dos for a user by status

```ts
import { Query } from 'dynamo-document-builder';

async function getTodosByStatus(
  userId: string,
  isCompleted: boolean
): Promise<Todo[]> {
  const result = await todoEntity.send(new Query({
    key: { userId },
    filter: { isCompleted },
  }));
  return result.items;
}
```

### Get all To-dos for a user in ascending or descending order

```ts
import { Query } from 'dynamo-document-builder';

async function getTodosOrderedByCreatedAt(
  userId: string,
  ascending: boolean = true
): Promise<Todo[]> {
  const result = await todoEntity.send(new Query({
    key: { userId },
    reverseIndexScan: !ascending, // false for ascending, true for descending
  }));
  return result.items;
}
```

### Update a To-do's completion status

```ts
import { Update } from 'dynamo-document-builder';

async function updateTodoCompletionStatus(
  userId: string,
  todoId: string,
  isCompleted: boolean
): Promise<Todo> {
  const result = await todoEntity.send(new Update({
    key: { userId, todoId },
    updates: {
      isCompleted,
    },
    returnValues: 'ALL_NEW',
  }));
  return result.item;
}
```

### Update a To-do's title or description

```ts
import { Update } from 'dynamo-document-builder';

async function updateTodoDetails(
  userId: string,
  todoId: string,
  title?: string,
  description?: string
): Promise<Todo> {
  const updates: Record<string, any> = {};
  if (title !== undefined) updates.title = title;
  if (description !== undefined) updates.description = description;

  const result = await todoEntity.send(new Update({
    key: { userId, todoId },
    updates,
    returnValues: 'ALL_NEW',
  }));
  return result.item;
}
```

### Delete a To-do by ID

```ts
import { Delete } from 'dynamo-document-builder';

async function deleteTodo(
  userId: string,
  todoId: string
): Promise<Todo | undefined> {
  const result = await todoEntity.send(new Delete({
    key: { userId, todoId },
    returnValues: 'ALL_OLD',
  }));
  return result.item;
}
```
