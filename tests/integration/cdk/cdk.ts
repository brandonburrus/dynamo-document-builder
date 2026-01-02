import * as cdk from 'aws-cdk-lib'
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb'
import { STS } from '@aws-sdk/client-sts'

type TestingTableProps = dynamodb.TableProps & {
  gsi?: dynamodb.GlobalSecondaryIndexProps[]
  lsi?: dynamodb.LocalSecondaryIndexProps[]
}

class TestingTable extends dynamodb.Table {
  constructor(stack: cdk.Stack, id: string, props: TestingTableProps = {}) {
    super(stack, id, {
      tableName: id,
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      deletionProtection: false,
      ...props,
    })
    if (props.gsi) {
      for (const gsi of props.gsi) {
        this.addGlobalSecondaryIndex(gsi)
      }
    }
    if (props.lsi) {
      for (const lsi of props.lsi) {
        this.addLocalSecondaryIndex(lsi)
      }
    }
  }
}

class IntegrationTestsStack extends cdk.Stack {
  constructor(app: cdk.App, props?: cdk.StackProps) {
    super(app, 'integration-tests', props)

    new TestingTable(this, 'simple-key-table', {
      partitionKey: { name: 'Key', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
    })

    new TestingTable(this, 'standard-table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
    })

    new TestingTable(this, 'gsi-table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
      gsi: [
        {
          indexName: 'GSI1',
          partitionKey: { name: 'GSI1PK', type: dynamodb.AttributeType.STRING },
          sortKey: { name: 'GSI1SK', type: dynamodb.AttributeType.STRING },
        },
      ],
    })

    new TestingTable(this, 'lsi-table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
      lsi: [
        {
          indexName: 'LSI1',
          sortKey: { name: 'LSI1SK', type: dynamodb.AttributeType.STRING },
        },
      ],
    })

    new TestingTable(this, 'simple-gsi-table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
      gsi: [
        {
          indexName: 'GSI1',
          partitionKey: { name: 'GSI1PK', type: dynamodb.AttributeType.STRING },
        },
      ],
    })

    new TestingTable(this, 'multi-index-table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
      gsi: [
        {
          indexName: 'GSI1',
          partitionKey: { name: 'GSI1PK', type: dynamodb.AttributeType.STRING },
          sortKey: { name: 'GSI1SK', type: dynamodb.AttributeType.STRING },
        },
        {
          indexName: 'GSI2',
          partitionKey: { name: 'GSI2PK', type: dynamodb.AttributeType.STRING },
          sortKey: { name: 'GSI2SK', type: dynamodb.AttributeType.STRING },
        },
      ],
      lsi: [
        {
          indexName: 'LSI1',
          sortKey: { name: 'LSI1SK', type: dynamodb.AttributeType.STRING },
        },
        {
          indexName: 'LSI2',
          sortKey: { name: 'LSI2SK', type: dynamodb.AttributeType.STRING },
        },
      ],
    })

    new TestingTable(this, 'scan-table', {
      partitionKey: { name: 'PK', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'SK', type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: 'TTL',
      gsi: [
        {
          indexName: 'GSI1',
          partitionKey: { name: 'GSI1PK', type: dynamodb.AttributeType.STRING },
          sortKey: { name: 'GSI1SK', type: dynamodb.AttributeType.STRING },
        },
      ],
    })
  }
}

async function main() {
  const sts = new STS()
  const identity = await sts.getCallerIdentity()

  const app = new cdk.App()
  new IntegrationTestsStack(app, {
    env: {
      region: 'us-east-1',
      account: identity.Account,
    },
  })

  app.synth()
}

main()
