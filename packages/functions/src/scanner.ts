import { Config } from 'sst/node/config'
import { Table } from 'sst/node/table'
import { Queue } from 'sst/node/queue'
import { Context } from 'aws-lambda'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { DynamoDBClient, ScanCommand, ScanCommandOutput } from '@aws-sdk/client-dynamodb'
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda'
import { SSMClient, PutParameterCommand, GetParameterCommand } from '@aws-sdk/client-ssm'

const SCAN_BATCH_SIZE = 100
const MIN_REMAINING_TIME_MS = 60_000
const LAST_EVALUATED_PARAM = `/migrate-block-index/${Config.STAGE}/last-evaluated`

/**
 * Lambda to scan an entire DynamoDB table, sending batches of records to an SQS Queue.
 * 
 * Stores it's progress in SSM parameter store so it can resume.
 * 
 * Invokes itself again when it's remaining execution time is less than `MIN_REMAINING_TIME_MS`.
 * 
 * Invoke it directly with 
 *   aws lambda invoke --function-name <name> --invocation-type Event
 */
export async function handler (event: any, context: Context) {
  console.log(`invoked: "${JSON.stringify(event)}"`)
  
  // set via the `bind` config in the sst stack
  const tableName = Table.srcTable.tableName
  const queueUrl = Queue.batchQueue.queueUrl

  const ssm = new SSMClient({})
  const sqs = new SQSClient({})
  const dynamo = new DynamoDBClient({})
  const lambda = new LambdaClient({})

  let recordCount = 0
  let lastEvaluated = await getLastEvaluated(ssm)

  while (recordCount < 500) {
    const res: ScanCommandOutput = await dynamo.send(new ScanCommand({
      ExclusiveStartKey: lastEvaluated,
      TableName: tableName,
      Limit: SCAN_BATCH_SIZE
    }))

    if (!res.Items) {
      console.error('Error: Scan returned no items', JSON.stringify(lastEvaluated))
      break
    }
    const records = res.Items.map(i => unmarshall(i))
    recordCount += await sendToQueue(sqs, queueUrl, records)

    if(!res.LastEvaluatedKey) {
      console.log(`Scan complete. Processed ${recordCount} records`)
      break // done!
    }

    lastEvaluated = await storeLastEvaluated(ssm, res.LastEvaluatedKey)
    
    if (context.getRemainingTimeInMillis() < MIN_REMAINING_TIME_MS) {
      console.log(`Reinvoking. Processed ${recordCount} records`)
      await invokeSelf(lambda, context)
      break
    }
  }

  return {}
}

async function sendToQueue (sqs: SQSClient, queueUrl: string, records: Record<string, any>[]) {
  const body = JSON.stringify(records)
  console.log(`Sending batch of ${records.length} with size ${new TextEncoder().encode(body).length}`)
  await sqs.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: body
  }))

  return records.length
}

async function invokeSelf (lambda: LambdaClient, context: Context) {
  return lambda.send(new InvokeCommand({
    FunctionName: context.functionName,
    InvocationType: 'Event',
    LogType: 'None',
  })) 
}

async function getLastEvaluated (ssm: SSMClient) {
  const res = await ssm.send(new GetParameterCommand({
    Name: LAST_EVALUATED_PARAM
  }))
  if (res.Parameter && res.Parameter.Value) {
    return JSON.parse(res.Parameter.Value)
  }
  return undefined
}

async function storeLastEvaluated (ssm: SSMClient, value: object) {
  await ssm.send(new PutParameterCommand({
    Name: LAST_EVALUATED_PARAM,
    Value: JSON.stringify(value),
    Type: 'String',
    Overwrite: true,
    Tier: 'Standard'
  }))
  return value
}
