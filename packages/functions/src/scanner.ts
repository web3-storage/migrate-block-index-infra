import { Config } from 'sst/node/config'
import { Table } from 'sst/node/table'
import { Queue } from 'sst/node/queue'
import { Context } from 'aws-lambda'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { DynamoDBClient, ScanCommand, ScanCommandOutput } from '@aws-sdk/client-dynamodb'
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda'
import { SSMClient, PutParameterCommand, GetParameterCommand, ParameterNotFound } from '@aws-sdk/client-ssm'

const SCAN_BATCH_SIZE = 500 // max sqs msg is 256KB. each record is ~350bytes, 350 * 500 = 175KB
const MIN_REMAINING_TIME_MS = 10_000 // threshold at which we reinvoke the lambda

/**
 * Lambda to scan an entire DynamoDB table, sending batches of records to an SQS Queue.
 * 
 * Stores it's progress in SSM parameter store so it can resume.
 * 
 * Invokes itself again when it's remaining execution time is less than `MIN_REMAINING_TIME_MS`.
 * 
 * Invoke it directly with 
 *   aws lambda invoke --function-name <name> --invocation-type Event
 * 
 * Pass { TotalSegments: number, Segment: number} to control the table scan partition
 * see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
 */
export async function handler(event: any, context: Context) {
  console.log(`invoked`, event)
  const TotalSegments = event.TotalSegments ?? 1
  const Segment = event.Segment ?? 0

  // set via the `bind` config in the sst stack
  const tableName = Table.srcTable.tableName
  const queueUrl = Queue.batchQueue.queueUrl

  const ssm = new SSMClient({})
  const sqs = new SQSClient({})
  const dynamo = new DynamoDBClient({})
  const lambda = new LambdaClient({})

  const ssmKey = `/migrate-block-index/${Config.STAGE}/last-evaluated/${TotalSegments}/${Segment}`
  let lastEvaluated = await getLastEvaluated(ssm, ssmKey)

  let recordCount = 0
  while (true) {
    const res: ScanCommandOutput = await dynamo.send(new ScanCommand({
      TotalSegments,
      Segment,
      ExclusiveStartKey: lastEvaluated,
      TableName: tableName,
      Limit: SCAN_BATCH_SIZE
    }))

    if (!res.Items) {
      console.error('Error: Scan returned no items', JSON.stringify(lastEvaluated), { TotalSegments, Segment })
      break
    }
    const records = res.Items.map(i => unmarshall(i))
    recordCount += await sendToQueue(sqs, queueUrl, records)

    if (!res.LastEvaluatedKey) {
      console.log(`Scan complete. Processed ${recordCount} records`, { TotalSegments, Segment })
      break // done!
    }

    lastEvaluated = await storeLastEvaluated(ssm, ssmKey, res.LastEvaluatedKey)

    const msRemaining = context.getRemainingTimeInMillis()
    if (msRemaining < MIN_REMAINING_TIME_MS) {
      console.log(`Reinvoking. Processed ${recordCount} records, ${msRemaining}ms remain`, { TotalSegments, Segment })
      await invokeSelf(lambda, event, context)
      break
    }
  }

  return { recordCount, TotalSegments, Segment }
}

async function sendToQueue(sqs: SQSClient, queueUrl: string, records: Record<string, any>[]) {
  const body = JSON.stringify(records)
  console.log(`Sending batch of ${records.length} with size ${new TextEncoder().encode(body).length}`)
  await sqs.send(new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: body
  }))
  return records.length
}

async function invokeSelf(lambda: LambdaClient, event: any, context: Context) {
  return lambda.send(new InvokeCommand({
    FunctionName: context.functionName,
    InvocationType: 'Event',
    LogType: 'None',
    Payload: JSON.stringify(event)
  }))
}

async function getLastEvaluated(ssm: SSMClient, key: string) {
  try {
    const res = await ssm.send(new GetParameterCommand({
      Name: key,
    }))
    if (res.Parameter && res.Parameter.Value) {
      return JSON.parse(res.Parameter.Value)
    }
  } catch (err) {
    if (err instanceof ParameterNotFound) {
      // ok
      return undefined
    }
    throw err
  }
  return undefined
}

async function storeLastEvaluated(ssm: SSMClient, key: string, value: object) {
  await ssm.send(new PutParameterCommand({
    Name: key,
    Value: JSON.stringify(value),
    Type: 'String',
    Overwrite: true,
    Tier: 'Standard'
  }))
  return value
}
