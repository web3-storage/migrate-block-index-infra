import { Config } from 'sst/node/config'
import { Table } from 'sst/node/table'
import { Queue } from 'sst/node/queue'
import { Context } from 'aws-lambda'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { AttributeValue, DynamoDBClient, ScanCommand, ScanCommandOutput } from '@aws-sdk/client-dynamodb'
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda'
import { SSMClient, PutParameterCommand, GetParameterCommand, ParameterNotFound } from '@aws-sdk/client-ssm'
import retry, { Options as RetryOpts } from 'p-retry'

// stored in ssm parameter store save state between invocations
type Progress = { recordCount: number, lastEvaluated?: Record<string, AttributeValue>, stop?: boolean }

const STOP_VALUE = 'STOP' // set ssmKey param to this to force the lambda to stop self-invoking
const SCAN_BATCH_SIZE = 100 // max sqs msg is 256KB. each record is ~350bytes, 350 * 500 = 175KB
const MIN_REMAINING_TIME_MS = (15 * 60 * 1000) - 5000 // 4mins. Threshold at which we reinvoke the lambda
const RETRY_OPTS: RetryOpts = {
  // spread 10 retries over 1min
  // see: https://www.wolframalpha.com/input?i=Sum%5B1000*x%5Ek,+%7Bk,+0,+9%7D%5D+%3D+1+*+60+*+1000
  factor: 1.369,
  retries: 10,
  minTimeout: 1000
}

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
 * Pass { TotalSegments: number, Segment: number } to control the table scan partition
 * see: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan
 */
export async function handler(event: any, context: Context) {
  console.log('invoked', event)
  const TotalSegments = event.TotalSegments ?? 1
  const Segment = event.Segment ?? 0

  // set via the `bind` config in the sst stack
  const tableName = Table.srcTable.tableName
  const queueUrl = Queue.batchQueue.queueUrl

  const ssm = new SSMClient({})
  const sqs = new SQSClient({})
  const dynamo = new DynamoDBClient({})
  const lambda = new LambdaClient({})

  const stopKey = `/migrate-block-index/${Config.STAGE}/stop`
  const progressKey = `/migrate-block-index/${Config.STAGE}/${TotalSegments}/${Segment}/progress`
  const prevProgress = await getProgress(ssm, progressKey)

  if (prevProgress.stop) {
    console.log(`Stopping! Found ${STOP_VALUE} under ssm param ${progressKey}`, prevProgress)
    return { TotalSegments, Segment, ...prevProgress }
  }

  if (await checkStop(ssm, stopKey)) {
    console.log(`Stopping! Found global stop param ${stopKey}`, prevProgress)
    return { TotalSegments, Segment, ...prevProgress }
  }

  // init loop info from previous progress report
  let { lastEvaluated, recordCount } = prevProgress

  while (true) {
    const cmd = new ScanCommand({
      TotalSegments,
      Segment,
      ExclusiveStartKey: lastEvaluated,
      TableName: tableName,
      Limit: SCAN_BATCH_SIZE
    })

    const res: ScanCommandOutput = await retry(() => dynamo.send(cmd), RETRY_OPTS)

    const items = res.Items ?? []
    if (items.length > 0) {
      const records = items.map(i => unmarshall(i))
      await sendToQueue(sqs, queueUrl, records)
    }

    lastEvaluated = res.LastEvaluatedKey
    recordCount += items.length

    const progress = { recordCount, lastEvaluated, stop: lastEvaluated === undefined }
    await storeProgress(ssm, progressKey, progress)

    // lastEvaluated is undefined when we reach the end of our scan partition
    if (lastEvaluated === undefined) {
      console.log(`Scan complete. Processed ${recordCount} records`, { TotalSegments, Segment })
      break // done!
    }

    if (await checkStop(ssm, stopKey)) {
      console.log(`Stopping! Found global stop param ${stopKey}`, progress)
      return { TotalSegments, Segment, ...progress }
    }

    const msRemaining = context.getRemainingTimeInMillis()
    if (msRemaining < MIN_REMAINING_TIME_MS) {
      console.log(`Reinvoking. Processed ${recordCount} records, ${msRemaining}ms remain`, { TotalSegments, Segment })
      await invokeSelf(lambda, event, context)
      break
    }
  }

  return { TotalSegments, Segment, recordCount, lastEvaluated }
}

async function sendToQueue(sqs: SQSClient, queueUrl: string, records: Record<string, any>[]) {
  const body = JSON.stringify(records)
  console.log(`Sending batch of ${records.length} with size ${new TextEncoder().encode(body).length} `)
  const cmd = new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: body
  })
  await retry(() => sqs.send(cmd), RETRY_OPTS)
  return records.length
}

/**
 * Invoke the currently executing lambda again.
 * Extract the lambda function name from the context.
 * 
 * `InvocationType: 'Event'` invokes the lambda async. We only wait for the invocation
 * command to be successful. The current lambda does not wait for the invoked to complete.
 */
async function invokeSelf(lambda: LambdaClient, event: any, context: Context) {
  const cmd = new InvokeCommand({
    FunctionName: context.functionName,
    InvocationType: 'Event',
    LogType: 'None',
    Payload: JSON.stringify(event)
  })
  return retry(() => lambda.send(cmd), RETRY_OPTS)
}

/**
 * Gets the last evaluated key so we can continue the scan from where we left off.
 */
async function getProgress(ssm: SSMClient, progressKey: string): Promise<Progress> {
  const val = await getKey(ssm, progressKey)
  if (val) {
    const res = JSON.parse(val)
    return {
      stop: res.stop,
      lastEvaluated: res.lastEvaluated,
      recordCount: res.recordCount ?? 0
    }
  }
  return { stop: false, lastEvaluated: undefined, recordCount: 0 }
}

/**
 * Check the global stop key. If a value exists, return true, else false
 */
async function checkStop(ssm: SSMClient, stopKey: string) {
  return Boolean(await getKey(ssm, stopKey))
}

/**
 * Get the value for an SSM parameter. or return undefined for ParameterNotFound errors.
 */
async function getKey(ssm: SSMClient, key: string) {
  const cmd = new GetParameterCommand({ Name: key })
  const val = await retry(async () => {
    try {
      const res = await ssm.send(cmd)
      if (res.Parameter?.Value === undefined) {
        throw new Error('Parameter.Value missing on ssm GetParameter response')
      }
      return res.Parameter.Value
    } catch (err) {
      if (err instanceof ParameterNotFound) {
        // ok
        return undefined
      }
      throw err
    }
  }, RETRY_OPTS)
  return val
}

async function storeProgress(ssm: SSMClient, key: string, value: Progress) {
  const cmd = new PutParameterCommand({
    Name: key,
    Value: JSON.stringify(value),
    Type: 'String',
    Overwrite: true,
    Tier: 'Standard'
  })
  await retry(() => ssm.send(cmd), RETRY_OPTS)
  return value
}
