import { pipeline } from 'node:stream/promises'
import { Table } from 'sst/node/table'
import { Queue } from 'sst/node/queue'
import { SQSEvent } from 'aws-lambda'
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs'
import { DynamoDBClient, BatchGetItemCommand, BatchWriteItemCommand, WriteRequest } from '@aws-sdk/client-dynamodb'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import batch from 'it-batch'

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.BatchOperations
const BATCH_READ_LIMIT = 100
const BATCH_WRITE_LIMIT = 25

export type BlocksIndex = { multihash: string, cars: Array<{ offset: number, length: number, car: string }> }
export type BlocksCarsPosition = { blockmultihash: string, carpath: string, length: number, offset: number }

/**
 * Lambda SQS queue consumer.
 * Transform, check if exists in destination table, and write if missing.
 * 
 * Each record is a stringified array of up to 500 BlocksIndex objects.
 * 
 * Failed writes are send to the unprocessedWritesQueue for debugging.
 */
export async function handler(event: SQSEvent) {
  const dstTable = Table.dstTable.tableName
  const unprocessedQueueUrl = Queue.unprocessedWritesQueue.queueUrl

  const sqs = new SQSClient({})
  const dynamo = new DynamoDBClient({})

  // the body of each record is an array of BlockIndex items
  const items: BlocksIndex[] = event.Records.flatMap(r => JSON.parse(r.body))

  console.log(`Processing "${items.length}" items`)
  const { itemCount, writeCount, unprocessedCount } = await migrator(dstTable, dynamo, items, captureUnprocessedItems(unprocessedQueueUrl, sqs))

  console.log(`Wrote ${writeCount} records. ${unprocessedCount} unprocessed writes`)
  return { itemCount, writeCount, unprocessedCount }
}

export async function migrator(dstTable: string, dynamo: DynamoDBClient, items: BlocksIndex[], unprocessedItemHandler: (unprocessed: WriteRequest[]) => Promise<void>) {
  let itemCount = 0
  let writeCount = 0
  let unprocessedCount = 0
  await pipeline(
    items,
    async function* (items) {
      for await (const item of items) {
        yield* transformItem(item)
      }
    },
    (items) => batch(items, BATCH_READ_LIMIT),
    async function* (batches) {
      for await (const batch of batches) {
        itemCount += batch.length
        yield* checkExists(dstTable, dynamo, batch)
      }
    },
    (items) => batch(items, BATCH_WRITE_LIMIT),
    async function* (batches) {
      for await (const batch of batches) {
        const unprocessed = await write(dstTable, dynamo, batch)
        if (unprocessed.length > 0) {
          await unprocessedItemHandler(unprocessed)
        }
        writeCount += batch.length - unprocessed.length
        unprocessedCount += unprocessed.length
      }
    }
  )
  return { itemCount, writeCount, unprocessedCount }
}

export function captureUnprocessedItems(queueUrl: string, sqs: SQSClient) {
  return async function (unprocessed: WriteRequest[]) {
    await sqs.send(new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify(unprocessed)
    }))
  }
}

/**
 * Converts a BlockIndex to 1 or more BlocksCarsPositions
 */
export function* transformItem(item: BlocksIndex) {
  for (const { offset, length, car } of item.cars) {
    const transformed: BlocksCarsPosition = {
      blockmultihash: item.multihash,
      carpath: car,
      offset,
      length
    }
    yield transformed
  }
}

/**
 * Yield records that don't already exist in the dstTable.
 * 
 * This is a cost saving step. A read is much cheaper than a write, so we avoid
 * writing records for which we already have that multihash + carPath combo.
 * 
 * Ideally we'd use a conditional write, but amazon charges the write cost 
 * regardless, even if the condition prevents the write.
 * 
 * We have to manually avoid duplicate multihash + carpath keys per batch:
 * > ValidationException: Provided list of item keys contains duplicates
 * 
 * We ignore any UnprocessedItems from the BatchGetItem response. It will
 * be interpreted as having not existed, and so cause a possibly un-needed write.
 */
export async function* checkExists(dstTable: string, client: DynamoDBClient, items: BlocksCarsPosition[]) {
  // remove duplicates
  const itemMap: Map<string, BlocksCarsPosition> = new Map()
  for (const item of items) {
    itemMap.set(`${item.blockmultihash}#${item.carpath}`, item)
  }

  // map items to dynamo GetItem query keys
  const Keys = Array.from(itemMap.values()).map(i => {
    return {
      blockmultihash: { S: i.blockmultihash },
      carpath: { S: i.carpath }
    }
  })

  const cmd = new BatchGetItemCommand({
    RequestItems: {
      [dstTable]: {
        Keys
      }
    }
  })

  const res = await client.send(cmd)

  const responses = res.Responses?.[dstTable]
  if (!responses) {
    throw new Error('Error: BatchGetItem returned no Responses object')
  }

  // @ts-expect-error get responses are BlocksCarsPosition objects
  const found: BlocksCarsPosition[] = responses.map(x => unmarshall(x))
  const foundSet = new Set()
  for (const { blockmultihash, carpath } of found) {
    foundSet.add(`${blockmultihash}#${carpath}`)
  }

  for (const item of itemMap.values()) {
    const { blockmultihash, carpath } = item
    if (!foundSet.has(`${blockmultihash}#${carpath}`)) {
      yield item
    }
  }
}

/** 
 * Write batches of records to the dst table.
 * 
 * Return any UnprocessedItems. These are writes that should happen but didn't
 * So send them to a DLQ for inspection and re-driving.
 */
async function write(dstTable: string, client: DynamoDBClient, batch: BlocksCarsPosition[]) {
  // remove duplicates
  const itemMap = new Map()
  for (const item of batch) {
    itemMap.set(`${item.blockmultihash}#${item.carpath}`, item)
  }

  const puts: WriteRequest[] = Array.from(itemMap.values()).map(item => {
    return {
      PutRequest: {
        Item: marshall(item)
      }
    }
  })

  const cmd = new BatchWriteItemCommand({
    RequestItems: {
      [dstTable]: puts
    }
  })
  const res = await client.send(cmd)

  return res.UnprocessedItems?.[dstTable] ?? []
}