import { expect, test, vi } from 'vitest'
import { transformItem, writeIfMissing, BlocksIndex, BlocksCarsPosition } from '../src/consumer.js'
import { mockClient } from 'aws-sdk-client-mock'
import { marshall } from '@aws-sdk/util-dynamodb'
import { DynamoDBClient, BatchGetItemCommand, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb'

test('transformItem maps a BlocksIndex to 1 BlocksCarsPositions', () => {
  const item: BlocksIndex = { multihash: 'hash', cars: [{ offset: 0, length: 10, car: 'over there' }] }
  const expected: BlocksCarsPosition = { blockmultihash: item.multihash, carpath: item.cars[0].car, offset: item.cars[0].offset, length: item.cars[0].length }
  const res = transformItem(item)
  expect(res.length).toBe(1)
  expect(res[0]).toEqual(expected)
})

test('transformItem maps a BlocksIndex with 2 cars to 2 BlocksCarsPositions', () => {
  const item: BlocksIndex = {
    multihash: 'hash', cars: [
      { offset: 0, length: 10, car: 'over there' },
      { offset: 20, length: 20, car: 'other one' }
    ]
  }
  const expected: BlocksCarsPosition[] = [
    { blockmultihash: item.multihash, carpath: item.cars[0].car, offset: item.cars[0].offset, length: item.cars[0].length },
    { blockmultihash: item.multihash, carpath: item.cars[1].car, offset: item.cars[1].offset, length: item.cars[1].length }
  ]
  const res = transformItem(item)
  expect(res.length).toBe(2)
  expect(res[0]).toEqual(expected[0])
  expect(res[1]).toEqual(expected[1])
})

test('writeIfMissing writes a record that doesn\'t exist already', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'
  const item: BlocksCarsPosition = { blockmultihash: 'hash', carpath: 'over there', offset: 0, length: 10 }

  dynamoMock
    .on(BatchGetItemCommand, {
      RequestItems: {
        [dstTable]: {
          Keys: [
            marshall({
              blockmultihash: item.blockmultihash,
              carpath: item.carpath
            })
          ]
        }
      }
    }).resolves({
      Responses: {
        [dstTable]: [] // none exist already in target table
      }
    })

    .on(BatchWriteItemCommand, {
      RequestItems: {
        [dstTable]: [{
          PutRequest: {
            Item: marshall(item)
          }
        }]
      }
    }).resolves({
      UnprocessedItems: {} // nothing unprocessed
    })

  const unprocessedHandler = vi.fn()

  const res = await writeIfMissing(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(1)
  expect(res.writeCount).toBe(1)
  expect(res.unprocessedCount).toBe(0)
  expect(unprocessedHandler).not.toHaveBeenCalled()
})

test('writeIfMissing skips write where record exists', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'
  const item: BlocksCarsPosition = { blockmultihash: 'hash', carpath: 'over there', offset: 0, length: 10 }

  dynamoMock
    .on(BatchGetItemCommand).resolves({
      Responses: {
        [dstTable]: [marshall(item)]
      }
    })

  const unprocessedHandler = vi.fn()

  const res = await writeIfMissing(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(1)
  expect(res.writeCount).toBe(0)
  expect(res.unprocessedCount).toBe(0)
  expect(unprocessedHandler).not.toHaveBeenCalled()
})

test('writeIfMissing calls unprocessedHandler', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'
  const item: BlocksCarsPosition = { blockmultihash: 'hash', carpath: 'over there', offset: 0, length: 10 }

  dynamoMock
    .on(BatchGetItemCommand).resolves({
      Responses: {
        [dstTable]: [] // none exist already in target table
      }
    })

    .on(BatchWriteItemCommand, {
      RequestItems: {}
    }).resolves({
      UnprocessedItems: { // write didn't happen
        [dstTable]: [{
          PutRequest: {
            Item: marshall(item)
          }
        }]
      }
    })

  const unprocessedHandler = vi.fn()

  const res = await writeIfMissing(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(1)
  expect(res.writeCount).toBe(0)
  expect(res.unprocessedCount).toBe(1)
  expect(unprocessedHandler).toHaveBeenCalledWith([{
    PutRequest: {
      Item: marshall(item)
    }
  }])
})
