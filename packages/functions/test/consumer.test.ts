import { expect, test, vi } from 'vitest'
import { migrator, BlocksIndex, BlocksCarsPosition } from '../src/consumer'
import { mockClient } from 'aws-sdk-client-mock'
import { marshall } from '@aws-sdk/util-dynamodb'
import { DynamoDBClient, BatchGetItemCommand, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb'

test('migrator writes a record that doesn\'t exist', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'

  const item: BlocksIndex = { multihash: 'hash', cars: [{ offset: 0, length: 10, car: 'over there' }] }
  const expected: BlocksCarsPosition = { blockmultihash: item.multihash, carpath: item.cars[0].car, offset: item.cars[0].offset, length: item.cars[0].length }

  dynamoMock
    .on(BatchGetItemCommand, {
      RequestItems: {
        [dstTable]: {
          Keys: [
            marshall({
              blockmultihash: expected.blockmultihash,
              carpath: expected.carpath
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
            Item: marshall(expected)
          }
        }]
      }
    }).resolves({
      UnprocessedItems: {} // nothing unprocessed
    })

  const unprocessedHandler = vi.fn()

  const res = await migrator(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(1)
  expect(res.writeCount).toBe(1)
  expect(res.unprocessedCount).toBe(0)
  expect(unprocessedHandler).not.toHaveBeenCalled()
})

test('migrator skips write where record exists', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'

  const item: BlocksIndex = { multihash: 'hash', cars: [{ offset: 0, length: 10, car: 'over there' }] }
  const expected: BlocksCarsPosition = { blockmultihash: item.multihash, carpath: item.cars[0].car, offset: item.cars[0].offset, length: item.cars[0].length }

  dynamoMock
    .on(BatchGetItemCommand).resolves({
      Responses: {
        [dstTable]: [marshall(expected)]
      }
    })

  const unprocessedHandler = vi.fn()

  const res = await migrator(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(1)
  expect(res.writeCount).toBe(0)
  expect(res.unprocessedCount).toBe(0)
  expect(unprocessedHandler).not.toHaveBeenCalled()
})

test('migrator calls unprocessedHandler', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'

  const item: BlocksIndex = { multihash: 'hash', cars: [{ offset: 0, length: 10, car: 'over there' }] }
  const expected: BlocksCarsPosition = { blockmultihash: item.multihash, carpath: item.cars[0].car, offset: item.cars[0].offset, length: item.cars[0].length }

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
            Item: marshall(expected)
          }
        }]
      }
    })

  const unprocessedHandler = vi.fn()

  const res = await migrator(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(1)
  expect(res.writeCount).toBe(0)
  expect(res.unprocessedCount).toBe(1)
  expect(unprocessedHandler).toHaveBeenCalledWith([{
    PutRequest: {
      Item: marshall(expected)
    }
  }])
})

test('migrator writes 2 records for 1 BlockIndex with 2 cars', async () => {
  const dynamo = new DynamoDBClient({})
  const dynamoMock = mockClient(dynamo)

  const dstTable = 'test'

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

  dynamoMock
    .on(BatchGetItemCommand).resolves({
      Responses: {
        [dstTable]: [] // none exist already in target table
      }
    })

    .on(BatchWriteItemCommand, {
      RequestItems: {
        [dstTable]: [
          { PutRequest: { Item: marshall(expected[0]) } },
          { PutRequest: { Item: marshall(expected[1]) } }
        ]
      }
    }).resolves({
      UnprocessedItems: {} // nothing unprocessed
    })

  const unprocessedHandler = vi.fn()

  const res = await migrator(dstTable, dynamo, [item], unprocessedHandler)
  expect(res.itemCount).toBe(2)
  expect(res.writeCount).toBe(2)
  expect(res.unprocessedCount).toBe(0)
  expect(unprocessedHandler).not.toHaveBeenCalled()
})