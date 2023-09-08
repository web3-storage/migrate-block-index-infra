# Migrate Block Index

Infra to migrate legacy `blocks` index DynamoDB Table.

The `Scanner` lambda does a full table scan of the legacy index table, and sends batches of records to an SQS Queue. Ported from https://github.com/alexdebrie/serverless-dynamodb-scanner

The `Consumer` lambda subscribes to the queue. It transforms the legacy records into the current format, checks if they exist in the destination table, and writes the missing ones in batches to that table.

## Getting started

The repo contains the infra deployment code for the table migration process.

```
├── packages       - lambda implementations
└── stacks         - sst and aws cdk code to deploy the lambdas
```

To work on this codebase **you need**:

- Node.js >= v18 (prod env is node v18)
- Install the deps with `npm i`

## Scanner
 
Lambda to scan an entire DynamoDB table, sending batches of records to an SQS Queue.

Invoke it directly:

```bash
aws lambda invoke --function-name arn-or-name \
  --invocation-type Event \
  --cli-binary-format raw-in-base64-out \
  --payload '{"TotalSegments":1,"Segment":0}' ./migrate-out.log
```

- `--function-name` can be the ARN or the function name
- `--invocation-type Event` causes the function to be invoked async, so we don't wait for it to complete.
- `--cli-binary-format` is required to make passing in the payload work! see [aws cli issue](https://github.com/awsdocs/aws-lambda-developer-guide/issues/180#issuecomment-1166923381)
- `--payload` lets you set the table scan partition parameters as a JSON string, see more details below
- `./no-such.log` the last arg is the `outfile`, which is required, but unused when invoking async!

 
The lambda stores it's progress in SSM parameter store so it can resume.
 
It invokes itself again when it's remaining execution time is less than `MIN_REMAINING_TIME_MS`, as we only get 15mins max lambda execution time.

Create an SSM parameter with name `/migrate-block-index/${Config.STAGE}/stop` to abort all currently running invocations for that stage e.g `/migrate-block-index/prod/stop` and set the value to any string e.g. STOP. The existence of a value for that key is the signal to stop.

### Scan partition

With ~858 million records to scan, 1 worker would take ~30days

```
@ ~1.5s processing time per 500 records.
(858,000,000 recs / 500 batch size) * 1.5s per batch = 2,574,000s = ~30days.
```

We can use [table scan partition](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan) parameters to split the scan into 10 segments to complete in about ~3 days, by invoking the lambda 10 times in parallel.

Pass `{ TotalSegments: number, Segment: number}` to control the scan partition parameters.
- `TotalSegments` is how many partitions to divide the full set into e.g `10`
- `Segment` is the scan partition index that this worker should operate on, e.g `0` for the first.

## Consumer

Queue consumer lambda to transform items to the current index format, check if they exists in destination table, and write those records that are missing.

Each record is a stringified array of up to 500 BlocksIndex objects from the Scanner.
 
Any failed writes from the DynamoDB BatchWriteCommand are send to the `unprocessedWritesQueue` for debugging and re-driving.

Any other errors and the message is released back to the queue. If that batch fails 3 times, it is send to the `batchDeadLetterQueue`

### Scaling

Lambda SQS consumer scaling is managed by AWS. But we can control how the batching is managed.

- `MaximumBatchingWindowInSeconds` - _default: 500ms_. Time, in seconds, that Lambda spends gathering records before invoking the function. Any value from 0 seconds to 300 seconds in increments of seconds.
- `BatchSize` - _default: 500ms_ The maximum number of records in each batch that Lambda pulls from your stream or queue and sends to your function. Lambda passes all of the records in the batch to the function in a single call, up to the payload limit for synchronous invocation (6 MB). When you set BatchSize to a value greater than 10, you must set MaximumBatchingWindowInSeconds to at least 1.

source: https://docs.aws.amazon.com/lambda/latest/dg/API_EventSourceMappingConfiguration.html

Setting batch size to `20` will send 10k records to the per invocation. `(20 * 500 = 10,000)`

> When a Lambda function subscribes to an SQS queue, Lambda polls the queue as it waits for messages to arrive. Lambda consumes messages in batches, starting at five concurrent batches with five functions at a time.
>
> If there are more messages in the queue, Lambda adds up to 60 functions per minute, up to 1,000 functions, to consume those messages. This means that Lambda can scale up to 1,000 concurrent Lambda functions processing messages from the SQS queue.
>
> This scaling behavior is managed by AWS and cannot be modified.
> 
> By default, Lambda batches up to 10 messages in a queue to process them during a single Lambda execution.
- https://aws.amazon.com/blogs/compute/understanding-how-aws-lambda-scales-when-subscribed-to-amazon-sqs-queues/

## Index formats

We need to convert the legacy format to the current format during the migration.

### Legacy

`blocks` table format.

| multihash | cars    | createdAt | data  | type    |
|-----------|---------|-----------|-------|---------|
| `zQm...` | `[ { "M" : { "offset" : { "N" : "3193520" }, "length" : { "N" : "100615" }, "car" : { "S" : "region/bucket/raw/bafy.../315.../ciq...car" } } } ]` | 2022-05-30T17:06:12.864Z | `{}` | raw

### Current

`blocks-cars-position` table format.

| blockmultihash | carPath | length | offset |
|----------------|---------|--------|--------|
| `z2D...` | region/bucket/raw/QmX.../315.../ciq...car | 2765 | 3317501 |

## Costs

This will be used to migrate indexes from the `blocks` table to the `blocks-cars-position` table.

### Source table scan cost

`blocks` table stats

| Item count    | Table size | Average item size
|---------------|------------|-----------------
| 858 million   | 273 GB     | 319 bytes

- 4k / 319 bytes = 12 items per 1 RCU _(eventaully consistent, cheap read, not transaction)_
- 858 million / 12 = 71 million RCUs 
- 71 * $0.25 per million = **$17 for a full table scan**

### Destination table write cost

`blocks-cars-position` table stats

| Item count    | Table size | Average item size
|---------------|------------|-----------------
| 43 billion    | 11 TB      | 255 bytes

Assuming we have to write 1 new record for every source record

- 1kb / 255 bytes = 3 items per WCU
- 858 million / 3 items per WCU = 286 million WCUs
- 286 * $1.25 per million = **$357.5 total write cost**

but initial explorations suggest we will actually only need to write 1% of the source table to the dst table. So the write cost will likely be very cheap as long we check for existence before attempting writes. Alas conditional writes cost as much as a write even if not write occurs.

### Lambda costs

On the consumer side, with `BatchSize: 20` we will process 10,000 records per invocation.
- 869 million / 10,000 = 85900 invocations.
- Estimate 10s to 30s per 10k processing time
- 1Gb ram
- = ~$30

On the scanner side
- @ ~1.5s processing time per 500 records.
- running for 11mins per invocation
- ((11 * 60) / 1.5) * 500 = 220,000 records per invocation
- 859 million / 220,000 recs per invocation = ~1,600 invocations
- = ~$9

per https://calculator.aws/#/addService/Lambda

### SQS costs

- 859,000,000 / 500 = 1,718,000 puts to the queue
- 3,436,000 queue ops
- ~$1

per https://calculator.aws/#/addService/SQS

### References

> Write operation costs $1.25 per million requests.
> Read operation costs $0.25 per million requests.
– https://dashbird.io/knowledge-base/dynamodb/dynamodb-pricing/

>Read consumption: Measured in 4KB increments (rounded up!) for each read operation. This is the amount of data that is read from the table or index... if you read a single 10KB item in DynamoDB, you will consume 3 RCUs (10 / 4 == 2.5, rounded up to 3).
>
> Write consumption: Measured in 1KB increments (also rounded up!) for each write operation. This is the size of the item you're writing / updating / deleting during a write operation... if you write a single 7.5KB item in DynamoDB, you will consume 8 WCUs (7.5 / 1 == 7.5, rounded up to 8).
– https://www.alexdebrie.com/posts/dynamodb-costs/

> If a ConditionExpression evaluates to false during a conditional write, DynamoDB still consumes write capacity from the table. The amount consumed is dependent on the size of the item (whether it’s an existing item in the table or a new one you are attempting to create or update). For example, if an existing item is 300kb and the new item you are trying to create or update is 310kb, the write capacity units consumed will be the 310kb item.
– https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate
