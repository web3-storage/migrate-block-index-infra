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
 
Stores it's progress in SSM parameter store so it can resume.
 
The lambda invokes itself again when it's remaining execution time is less than `MIN_REMAINING_TIME_MS`, as we only get 15mins max lambda execution time.
 
Invoke it directly with 
```shell
aws lambda invoke --function-name <name> --invocation-type Event
```

Pass `{ TotalSegments: number, Segment: number}` to control the [table scan partition](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan)

With ~858 million records to scan, we will run with 10 segments to complete in about ~3 days.
```
~1.5s per 500 records. 
(858,000,000 recs / 500 batch size) * 1.5s per batch = 2,574,000s = ~30days.
```

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
| `z2D...` | region/bucket/raw/QmX.../315318734258452893/ciq...car | 2765 | 3317501 |

