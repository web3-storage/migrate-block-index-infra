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

