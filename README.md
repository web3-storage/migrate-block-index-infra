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

