import { StackContext, Function, Queue, Table } from "sst/constructs"
import * as dynamodb from "aws-cdk-lib/aws-dynamodb"

export function BlockIndexMigrator({ app, stack }: StackContext) {

  app.setDefaultFunctionProps({
    runtime: "nodejs18.x",
    architecture: "arm_64",
    logRetention: "one_week"
  })

  const srcTable = new Table(stack, 'srcTable', {
    cdk: {
      table: dynamodb.Table.fromTableName(stack, "srcTableImport", env('SRC_TABLE')),
    }
  })

  const dstTable = new Table(stack, 'dstTable', {
    cdk: {
      table: dynamodb.Table.fromTableName(stack, "dstTableImport", env('DST_TABLE')),
    }
  })

  const batchQueue = new Queue(stack, 'batchQueue', {
    consumer: {
      function: {
        handler: "packages/functions/src/consumer.handler",
        permissions: ["dynamodb:GetItem", "dynamodb:PutItem"],
        bind: [dstTable]
      }
    }
  })

  const scanner = new Function(stack, 'scanner', {
    handler: "packages/functions/src/scanner.handler",
    permissions: ["dynamodb:Scan"],
    bind: [srcTable, batchQueue]
  })

  stack.addOutputs({
    scanner: scanner.functionArn,
    queue: batchQueue.queueUrl,
  })
}

function env (key: string) {
  const val = process.env[key]
  if (!val) {
    throw new Error(`${key} must be defined in env`)
  }
  return val
}
