import { SQSEvent } from "aws-lambda"

// transform the records
// check if they exist in target dynamo
// write missing ones

export async function handler(event: SQSEvent) {
  const records: any[] = event.Records;
  console.log(`Message processed: "${records[0].body}"`);
  return {}
}
