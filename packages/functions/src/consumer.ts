import { SQSEvent } from "aws-lambda"

// transform the records
// check if they exist in target dynamo
// write missing ones
export async function handler(event: SQSEvent) {
  const records: any[] = event.Records;
  console.log(`Received "${records.length}" records, `)
  for (const r of records) {
    const batch = JSON.parse(r.body)
    console.log(`Batch of ${batch.length}`)
  }

}
