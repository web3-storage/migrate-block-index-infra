import { SSTConfig } from "sst";
import { BlockIndexMigrator } from "./stacks/migrate-block-index.js"

export default {
  config(_input) {
    return {
      name: "migrate-block-index",
      region: "us-west-2",
    };
  },
  stacks(app) {
    app.stack(BlockIndexMigrator);
  }
} satisfies SSTConfig
