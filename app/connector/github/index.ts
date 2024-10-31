// From original index.ts
import { start } from "@hasura/ndc-duckduckapi";
import { makeConnector, duckduckapi } from "@hasura/ndc-duckduckapi";
import * as path from "path";
import { GithubIssuesSyncSchema, GitHubIssueSyncManager} from "./functions";

const connectorConfig: duckduckapi = {
  dbSchema: `

    SELECT 1;

    -- Add your SQL schema here.
    -- This SQL will be run on startup every time.
    -- CREATE TABLE TABLE_NAME (.....);

  ` + GithubIssuesSyncSchema,
  functionsFilePath: path.resolve(__dirname, "./functions.ts"),
};
 
(async () => {
  const connector = await makeConnector(connectorConfig);
  start(connector);
  const manager = new GitHubIssueSyncManager('hasura', 'graphql-engine');
  if (!process.env.GITHUB_API_TOKEN) {
    throw new Error('GITHUB_API_TOKEN environment variable is required');
  }
  await manager.initialize(process.env.GITHUB_API_TOKEN);
})();
