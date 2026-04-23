import "dotenv/config";
import { describe, expect, it } from "vitest";
import { discoverDatabases, discoverTables } from "../src/main.js";
import { normalizeResult } from "../src/shim/normalize.js";
import { StdbClient } from "../src/shim/stdb-client.js";

const hasLiveEnv = Boolean(
  process.env.STDB_BASE_URL &&
    process.env.STDB_AUTH_TOKEN &&
    process.env.STDB_SOURCE_DATABASE
);

describe.skipIf(!hasLiveEnv)("live table discovery", () => {
  it(
    "discovers the current Spacetime database set",
    async () => {
      const databaseNames = await discoverDatabases(new StdbClient());

      expect(databaseNames).toContain("fms-glm-control");
      expect(databaseNames).toContain("fms-glm-org-glm-logistics");
      expect(databaseNames).toContain("fms-glm-org-tt");
      expect(databaseNames).toContain("fms-glm-org-airfreight");
    },
    120000
  );

  it(
    "discovers and samples every public user table",
    async () => {
      const stdbClient = new StdbClient();
      const tableNames = await discoverTables(stdbClient);

      expect(tableNames.length).toBeGreaterThan(0);

      for (const tableName of tableNames) {
        const result = await stdbClient.selectSample(tableName);
        const normalized = normalizeResult(tableName, result);

        expect(normalized.tableName).toBe(tableName);
        expect(normalized.columns.length).toBeGreaterThan(0);
      }
    },
    120000
  );
});
