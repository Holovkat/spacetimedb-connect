import "dotenv/config";
import { beforeAll, describe, expect, it } from "vitest";

const hasLiveEnv = Boolean(
  process.env.STDB_BASE_URL &&
    process.env.STDB_AUTH_TOKEN &&
    process.env.STDB_SOURCE_DATABASE
);
const sourceDatabase = process.env.STDB_SOURCE_DATABASE ?? "example-app-db";
let discoverDatabases: typeof import("../src/main.js").discoverDatabases;
let discoverTables: typeof import("../src/main.js").discoverTables;
let normalizeResult: typeof import("../src/shim/normalize.js").normalizeResult;
let StdbClient: typeof import("../src/shim/stdb-client.js").StdbClient;

function parseCsvEnv(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function getExpectedLiveDatabases(): string[] {
  const configured = parseCsvEnv(process.env.STDB_EXPECTED_DATABASES);
  return configured.length > 0 ? configured : [sourceDatabase];
}

describe.skipIf(!hasLiveEnv)("live table discovery", () => {
  beforeAll(async () => {
    ({ discoverDatabases, discoverTables } = await import("../src/main.js"));
    ({ normalizeResult } = await import("../src/shim/normalize.js"));
    ({ StdbClient } = await import("../src/shim/stdb-client.js"));
  });

  it(
    "discovers the current Spacetime database set",
    async () => {
      const databaseNames = await discoverDatabases(new StdbClient());

      for (const databaseName of getExpectedLiveDatabases()) {
        expect(databaseNames).toContain(databaseName);
      }
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
