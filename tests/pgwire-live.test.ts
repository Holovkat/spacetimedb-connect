import "dotenv/config";
import net from "node:net";
import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const hasLiveEnv = Boolean(
  process.env.STDB_BASE_URL &&
    process.env.STDB_AUTH_TOKEN &&
    process.env.STDB_SOURCE_DATABASE
);
const sourceDatabase = process.env.STDB_SOURCE_DATABASE ?? "example-app-db";
const hasLiveDmlFixture = Boolean(
  process.env.STDB_LIVE_DML_TABLE &&
    process.env.STDB_LIVE_DML_KEY_COLUMN &&
    process.env.STDB_LIVE_DML_VALUE_COLUMN
);

function parseCsvEnv(value: string | undefined): string[] {
  return (value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replaceAll("\"", "\"\"")}"`;
}

function getExpectedLiveDatabases(): string[] {
  const configured = parseCsvEnv(process.env.STDB_EXPECTED_DATABASES);
  return configured.length > 0 ? configured : [sourceDatabase];
}

let createPgwireServer: typeof import("../src/pgwire/server.js").createPgwireServer;

function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const tester = net.createServer();
    tester.once("error", reject);
    tester.listen(0, "127.0.0.1", () => {
      const address = tester.address();
      if (!address || typeof address === "string") {
        reject(new Error("Failed to allocate port"));
        return;
      }
      const { port } = address;
      tester.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(port);
      });
    });
  });
}

describe.skipIf(!hasLiveEnv)("pgwire pass-through", () => {
  let server: net.Server;
  let port: number;

  beforeAll(async () => {
    ({ createPgwireServer } = await import("../src/pgwire/server.js"));
    port = await getFreePort();
    server = createPgwireServer();
    await new Promise<void>((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, "127.0.0.1", () => {
        server.off("error", reject);
        resolve();
      });
    });
  }, 120000);

  afterAll(async () => {
    if (!server) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }, 120000);

  it(
    "serves database metadata through pgwire",
    async () => {
      const client = new Client({
        host: "127.0.0.1",
        port,
        database: "postgres",
        user: "shim",
        password: "shim",
      });

      await client.connect();
      const databases = await client.query("select datname from pg_database order by datname");
      await client.end();

      expect(databases.rows.map((row) => row.datname)).toContain("postgres");
      for (const databaseName of getExpectedLiveDatabases()) {
        expect(databases.rows.map((row) => row.datname)).toContain(databaseName);
      }
    },
    120000
  );

  it(
    "passes through table reads for a discovered public table",
    async () => {
      const client = new Client({
        host: "127.0.0.1",
        port,
        database: sourceDatabase,
        user: "shim",
        password: "shim",
      });

      await client.connect();
      let result;
      try {
        const tables = await client.query(
          "select table_name from information_schema.tables where table_schema = 'public' order by table_name limit 1"
        );
        const tableName = tables.rows[0]?.table_name;
        expect(tableName).toBeDefined();

        const columns = await client.query(
          "select column_name from information_schema.columns where table_schema = 'public' and table_name = $1 order by ordinal_position limit 3",
          [tableName]
        );
        const columnNames = columns.rows.map((row) => row.column_name);
        expect(columnNames.length).toBeGreaterThan(0);

        result = await client.query(
          `select ${columnNames.map(quoteIdentifier).join(", ")} from public.${quoteIdentifier(tableName)} limit 3`
        );
      } finally {
        await client.end();
      }

      expect(result.command).toBe("SELECT");
      expect(result.fields.length).toBeGreaterThan(0);
    },
    120000
  );

  it.skipIf(!hasLiveDmlFixture)(
    "passes through parameterized insert/update/delete live when a DML fixture is configured",
    async () => {
      const client = new Client({
        host: "127.0.0.1",
        port,
        database: sourceDatabase,
        user: "shim",
        password: "shim",
      });

      const tableName = process.env.STDB_LIVE_DML_TABLE!;
      const keyColumn = process.env.STDB_LIVE_DML_KEY_COLUMN!;
      const valueColumn = process.env.STDB_LIVE_DML_VALUE_COLUMN!;
      const probeKey = `shim_live_${Date.now()}`;

      await client.connect();
      try {
        const insertResult = await client.query({
          text: `insert into public.${quoteIdentifier(tableName)} (${quoteIdentifier(keyColumn)}, ${quoteIdentifier(valueColumn)}) values ($1, $2)`,
          values: [probeKey, "one"],
        });
        const updateResult = await client.query({
          text: `update public.${quoteIdentifier(tableName)} set ${quoteIdentifier(valueColumn)} = $1 where ${quoteIdentifier(keyColumn)} = $2`,
          values: ["two", probeKey],
        });
        const selectResult = await client.query({
          text: `select ${quoteIdentifier(keyColumn)} as key, ${quoteIdentifier(valueColumn)} as value from public.${quoteIdentifier(tableName)} where ${quoteIdentifier(keyColumn)} = $1`,
          values: [probeKey],
        });
        const deleteResult = await client.query({
          text: `delete from public.${quoteIdentifier(tableName)} where ${quoteIdentifier(keyColumn)} = $1`,
          values: [probeKey],
        });

        expect(insertResult.command).toBe("INSERT");
        expect(insertResult.rowCount).toBe(1);
        expect(updateResult.command).toBe("UPDATE");
        expect(updateResult.rowCount).toBe(1);
        expect(selectResult.rows).toEqual([{ key: probeKey, value: "two" }]);
        expect(deleteResult.command).toBe("DELETE");
        expect(deleteResult.rowCount).toBe(1);
      } finally {
        await client.end();
      }
    },
    120000
  );
});
