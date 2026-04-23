import "dotenv/config";
import net from "node:net";
import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPgwireServer } from "../src/pgwire/server.js";

const hasLiveEnv = Boolean(
  process.env.STDB_BASE_URL &&
    process.env.STDB_AUTH_TOKEN &&
    process.env.STDB_SOURCE_DATABASE
);

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
      const databases = await client.query(
        "select datname from pg_database where datname like 'fms-glm%' order by datname"
      );
      await client.end();

      expect(databases.rows.map((row) => row.datname)).toContain("fms-glm-control");
      expect(databases.rows.map((row) => row.datname)).toContain("fms-glm-org-tt");
    },
    120000
  );

  it(
    "passes through table reads live",
    async () => {
      const client = new Client({
        host: "127.0.0.1",
        port,
        database: "fms-glm-org-tt",
        user: "shim",
        password: "shim",
      });

      await client.connect();
      let result;
      try {
        result = await client.query(
          "select id, session_id, type from public.conversation_items order by id limit 3"
        );
      } finally {
        await client.end();
      }

      expect(result.rows.length).toBe(3);
      expect(result.rows[0].id).toBeDefined();
    },
    120000
  );

  it(
    "passes through parameterized insert/update/delete live",
    async () => {
      const client = new Client({
        host: "127.0.0.1",
        port,
        database: "fms-glm-org-tt",
        user: "shim",
        password: "shim",
      });

      const probeKey = `shim_live_${Date.now()}`;

      await client.connect();
      try {
        const insertResult = await client.query({
          text: "insert into public.config (key, value) values ($1, $2)",
          values: [probeKey, "one"],
        });
        const updateResult = await client.query({
          text: "update public.config set value = $1 where key = $2",
          values: ["two", probeKey],
        });
        const selectResult = await client.query({
          text: "select key, value from public.config where key = $1",
          values: [probeKey],
        });
        const deleteResult = await client.query({
          text: "delete from public.config where key = $1",
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
