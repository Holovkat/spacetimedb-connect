import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type FetchMock = ReturnType<typeof vi.fn>;
const ENV_KEYS = [
  "STDB_BASE_URL",
  "STDB_AUTH_TOKEN",
  "STDB_SOURCE_DATABASE",
  "STDB_DATABASES",
  "OPS_STDB_DB",
  "OPS_STDB_TOKEN",
] as const;

function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
}

describe("StdbClient HTTP discovery", () => {
  let fetchMock: FetchMock;
  let previousEnv: Record<(typeof ENV_KEYS)[number], string | undefined>;

  beforeEach(() => {
    vi.resetModules();
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    previousEnv = Object.fromEntries(
      ENV_KEYS.map((key) => [key, process.env[key]])
    ) as Record<(typeof ENV_KEYS)[number], string | undefined>;
    process.env.STDB_BASE_URL = "http://localhost:3000";
    process.env.STDB_AUTH_TOKEN = "test-token";
    process.env.STDB_SOURCE_DATABASE = "example-app-db";
    delete process.env.STDB_DATABASES;
    delete process.env.OPS_STDB_DB;
    delete process.env.OPS_STDB_TOKEN;
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    for (const key of ENV_KEYS) {
      const value = previousEnv[key];
      if (value === undefined) {
        delete process.env[key];
        continue;
      }
      process.env[key] = value;
    }
  });

  it("fetches database schema over HTTP and caches the response", async () => {
    fetchMock.mockResolvedValue(
      jsonResponse({
        reducers: [{ name: "say_hello", params: { elements: [] } }],
      })
    );

    const { StdbClient } = await import("../src/shim/stdb-client.js");
    const client = new StdbClient();

    const first = await client.describeDatabase("example-app-db");
    const second = await client.describeDatabase("example-app-db");

    expect(first).toEqual(second);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringMatching(/\/v1\/database\/example-app-db\/schema\?version=9$/),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: expect.stringMatching(/^Bearer /),
        }),
      })
    );
  });

  it("derives owned database identities over HTTP using the source database owner", async () => {
    const ownerIdentity = "a".repeat(64);
    const sourceDatabaseIdentity = "d".repeat(64);
    const firstDatabase = "b".repeat(64);
    const secondDatabase = "c".repeat(64);

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          database_identity: { __identity__: `0x${sourceDatabaseIdentity}` },
          owner_identity: { __identity__: `0x${ownerIdentity}` },
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          identities: [firstDatabase, secondDatabase],
        })
      );

    const { StdbClient } = await import("../src/shim/stdb-client.js");
    const client = new StdbClient();

    await expect(client.listDatabaseIdentities()).resolves.toEqual([
      firstDatabase,
      secondDatabase,
      sourceDatabaseIdentity,
    ]);

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(/\/v1\/database\/example-app-db$/),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: expect.stringMatching(/^Bearer /),
        }),
      })
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(
        new RegExp(`/v1/identity/${ownerIdentity}/databases$`)
      )
    );
  });

  it("resolves discovered database names and falls back to identities when names are unavailable", async () => {
    const ownerIdentity = "a".repeat(64);
    const sourceDatabaseIdentity = "d".repeat(64);
    const firstDatabase = "b".repeat(64);
    const secondDatabase = "c".repeat(64);

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          database_identity: sourceDatabaseIdentity,
          owner_identity: ownerIdentity,
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          identities: [firstDatabase, secondDatabase],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          names: ["example-app-db"],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          names: ["example-ops-db"],
        })
      )
      .mockResolvedValueOnce(
        new Response("not found", {
          status: 404,
          statusText: "Not Found",
        })
      );

    const { StdbClient } = await import("../src/shim/stdb-client.js");
    const client = new StdbClient();

    await expect(client.listDiscoveredDatabases()).resolves.toEqual([
      sourceDatabaseIdentity,
      "example-app-db",
      "example-ops-db",
    ]);
  });

  it("accepts legacy address discovery responses", async () => {
    const ownerIdentity = "a".repeat(64);
    const sourceDatabaseIdentity = "d".repeat(64);
    const databaseIdentity = "b".repeat(64);

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          database_identity: sourceDatabaseIdentity,
          owner_identity: ownerIdentity,
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          addresses: [databaseIdentity],
        })
      );

    const { StdbClient } = await import("../src/shim/stdb-client.js");
    const client = new StdbClient();

    await expect(client.listDatabaseIdentities()).resolves.toEqual([
      databaseIdentity,
      sourceDatabaseIdentity,
    ]);
  });

  it("uses configured database names and paired token mappings as discovery seeds", async () => {
    process.env.STDB_DATABASES = "example-explicit-db";
    process.env.OPS_STDB_DB = "example-token-db";
    process.env.OPS_STDB_TOKEN = "ops-token";
    const sourceOwner = "a".repeat(64);
    const explicitOwner = "b".repeat(64);
    const tokenOwner = "c".repeat(64);
    const sourceDatabase = "d".repeat(64);
    const explicitDatabase = "e".repeat(64);
    const tokenDatabase = "f".repeat(64);

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          database_identity: { __identity__: `0x${sourceDatabase}` },
          owner_identity: { __identity__: `0x${sourceOwner}` },
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          database_identity: { __identity__: `0x${explicitDatabase}` },
          owner_identity: { __identity__: `0x${explicitOwner}` },
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          database_identity: { __identity__: `0x${tokenDatabase}` },
          owner_identity: { __identity__: `0x${tokenOwner}` },
        })
      )
      .mockResolvedValueOnce(jsonResponse({ identities: [] }))
      .mockResolvedValueOnce(jsonResponse({ identities: [] }))
      .mockResolvedValueOnce(jsonResponse({ identities: [] }));

    const { StdbClient } = await import("../src/shim/stdb-client.js");
    const client = new StdbClient();

    await expect(client.listDatabaseIdentities()).resolves.toEqual([
      sourceDatabase,
      explicitDatabase,
      tokenDatabase,
    ]);

    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      expect.stringMatching(/\/v1\/database\/example-token-db$/),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer ops-token",
        }),
      })
    );
  });
});
