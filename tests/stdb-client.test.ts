import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type FetchMock = ReturnType<typeof vi.fn>;

function jsonResponse(body: unknown, init?: ResponseInit): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
}

describe("StdbClient HTTP discovery", () => {
  let fetchMock: FetchMock;

  beforeEach(() => {
    vi.resetModules();
    fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
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
      expect.stringMatching(/\/v1\/database\/example-app-db\/schema$/),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: expect.stringMatching(/^Bearer /),
        }),
      })
    );
  });

  it("derives owned database identities over HTTP using the caller identity header", async () => {
    const callerIdentity = "a".repeat(64);
    const firstDatabase = "b".repeat(64);
    const secondDatabase = "c".repeat(64);

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse(
          { reducers: [] },
          {
            headers: {
              "spacetime-identity": callerIdentity,
            },
          }
        )
      )
      .mockResolvedValueOnce(
        jsonResponse({
          addresses: [firstDatabase, secondDatabase],
        })
      );

    const { StdbClient } = await import("../src/shim/stdb-client.js");
    const client = new StdbClient();

    await expect(client.listDatabaseIdentities()).resolves.toEqual([
      firstDatabase,
      secondDatabase,
    ]);

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      expect.stringMatching(/\/v1\/database\/.*\/schema$/),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: expect.stringMatching(/^Bearer /),
        }),
      })
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      expect.stringMatching(
        new RegExp(`/v1/identity/${callerIdentity}/databases$`)
      )
    );
  });

  it("resolves discovered database names and falls back to identities when names are unavailable", async () => {
    const callerIdentity = "a".repeat(64);
    const firstDatabase = "b".repeat(64);
    const secondDatabase = "c".repeat(64);

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse(
          { reducers: [] },
          {
            headers: {
              "spacetime-identity": callerIdentity,
            },
          }
        )
      )
      .mockResolvedValueOnce(
        jsonResponse({
          addresses: [firstDatabase, secondDatabase],
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          names: ["example-app-db"],
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
      secondDatabase,
      "example-app-db",
    ]);
  });
});
