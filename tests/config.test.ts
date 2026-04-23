import { describe, expect, it } from "vitest";
import {
  buildDatabaseAuthTokens,
  parseBaseEnv,
  parseSyncEnv,
} from "../src/config-helpers.js";

describe("config helpers", () => {
  it("builds database token mappings from paired *_DB and *_TOKEN keys", () => {
    expect(
      buildDatabaseAuthTokens({
        EXAMPLE_APP_STDB_MAIN_DB: "example-app-db",
        EXAMPLE_APP_STDB_MAIN_TOKEN: "main-token",
        EXAMPLE_APP_STDB_AUDIT_DB: "example-audit-db",
        EXAMPLE_APP_STDB_AUDIT_TOKEN: "audit-token",
        PG_TARGET_DATABASE: "ignored",
      })
    ).toEqual({
      "example-app-db": "main-token",
      "example-audit-db": "audit-token",
    });
  });

  it("parses base env without requiring sync-to-postgres settings", () => {
    expect(
      parseBaseEnv({
        STDB_BASE_URL: "http://localhost:6900",
        STDB_AUTH_TOKEN: "token",
        STDB_SOURCE_DATABASE: "example-app-db",
      })
    ).toMatchObject({
      STDB_BASE_URL: "http://localhost:6900",
      STDB_AUTH_TOKEN: "token",
      STDB_SOURCE_DATABASE: "example-app-db",
      PGWIRE_HOST: "127.0.0.1",
      PGWIRE_PORT: 45434,
    });
  });

  it("ignores tenant-specific admin token fallbacks", () => {
    expect(
      parseBaseEnv({
        STDB_BASE_URL: "http://localhost:6900",
        STDB_AUTH_TOKEN: "token",
        EXAMPLE_APP_STDB_ADMIN_TOKEN: "legacy-token",
        STDB_SOURCE_DATABASE: "example-app-db",
      })
    ).toMatchObject({
      STDB_BASE_URL: "http://localhost:6900",
      STDB_AUTH_TOKEN: "token",
      STDB_ADMIN_AUTH_TOKEN: undefined,
      STDB_SOURCE_DATABASE: "example-app-db",
    });
  });

  it("defaults the sync target database to the source database", () => {
    expect(
      parseSyncEnv(
        {
          PG_SUPER_URL: "postgresql://shim:shim@127.0.0.1:45433/postgres",
        },
        "example-app-db"
      )
    ).toMatchObject({
      PG_SUPER_URL: "postgresql://shim:shim@127.0.0.1:45433/postgres",
      PG_TARGET_DATABASE: "example-app-db",
      PG_TARGET_SCHEMA: "public",
    });
  });
});
