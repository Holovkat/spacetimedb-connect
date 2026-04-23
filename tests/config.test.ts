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
        FMS_GLM_STDB_TT_DB: "fms-glm-org-tt",
        FMS_GLM_STDB_TT_TOKEN: "tt-token",
        FMS_GLM_STDB_ROAD_RUNNER_DB: "fms-glm-org-road-runner",
        FMS_GLM_STDB_ROAD_RUNNER_TOKEN: "rr-token",
        PG_TARGET_DATABASE: "ignored",
      })
    ).toEqual({
      "fms-glm-org-road-runner": "rr-token",
      "fms-glm-org-tt": "tt-token",
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
