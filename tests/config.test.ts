import { describe, expect, it } from "vitest";
import { buildDatabaseAuthTokens } from "../src/config.js";

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
});
