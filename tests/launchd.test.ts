import { describe, expect, it } from "vitest";
import { buildLaunchdPlist } from "../src/launchd.js";

describe("launchd service plist", () => {
  it("uses product-facing service names and connector logs", () => {
    const plist = buildLaunchdPlist({
      workingDirectory: "/tmp/spacetimedb-connect",
      programArguments: ["/usr/local/bin/node", "/usr/local/bin/spacetimedb-connect", "serve"],
    });

    expect(plist).toContain("com.holovkat.spacetimedb-connect.connector");
    expect(plist).toContain("<string>/tmp/spacetimedb-connect</string>");
    expect(plist).toContain("<string>/usr/local/bin/spacetimedb-connect</string>");
    expect(plist).toContain("connector.log");
    expect(plist).not.toContain("com.holovkat.spacetimedb-connect.pgwire");
  });
});
