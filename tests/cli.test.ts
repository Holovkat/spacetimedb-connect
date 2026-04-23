import { describe, expect, it, vi } from "vitest";
import { buildHelpText, normalizeCliCommand, runCli } from "../src/main.js";

describe("cli packaging surface", () => {
  it("defaults to help when no command is provided", () => {
    expect(normalizeCliCommand(undefined)).toBe("help");
  });

  it("normalizes serve and help aliases", () => {
    expect(normalizeCliCommand("serve")).toBe("serve-pgwire");
    expect(normalizeCliCommand("--help")).toBe("help");
    expect(normalizeCliCommand("-h")).toBe("help");
    expect(normalizeCliCommand("install-service")).toBe("install-service");
    expect(normalizeCliCommand("status")).toBe("status");
  });

  it("renders package help text for the public command surface", () => {
    expect(buildHelpText()).toContain("Usage: spacetimedb-connect <command>");
    expect(buildHelpText()).toContain("serve");
    expect(buildHelpText()).toContain("install-service");
    expect(buildHelpText()).toContain("sync");
  });

  it("prints help without requiring runtime env", async () => {
    const consoleSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    await runCli(undefined);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining("Usage: spacetimedb-connect <command>"));
    consoleSpy.mockRestore();
  });
});
