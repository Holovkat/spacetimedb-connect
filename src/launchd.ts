import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { homedir } from "node:os";
import { join, resolve } from "node:path";
import { realpathSync } from "node:fs";

export type LaunchdAction =
  | "install"
  | "uninstall"
  | "restart"
  | "status"
  | "print-plist";

const label = "com.holovkat.spacetimedb-connect.connector";
const legacyLabels = ["com.holovkat.spacetimedb-connect.pgwire"];
const launchAgentsDir = join(homedir(), "Library", "LaunchAgents");
const logsDir = join(homedir(), "Library", "Logs", "spacetimedb-connect");
const plistPath = join(launchAgentsDir, `${label}.plist`);

const pathValue = [
  "/opt/homebrew/bin",
  "/usr/local/bin",
  "/usr/bin",
  "/bin",
  "/usr/sbin",
  "/sbin",
].join(":");

function assertMacos(): void {
  if (process.platform !== "darwin") {
    throw new Error(
      "launchd service management is only supported on macOS. Use Docker or run `spacetimedb-connect serve` directly on this platform."
    );
  }
}

function currentGuiDomain(): string {
  if (!process.getuid) {
    throw new Error("Cannot resolve the current macOS user id");
  }

  return `gui/${process.getuid()}`;
}

function xmlEscape(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function currentCliArguments(): string[] {
  if (!process.argv[1]) {
    throw new Error("Cannot resolve the spacetimedb-connect CLI path");
  }

  return [process.execPath, realpathSync(process.argv[1]), "serve"];
}

export function buildLaunchdPlist({
  workingDirectory = process.cwd(),
  programArguments = currentCliArguments(),
}: {
  workingDirectory?: string;
  programArguments?: string[];
} = {}): string {
  const escapedWorkingDirectory = xmlEscape(resolve(workingDirectory));
  const escapedPath = xmlEscape(pathValue);
  const escapedLog = xmlEscape(join(logsDir, "connector.log"));
  const escapedErrorLog = xmlEscape(join(logsDir, "connector.error.log"));
  const programArgumentXml = programArguments
    .map((argument) => `    <string>${xmlEscape(argument)}</string>`)
    .join("\n");

  return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${label}</string>
  <key>WorkingDirectory</key>
  <string>${escapedWorkingDirectory}</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>${escapedPath}</string>
  </dict>
  <key>ProgramArguments</key>
  <array>
${programArgumentXml}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>${escapedLog}</string>
  <key>StandardErrorPath</key>
  <string>${escapedErrorLog}</string>
</dict>
</plist>
`;
}

function runLaunchctl(args: string[], { allowFailure = false } = {}): string {
  const result = spawnSync("launchctl", args, { encoding: "utf8" });
  const failed = result.status !== 0 && !allowFailure;

  if (failed) {
    const detail = result.stderr || result.stdout || `exit ${result.status}`;
    throw new Error(`launchctl ${args.join(" ")} failed: ${detail.trim()}`);
  }

  return result.stdout || result.stderr || "";
}

function ensureDirs(): void {
  mkdirSync(launchAgentsDir, { recursive: true });
  mkdirSync(logsDir, { recursive: true });
}

function bootout(plist: string): void {
  runLaunchctl(["bootout", currentGuiDomain(), plist], { allowFailure: true });
}

function removeLegacyAgents(): void {
  for (const legacyLabel of legacyLabels) {
    const legacyPath = join(launchAgentsDir, `${legacyLabel}.plist`);
    bootout(legacyPath);
    if (existsSync(legacyPath)) {
      rmSync(legacyPath);
    }
  }
}

function install(): string {
  ensureDirs();
  bootout(plistPath);
  removeLegacyAgents();
  writeFileSync(plistPath, buildLaunchdPlist(), "utf8");
  runLaunchctl(["bootstrap", currentGuiDomain(), plistPath]);
  runLaunchctl(["enable", `${currentGuiDomain()}/${label}`]);
  runLaunchctl(["kickstart", "-k", `${currentGuiDomain()}/${label}`]);
  return [
    `Installed and started ${label}`,
    `Logs: ${join(logsDir, "connector.log")}`,
  ].join("\n");
}

function uninstall(): string {
  bootout(plistPath);
  if (existsSync(plistPath)) {
    rmSync(plistPath);
  }
  return `Uninstalled ${label}`;
}

function restart(): string {
  if (!existsSync(plistPath)) {
    return install();
  }

  runLaunchctl(["kickstart", "-k", `${currentGuiDomain()}/${label}`]);
  return `Restarted ${label}`;
}

function status(): string {
  if (!existsSync(plistPath)) {
    return `${label} is not installed`;
  }

  return (
    runLaunchctl(["print", `${currentGuiDomain()}/${label}`], { allowFailure: true }).trim() ||
    `${label} is installed but launchctl returned no status`
  );
}

export function runLaunchdAction(action: LaunchdAction): string {
  assertMacos();

  const actions: Record<LaunchdAction, () => string> = {
    install,
    uninstall,
    restart,
    status,
    "print-plist": () => buildLaunchdPlist(),
  };

  return actions[action]();
}
