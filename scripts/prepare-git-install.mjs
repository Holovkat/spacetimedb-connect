import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";

const npmCommand = process.platform === "win32" ? "npm.cmd" : "npm";

const requiredBuildInputs = [
  "node_modules/typescript/bin/tsc",
  "node_modules/@types/node/package.json",
  "node_modules/dotenv/package.json",
  "node_modules/pg/package.json",
];

function run(args) {
  const result = spawnSync(npmCommand, args, { stdio: "inherit" });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

const hasBuildInputs = requiredBuildInputs.every((path) =>
  existsSync(join(process.cwd(), path))
);

if (!hasBuildInputs) {
  run([
    "install",
    "--include=dev",
    "--include=peer",
    "--include=optional",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
  ]);
}

run(["run", "build"]);
