import "dotenv/config";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import dotenv from "dotenv";
import {
  buildDatabaseAuthTokens,
  parseBaseEnv,
  parseSyncEnv,
} from "./config-helpers.js";

function loadSecureEnv(): Record<string, string> {
  const secureEnvPath = join(homedir(), ".secure/.env");
  if (!existsSync(secureEnvPath)) {
    return {};
  }

  return dotenv.parse(readFileSync(secureEnvPath));
}

const mergedEnv = {
  ...loadSecureEnv(),
  ...process.env,
};

const databaseAuthTokens = buildDatabaseAuthTokens(mergedEnv);

const parsedEnv = parseBaseEnv(mergedEnv);
let parsedSyncEnv: ReturnType<typeof parseSyncEnv> | undefined;

export const env = {
  ...parsedEnv,
  STDB_DATABASE_AUTH_TOKENS: databaseAuthTokens,
} as const;

export function getSyncEnv() {
  parsedSyncEnv ??= parseSyncEnv(mergedEnv, env.STDB_SOURCE_DATABASE);
  return parsedSyncEnv;
}

export { buildDatabaseAuthTokens, parseBaseEnv, parseSyncEnv };

export function resolveDatabaseAuthToken(
  databaseName: string,
  preferAdmin = false
): string {
  if (preferAdmin && parsedEnv.STDB_ADMIN_AUTH_TOKEN) {
    return parsedEnv.STDB_ADMIN_AUTH_TOKEN;
  }

  return env.STDB_DATABASE_AUTH_TOKENS[databaseName] ?? parsedEnv.STDB_AUTH_TOKEN;
}

export function parseTableList(value: string | undefined): string[] {
  if (!value) {
    return [];
  }

  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export const parseCsvList = parseTableList;
