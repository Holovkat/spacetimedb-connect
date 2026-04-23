import "dotenv/config";
import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import dotenv from "dotenv";
import { z } from "zod";

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

const envSchema = z.object({
  STDB_BASE_URL: z.url(),
  STDB_AUTH_TOKEN: z.string().min(1),
  STDB_ADMIN_AUTH_TOKEN: z.string().min(1).optional(),
  STDB_SOURCE_DATABASE: z.string().min(1),
  STDB_SOURCE_SCHEMA: z.string().min(1).default("public"),
  STDB_DATABASES: z.string().optional(),
  STDB_DISCOVERY_SERVER: z.string().min(1).optional(),
  PGWIRE_HOST: z.string().min(1).default("127.0.0.1"),
  PGWIRE_PORT: z.coerce.number().int().positive().default(45434),
  SHIM_INCLUDE_TABLES: z.string().optional(),
  SHIM_EXCLUDE_TABLES: z.string().optional(),
  PG_SUPER_URL: z.string().min(1),
  PG_TARGET_DATABASE: z.string().min(1),
  PG_TARGET_SCHEMA: z.string().min(1).default("public"),
});

const targetDatabase =
  mergedEnv.PG_TARGET_DATABASE ?? mergedEnv.STDB_SOURCE_DATABASE;

export function buildDatabaseAuthTokens(
  source: Record<string, string | undefined>
): Record<string, string> {
  const tokens: Record<string, string> = {};

  for (const [key, value] of Object.entries(source)) {
    if (!key.endsWith("_DB") || !key.includes("STDB") || !value) {
      continue;
    }

    const token = source[`${key.slice(0, -3)}_TOKEN`];
    if (!token) {
      continue;
    }

    tokens[value] = token;
  }

  return tokens;
}

const databaseAuthTokens = buildDatabaseAuthTokens(mergedEnv);

const parsedEnv = envSchema.parse({
  STDB_BASE_URL: mergedEnv.STDB_BASE_URL,
  STDB_AUTH_TOKEN: mergedEnv.STDB_AUTH_TOKEN,
  STDB_ADMIN_AUTH_TOKEN:
    mergedEnv.STDB_ADMIN_AUTH_TOKEN ?? mergedEnv.FMS_GLM_STDB_ADMIN_TOKEN,
  STDB_SOURCE_DATABASE: mergedEnv.STDB_SOURCE_DATABASE,
  STDB_SOURCE_SCHEMA: mergedEnv.STDB_SOURCE_SCHEMA ?? "public",
  STDB_DATABASES: mergedEnv.STDB_DATABASES,
  STDB_DISCOVERY_SERVER:
    mergedEnv.STDB_DISCOVERY_SERVER ?? mergedEnv.STDB_BASE_URL,
  PGWIRE_HOST: mergedEnv.PGWIRE_HOST ?? "127.0.0.1",
  PGWIRE_PORT: mergedEnv.PGWIRE_PORT ?? 45434,
  SHIM_INCLUDE_TABLES: mergedEnv.SHIM_INCLUDE_TABLES,
  SHIM_EXCLUDE_TABLES: mergedEnv.SHIM_EXCLUDE_TABLES,
  PG_SUPER_URL: mergedEnv.PG_SUPER_URL,
  PG_TARGET_DATABASE: targetDatabase,
  PG_TARGET_SCHEMA: mergedEnv.PG_TARGET_SCHEMA ?? "public",
});

export const env = {
  ...parsedEnv,
  STDB_DATABASE_AUTH_TOKENS: databaseAuthTokens,
} as const;

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
