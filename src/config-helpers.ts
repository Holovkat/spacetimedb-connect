import { z } from "zod";

const baseEnvSchema = z.object({
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
});

const syncEnvSchema = z.object({
  PG_SUPER_URL: z.string().min(1),
  PG_TARGET_DATABASE: z.string().min(1),
  PG_TARGET_SCHEMA: z.string().min(1).default("public"),
});

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

export function parseBaseEnv(source: Record<string, string | undefined>) {
  return baseEnvSchema.parse({
    STDB_BASE_URL: source.STDB_BASE_URL,
    STDB_AUTH_TOKEN: source.STDB_AUTH_TOKEN,
    STDB_ADMIN_AUTH_TOKEN:
      source.STDB_ADMIN_AUTH_TOKEN ?? source.FMS_GLM_STDB_ADMIN_TOKEN,
    STDB_SOURCE_DATABASE: source.STDB_SOURCE_DATABASE,
    STDB_SOURCE_SCHEMA: source.STDB_SOURCE_SCHEMA ?? "public",
    STDB_DATABASES: source.STDB_DATABASES,
    STDB_DISCOVERY_SERVER: source.STDB_DISCOVERY_SERVER ?? source.STDB_BASE_URL,
    PGWIRE_HOST: source.PGWIRE_HOST ?? "127.0.0.1",
    PGWIRE_PORT: source.PGWIRE_PORT ?? 45434,
    SHIM_INCLUDE_TABLES: source.SHIM_INCLUDE_TABLES,
    SHIM_EXCLUDE_TABLES: source.SHIM_EXCLUDE_TABLES,
  });
}

export function parseSyncEnv(
  source: Record<string, string | undefined>,
  sourceDatabase: string
) {
  return syncEnvSchema.parse({
    PG_SUPER_URL: source.PG_SUPER_URL,
    PG_TARGET_DATABASE: source.PG_TARGET_DATABASE ?? sourceDatabase,
    PG_TARGET_SCHEMA: source.PG_TARGET_SCHEMA ?? "public",
  });
}
