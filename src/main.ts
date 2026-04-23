#!/usr/bin/env node
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import type { StdbClient } from "./shim/stdb-client.js";

const PUBLIC_COMMANDS = [
  "help",
  "serve",
  "list-databases",
  "list-tables",
  "sync",
  "sync-all",
] as const;

type CliCommand = (typeof PUBLIC_COMMANDS)[number] | "serve-pgwire";

async function createStdbClient(): Promise<StdbClient> {
  const { StdbClient } = await import("./shim/stdb-client.js");
  return new StdbClient();
}

async function filterTables(tableNames: string[]): Promise<string[]> {
  const { env, parseTableList } = await import("./config.js");
  const includes = parseTableList(env.SHIM_INCLUDE_TABLES);
  const excludes = new Set(parseTableList(env.SHIM_EXCLUDE_TABLES));
  const selected =
    includes.length > 0
      ? tableNames.filter((tableName) => includes.includes(tableName))
      : tableNames;

  return selected.filter((tableName) => !excludes.has(tableName));
}

export async function discoverTables(stdbClient: StdbClient): Promise<string[]> {
  const tables = await stdbClient.listPublicTables();
  const filtered = await filterTables(tables.map((table) => table.tableName));

  if (filtered.length === 0) {
    throw new Error("No tables selected for sync");
  }

  return filtered;
}

export async function discoverDatabases(stdbClient: StdbClient): Promise<string[]> {
  const databases = await stdbClient.listDatabases();
  const reachable: string[] = [];

  for (const databaseName of databases) {
    if (await stdbClient.databaseExists(databaseName)) {
      reachable.push(databaseName);
    }
  }

  if (reachable.length === 0) {
    throw new Error("No reachable Spacetime databases found");
  }

  return reachable;
}

export async function syncDatabase(sourceDatabase: string, targetDatabase = sourceDatabase): Promise<void> {
  const [{ normalizeResult }, { createTargetPool, ensureTargetDatabase, refreshTable }, { StdbClient }] =
    await Promise.all([
      import("./shim/normalize.js"),
      import("./shim/postgres.js"),
      import("./shim/stdb-client.js"),
    ]);

  console.log(`Syncing ${sourceDatabase} -> ${targetDatabase}`);

  await ensureTargetDatabase(targetDatabase);
  const pool = createTargetPool(targetDatabase);
  const stdbClient = new StdbClient();
  const tableNames = await filterTables(
    (await stdbClient.listPublicTables(sourceDatabase)).map((table) => table.tableName)
  );

  if (tableNames.length === 0) {
    throw new Error(`No tables selected for sync in ${sourceDatabase}`);
  }

  try {
    console.log(`Discovered ${tableNames.length} public user tables`);

    for (const tableName of tableNames) {
      console.log(`Refreshing ${tableName}...`);
      const result = await stdbClient.selectAll(tableName, sourceDatabase);
      const normalized = normalizeResult(tableName, result);
      await refreshTable(pool, normalized, sourceDatabase);
      console.log(`Refreshed ${tableName}: ${normalized.rows.length} rows`);
    }
  } finally {
    await pool.end();
  }
}

export async function sync(): Promise<void> {
  const { env, getSyncEnv } = await import("./config.js");
  await syncDatabase(env.STDB_SOURCE_DATABASE, getSyncEnv().PG_TARGET_DATABASE);
}

export async function syncAll(): Promise<void> {
  const stdbClient = await createStdbClient();
  const databaseNames = await discoverDatabases(stdbClient);

  console.log(`Discovered ${databaseNames.length} Spacetime databases`);

  for (const databaseName of databaseNames) {
    await syncDatabase(databaseName, databaseName);
  }
}

export async function servePgwire(): Promise<void> {
  const [{ env }, { listenPgwireServer }] = await Promise.all([
    import("./config.js"),
    import("./pgwire/server.js"),
  ]);
  const server = await listenPgwireServer();
  console.log(
    `pgwire pass-through listening on ${env.PGWIRE_HOST}:${env.PGWIRE_PORT}`
  );

  await new Promise<void>((resolve, reject) => {
    server.once("close", resolve);
    server.once("error", reject);
  });
}

export function buildHelpText(): string {
  return [
    "Usage: spacetimedb-connect <command>",
    "",
    "Commands:",
    "  serve            Start the pgwire connector server",
    "  list-databases   List reachable SpacetimeDB databases",
    "  list-tables      List selected public tables from the source database",
    "  sync             Optional Postgres debug/alignment sync for the source database",
    "  sync-all         Optional Postgres debug/alignment sync for all discovered databases",
    "  help             Show this help output",
    "",
    "Notes:",
    "  No command prints this help instead of starting sync mode.",
    "  The sync commands are optional debugging/alignment paths and still require Postgres config.",
  ].join("\n");
}

export function normalizeCliCommand(command?: string): CliCommand | null {
  if (!command || command === "help" || command === "-h" || command === "--help") {
    return "help";
  }

  if (command === "serve") {
    return "serve-pgwire";
  }

  if (
    command === "serve-pgwire" ||
    PUBLIC_COMMANDS.includes(command as (typeof PUBLIC_COMMANDS)[number])
  ) {
    return command as CliCommand;
  }

  return null;
}

export async function runCli(command?: string): Promise<void> {
  const normalizedCommand = normalizeCliCommand(command);

  if (!normalizedCommand) {
    throw new Error(`Unknown command: ${command}`);
  }

  if (normalizedCommand === "help") {
    console.log(buildHelpText());
    return;
  }

  if (normalizedCommand === "sync") {
    await sync();
    return;
  }

  if (normalizedCommand === "list-tables") {
    const tableNames = await discoverTables(await createStdbClient());
    console.log(tableNames.join("\n"));
    return;
  }

  if (normalizedCommand === "list-databases") {
    const databaseNames = await discoverDatabases(await createStdbClient());
    console.log(databaseNames.join("\n"));
    return;
  }

  if (normalizedCommand === "sync-all") {
    await syncAll();
    return;
  }

  if (normalizedCommand === "serve-pgwire") {
    await servePgwire();
    return;
  }
}

function isDirectCliInvocation(): boolean {
  if (!process.argv[1]) {
    return false;
  }

  try {
    return (
      realpathSync(process.argv[1]) ===
      realpathSync(fileURLToPath(import.meta.url))
    );
  } catch {
    return false;
  }
}

if (isDirectCliInvocation()) {
  try {
    await runCli(process.argv[2]);
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
