import { pathToFileURL } from "node:url";
import { env, getSyncEnv, parseTableList } from "./config.js";
import { listenPgwireServer } from "./pgwire/server.js";
import { normalizeResult } from "./shim/normalize.js";
import { createTargetPool, ensureTargetDatabase, refreshTable } from "./shim/postgres.js";
import { StdbClient } from "./shim/stdb-client.js";

function filterTables(tableNames: string[]): string[] {
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
  const filtered = filterTables(tables.map((table) => table.tableName));

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
  console.log(`Syncing ${sourceDatabase} -> ${targetDatabase}`);

  await ensureTargetDatabase(targetDatabase);
  const pool = createTargetPool(targetDatabase);
  const stdbClient = new StdbClient();
  const tableNames = filterTables(
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
  await syncDatabase(env.STDB_SOURCE_DATABASE, getSyncEnv().PG_TARGET_DATABASE);
}

export async function syncAll(): Promise<void> {
  const stdbClient = new StdbClient();
  const databaseNames = await discoverDatabases(stdbClient);

  console.log(`Discovered ${databaseNames.length} Spacetime databases`);

  for (const databaseName of databaseNames) {
    await syncDatabase(databaseName, databaseName);
  }
}

export async function servePgwire(): Promise<void> {
  const server = await listenPgwireServer();
  console.log(
    `pgwire pass-through listening on ${env.PGWIRE_HOST}:${env.PGWIRE_PORT}`
  );

  await new Promise<void>((resolve, reject) => {
    server.once("close", resolve);
    server.once("error", reject);
  });
}

export async function runCli(command = process.argv[2] ?? "sync"): Promise<void> {
  if (command === "sync") {
    await sync();
    return;
  }

  if (command === "list-tables") {
    const tableNames = await discoverTables(new StdbClient());
    console.log(tableNames.join("\n"));
    return;
  }

  if (command === "list-databases") {
    const databaseNames = await discoverDatabases(new StdbClient());
    console.log(databaseNames.join("\n"));
    return;
  }

  if (command === "sync-all") {
    await syncAll();
    return;
  }

  if (command === "serve-pgwire") {
    await servePgwire();
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runCli();
}
