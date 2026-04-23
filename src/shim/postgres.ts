import { Pool } from "pg";
import { env, getSyncEnv } from "../config.js";
import type { NormalizedTable } from "./types.js";
import { StdbClient } from "./stdb-client.js";

const INSERT_BATCH_SIZE = 250;

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, "\"\"")}"`;
}

export async function ensureTargetDatabase(
  targetDatabase = getSyncEnv().PG_TARGET_DATABASE
): Promise<void> {
  const pool = new Pool({ connectionString: getSyncEnv().PG_SUPER_URL });
  try {
    const exists = await pool.query(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [targetDatabase]
    );

    if (exists.rowCount === 0) {
      await pool.query(`CREATE DATABASE ${quoteIdentifier(targetDatabase)}`);
    }
  } finally {
    await pool.end();
  }
}

export function createTargetPool(
  targetDatabase = getSyncEnv().PG_TARGET_DATABASE
): Pool {
  const superUrl = new URL(getSyncEnv().PG_SUPER_URL);
  superUrl.pathname = `/${targetDatabase}`;
  return new Pool({ connectionString: superUrl.toString() });
}

export async function refreshTable(
  pool: Pool,
  normalized: NormalizedTable,
  sourceDatabase = env.STDB_SOURCE_DATABASE
): Promise<void> {
  const syncEnv = getSyncEnv();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      `CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(syncEnv.PG_TARGET_SCHEMA)}`
    );

    const qualifiedTable = `${quoteIdentifier(syncEnv.PG_TARGET_SCHEMA)}.${quoteIdentifier(normalized.tableName)}`;

    await client.query(`DROP TABLE IF EXISTS ${qualifiedTable}`);

    const columnDefs = normalized.columns.map(
      (column) => `${quoteIdentifier(column.pgName)} ${column.pgType}`
    );
    columnDefs.push(`_shim_source_database text not null`);
    columnDefs.push(`_shim_synced_at timestamptz not null`);
    columnDefs.push(`_shim_row_hash text not null`);

    await client.query(
      `CREATE TABLE ${qualifiedTable} (${columnDefs.join(", ")})`
    );

    if (normalized.rows.length > 0) {
      const columnNames = normalized.columns.map((column) => column.pgName);
      const allColumns = [
        ...columnNames,
        "_shim_source_database",
        "_shim_synced_at",
        "_shim_row_hash",
      ];

      const syncedAt = new Date().toISOString();

      for (
        let startIndex = 0;
        startIndex < normalized.rows.length;
        startIndex += INSERT_BATCH_SIZE
      ) {
        const batch = normalized.rows.slice(
          startIndex,
          startIndex + INSERT_BATCH_SIZE
        );
        const values: unknown[] = [];
        const valueGroups = batch.map((row, rowIndex) => {
          const rowValues = [
            ...columnNames.map((columnName) => row[columnName] ?? null),
            sourceDatabase,
            syncedAt,
            StdbClient.rowHash(row),
          ];
          values.push(...rowValues);
          const offset = rowIndex * allColumns.length;
          const placeholders = rowValues.map((_, columnIndex) => {
            return `$${offset + columnIndex + 1}`;
          });
          return `(${placeholders.join(", ")})`;
        });

        await client.query(
          `INSERT INTO ${qualifiedTable} (${allColumns
            .map(quoteIdentifier)
            .join(", ")}) VALUES ${valueGroups.join(", ")}`,
          values
        );
      }
    }

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
