import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { env, parseCsvList, resolveDatabaseAuthToken } from "../config.js";
import { mapAlgebraicTypeToPgType } from "./normalize.js";
import type {
  StdbDatabaseSchema,
  StdbQueryResult,
  StdbRoutineRow,
  StdbTableRow,
} from "./types.js";

const execFileAsync = promisify(execFile);
const DATABASE_IDENTITY_PATTERN = /^[a-f0-9]{64}$/i;
const DATABASE_NAMES_ENDPOINT_PATTERN =
  /\/v1\/database\/[^/]+\/names$/i;

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, "\"\"")}"`;
}

export class StdbClient {
  private readonly schemaCache = new Map<string, Promise<StdbDatabaseSchema>>();

  async query(
    sql: string,
    databaseName = env.STDB_SOURCE_DATABASE,
    options?: { preferAdmin?: boolean }
  ): Promise<StdbQueryResult[]> {
    const response = await fetch(
      `${env.STDB_BASE_URL}/v1/database/${databaseName}/sql`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${resolveDatabaseAuthToken(
            databaseName,
            options?.preferAdmin ?? false
          )}`,
          "Content-Type": "text/plain",
        },
        body: sql,
      }
    );

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `Spacetime SQL failed (${response.status} ${response.statusText}): ${body}`
      );
    }

    return (await response.json()) as StdbQueryResult[];
  }

  async selectAll(
    tableName: string,
    databaseName = env.STDB_SOURCE_DATABASE
  ): Promise<StdbQueryResult> {
    const result = await this.query(
      `SELECT * FROM ${quoteIdentifier(tableName)}`,
      databaseName
    );
    if (result.length !== 1) {
      throw new Error(`Expected single result set for ${tableName}`);
    }
    return result[0];
  }

  async selectSample(
    tableName: string,
    limit = 1,
    databaseName = env.STDB_SOURCE_DATABASE
  ): Promise<StdbQueryResult> {
    const result = await this.query(
      `SELECT * FROM ${quoteIdentifier(tableName)} LIMIT ${limit}`,
      databaseName
    );
    if (result.length !== 1) {
      throw new Error(`Expected single result set for ${tableName}`);
    }
    return result[0];
  }

  async describeTable(
    tableName: string,
    databaseName = env.STDB_SOURCE_DATABASE
  ): Promise<StdbQueryResult> {
    const result = await this.query(
      `SELECT * FROM ${quoteIdentifier(tableName)} LIMIT 0`,
      databaseName
    );
    if (result.length !== 1) {
      throw new Error(`Expected single result set for ${tableName}`);
    }
    return result[0];
  }

  async listPublicTables(
    databaseName = env.STDB_SOURCE_DATABASE
  ): Promise<StdbTableRow[]> {
    const result = await this.query(
      "SELECT * FROM st_table WHERE table_type = 'user' AND table_access = 'public'",
      databaseName
    );

    if (result.length !== 1) {
      throw new Error("Expected a single result set from st_table");
    }

    return result[0].rows
      .map((row) => ({
        tableId: Number(row[0]),
        tableName: String(row[1]),
        tableType: String(row[2]),
        tableAccess: String(row[3]),
      }))
      .sort((left, right) => left.tableName.localeCompare(right.tableName));
  }

  async listRoutines(
    databaseName = env.STDB_SOURCE_DATABASE
  ): Promise<StdbRoutineRow[]> {
    const schema = await this.describeDatabase(databaseName);
    return (schema.reducers ?? [])
      .map((reducer) => ({
        routineName: reducer.name,
        routineType: "PROCEDURE" as const,
        parameters: (reducer.params?.elements ?? []).map((element) => ({
          name: element.name?.some ?? null,
          pgType: mapAlgebraicTypeToPgType(element.algebraic_type),
        })),
      }))
      .sort((left, right) => left.routineName.localeCompare(right.routineName));
  }

  async listDatabases(): Promise<string[]> {
    const configured = parseCsvList(env.STDB_DATABASES);
    const discovered = await this.listDiscoveredDatabases();
    const combined = [...configured, ...discovered];
    const unique = new Set<string>();

    for (const databaseName of combined) {
      const trimmed = databaseName.trim();
      if (!trimmed) {
        continue;
      }
      unique.add(trimmed);
    }

    if (unique.size === 0) {
      unique.add(env.STDB_SOURCE_DATABASE);
    }

    return [...unique].sort((left, right) => left.localeCompare(right));
  }

  async databaseExists(databaseName: string): Promise<boolean> {
    try {
      await this.query("SELECT * FROM st_table LIMIT 1", databaseName);
      return true;
    } catch {
      return false;
    }
  }

  async listDiscoveredDatabases(): Promise<string[]> {
    const identities = await this.listDatabaseIdentities();
    const databaseNames = new Set<string>();

    for (const identity of identities) {
      const names = await this.resolveDatabaseNames(identity);
      if (names.length === 0) {
        databaseNames.add(identity);
        continue;
      }

      names.forEach((name) => databaseNames.add(name));
    }

    return [...databaseNames].sort((left, right) => left.localeCompare(right));
  }

  async listDatabaseIdentities(): Promise<string[]> {
    try {
      const { stdout } = await execFileAsync("spacetime", [
        "list",
        "--server",
        env.STDB_DISCOVERY_SERVER ?? env.STDB_BASE_URL,
        "-y",
      ]);

      return stdout
        .split("\n")
        .map((line) => line.trim())
        .filter((line) => DATABASE_IDENTITY_PATTERN.test(line));
    } catch {
      return [];
    }
  }

  async resolveDatabaseNames(nameOrIdentity: string): Promise<string[]> {
    const response = await fetch(
      `${this.databaseApiBaseUrl(nameOrIdentity)}/names`,
      {
        headers: {
          Authorization: `Bearer ${env.STDB_AUTH_TOKEN}`,
        },
      }
    );

    if (!response.ok) {
      return [];
    }

    const body = (await response.json()) as { names?: unknown };
    if (!Array.isArray(body.names)) {
      return [];
    }

    return body.names
      .map((entry) => String(entry).trim())
      .filter(Boolean);
  }

  async describeDatabase(
    databaseName = env.STDB_SOURCE_DATABASE
  ): Promise<StdbDatabaseSchema> {
    const cached = this.schemaCache.get(databaseName);
    if (cached) {
      return cached;
    }

    const pending = this.fetchDatabaseSchema(databaseName);
    this.schemaCache.set(databaseName, pending);

    try {
      return await pending;
    } catch (error) {
      this.schemaCache.delete(databaseName);
      throw error;
    }
  }

  private databaseApiBaseUrl(nameOrIdentity: string): string {
    const normalizedBaseUrl = env.STDB_BASE_URL.replace(/\/+$/, "");
    if (DATABASE_NAMES_ENDPOINT_PATTERN.test(normalizedBaseUrl)) {
      return normalizedBaseUrl.replace(/\/names$/i, "");
    }
    return `${normalizedBaseUrl}/v1/database/${encodeURIComponent(nameOrIdentity)}`;
  }

  private async fetchDatabaseSchema(
    databaseName: string
  ): Promise<StdbDatabaseSchema> {
    const { stdout } = await execFileAsync(
      "spacetime",
      [
        "describe",
        "--json",
        "--server",
        env.STDB_DISCOVERY_SERVER ?? env.STDB_BASE_URL,
        "-y",
        databaseName,
      ],
      {
        maxBuffer: 32 * 1024 * 1024,
      }
    );

    return JSON.parse(stdout) as StdbDatabaseSchema;
  }

  static rowHash(row: Record<string, unknown>): string {
    return createHash("sha256")
      .update(JSON.stringify(row))
      .digest("hex");
  }
}
