import { createHash } from "node:crypto";
import { env, parseCsvList, resolveDatabaseAuthToken } from "../config.js";
import { mapAlgebraicTypeToPgType } from "./normalize.js";
import type {
  StdbDatabaseSchema,
  StdbQueryResult,
  StdbRoutineRow,
  StdbTableRow,
} from "./types.js";

const IDENTITY_PATTERN = /^[a-f0-9]{64}$/i;
const API_ENDPOINT_PATTERN =
  /\/v1\/(?:database\/[^/]+(?:\/(?:identity|logs|names|schema|sql))?|identity\/[^/]+\/(?:databases|verify))$/i;
const RAW_MODULE_DEF_VERSION = 9;

type StdbDatabaseMetadata = {
  database_identity?: unknown;
  owner_identity?: unknown;
};

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, "\"\"")}"`;
}

function normalizeIdentity(value: unknown): string | null {
  const identityObject =
    value && typeof value === "object"
      ? (value as { __identity__?: unknown })
      : null;
  const candidate =
    typeof value === "string"
      ? value
      : typeof identityObject?.__identity__ === "string"
        ? identityObject.__identity__
        : null;

  const identity = candidate?.trim().replace(/^0x/i, "") ?? "";
  return IDENTITY_PATTERN.test(identity) ? identity : null;
}

function normalizeIdentityList(entries: unknown): string[] {
  if (!Array.isArray(entries)) {
    return [];
  }

  return entries
    .map((entry) => normalizeIdentity(entry))
    .filter((entry): entry is string => entry !== null);
}

function uniqueSorted(values: Iterable<string>): string[] {
  return [...new Set([...values].map((value) => value.trim()).filter(Boolean))]
    .sort((left, right) => left.localeCompare(right));
}

export class StdbClient {
  private readonly schemaCache = new Map<string, Promise<StdbDatabaseSchema>>();

  async query(
    sql: string,
    databaseName = env.STDB_SOURCE_DATABASE,
    options?: { preferAdmin?: boolean }
  ): Promise<StdbQueryResult[]> {
    const response = await fetch(
      `${this.queryApiBaseUrl(databaseName)}/sql`,
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
    const seeds = this.databaseDiscoverySeeds();
    const discovered = await this.listDiscoveredDatabases();
    return uniqueSorted([...seeds, ...discovered]);
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
    const databaseIdentities = new Set<string>();
    const ownerIdentities = new Set<string>();

    for (const databaseName of this.databaseDiscoverySeeds()) {
      const metadata = await this.fetchDatabaseMetadata(databaseName);
      const databaseIdentity = normalizeIdentity(metadata?.database_identity);
      const ownerIdentity = normalizeIdentity(metadata?.owner_identity);

      if (databaseIdentity) {
        databaseIdentities.add(databaseIdentity);
      }
      if (ownerIdentity) {
        ownerIdentities.add(ownerIdentity);
      }
    }

    for (const ownerIdentity of ownerIdentities) {
      const ownedDatabaseIdentities = await this.listOwnedDatabaseIdentities(
        ownerIdentity
      );
      ownedDatabaseIdentities.forEach((identity) =>
        databaseIdentities.add(identity)
      );
    }

    return uniqueSorted(databaseIdentities);
  }

  async listOwnedDatabaseIdentities(ownerIdentity: string): Promise<string[]> {
    try {
      const normalizedOwnerIdentity = normalizeIdentity(ownerIdentity);
      if (!normalizedOwnerIdentity) {
        return [];
      }

      const response = await fetch(
        `${this.identityApiBaseUrl(normalizedOwnerIdentity)}/databases`
      );

      if (!response.ok) {
        return [];
      }

      const body = (await response.json()) as {
        addresses?: unknown;
        identities?: unknown;
      };
      const identities = body.identities ?? body.addresses;

      return normalizeIdentityList(identities);
    } catch {
      return [];
    }
  }

  async resolveDatabaseNames(nameOrIdentity: string): Promise<string[]> {
    const response = await fetch(
      `${this.discoveryDatabaseApiBaseUrl(nameOrIdentity)}/names`,
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

  private discoveryDatabaseApiBaseUrl(nameOrIdentity: string): string {
    const baseUrl = env.STDB_DISCOVERY_SERVER ?? env.STDB_BASE_URL;
    return this.databaseApiBaseUrl(nameOrIdentity, baseUrl);
  }

  private async fetchDatabaseSchema(
    databaseName: string
  ): Promise<StdbDatabaseSchema> {
    const response = await fetch(
      this.databaseSchemaUrl(databaseName),
      {
        headers: {
          Authorization: `Bearer ${resolveDatabaseAuthToken(databaseName)}`,
        },
      }
    );

    if (!response.ok) {
      const body = await response.text();
      throw new Error(
        `Spacetime schema fetch failed (${response.status} ${response.statusText}): ${body}`
      );
    }

    return (await response.json()) as StdbDatabaseSchema;
  }

  private apiRoot(baseUrl: string): string {
    return baseUrl.replace(/\/+$/, "").replace(API_ENDPOINT_PATTERN, "");
  }

  private databaseApiBaseUrl(nameOrIdentity: string, baseUrl: string): string {
    return `${this.apiRoot(baseUrl)}/v1/database/${encodeURIComponent(nameOrIdentity)}`;
  }

  private queryApiBaseUrl(databaseName: string): string {
    return this.databaseApiBaseUrl(databaseName, env.STDB_BASE_URL);
  }

  private identityApiBaseUrl(identity: string): string {
    const baseUrl = env.STDB_DISCOVERY_SERVER ?? env.STDB_BASE_URL;
    return `${this.apiRoot(baseUrl)}/v1/identity/${encodeURIComponent(identity)}`;
  }

  private databaseSchemaUrl(databaseName: string): string {
    return `${this.discoveryDatabaseApiBaseUrl(databaseName)}/schema?version=${RAW_MODULE_DEF_VERSION}`;
  }

  private databaseDiscoverySeeds(): string[] {
    return uniqueSorted([
      env.STDB_SOURCE_DATABASE,
      ...parseCsvList(env.STDB_DATABASES),
      ...Object.keys(env.STDB_DATABASE_AUTH_TOKENS),
    ]);
  }

  private async fetchDatabaseMetadata(
    databaseName: string
  ): Promise<StdbDatabaseMetadata | null> {
    try {
      const response = await fetch(
        this.discoveryDatabaseApiBaseUrl(databaseName),
        {
          headers: {
            Authorization: `Bearer ${resolveDatabaseAuthToken(
              databaseName
            )}`,
          },
        }
      );

      if (!response.ok) {
        return null;
      }

      return (await response.json()) as StdbDatabaseMetadata;
    } catch {
      return null;
    }
  }

  static rowHash(row: Record<string, unknown>): string {
    return createHash("sha256")
      .update(JSON.stringify(row))
      .digest("hex");
  }
}
