import { normalizeResult, normalizeValue } from "../shim/normalize.js";
import { StdbClient } from "../shim/stdb-client.js";
import type { StdbQueryResult, StdbRoutineRow } from "../shim/types.js";
import { PgwireError } from "./error.js";
import type { PgwireField, PgwireQueryResult } from "./types.js";
import { toTypeOid } from "./protocol.js";

const DML_PATTERN = /^\s*(insert|update|delete)\b/i;
const UNSUPPORTED_STATEMENT_PATTERN =
  /^\s*(create|alter|drop|truncate|copy|call|grant|revoke|comment|vacuum|analyze)\b/i;

const TRANSACTION_PATTERN = /^\s*(begin|commit|rollback|start transaction)\b/i;
const SET_PATTERN = /^\s*(set|reset)\b/i;
const SHOW_PATTERN = /^\s*show\b/i;
const PUBLIC_SCHEMA_OID = "2200";
const VOID_TYPE_OID = "2278";
const TEXT_ARRAY_TYPE_OID = 1009;
const CHAR_ARRAY_TYPE_OID = 1002;
const OID_ARRAY_TYPE_OID = 1028;

function textField(name: string): PgwireField {
  return { name, typeOid: toTypeOid("text") };
}

function booleanField(name: string): PgwireField {
  return { name, typeOid: toTypeOid("boolean") };
}

function numericField(name: string): PgwireField {
  return { name, typeOid: toTypeOid("numeric") };
}

function integerField(name: string): PgwireField {
  return { name, typeOid: toTypeOid("integer") };
}

function textArrayField(name: string): PgwireField {
  return { name, typeOid: TEXT_ARRAY_TYPE_OID };
}

function charArrayField(name: string): PgwireField {
  return { name, typeOid: CHAR_ARRAY_TYPE_OID };
}

function oidArrayField(name: string): PgwireField {
  return { name, typeOid: OID_ARRAY_TYPE_OID };
}

function singleValueResult(
  field: PgwireField,
  value: string,
  commandTag = "SELECT 1"
): PgwireQueryResult {
  return {
    fields: [field],
    rows: [[value]],
    commandTag,
  };
}

function emptyResult(commandTag: string): PgwireQueryResult {
  return { fields: [], rows: [], commandTag };
}

function parseDmlVerb(sql: string): "INSERT" | "UPDATE" | "DELETE" | null {
  const match = sql.match(DML_PATTERN);
  if (!match) {
    return null;
  }

  return match[1].toUpperCase() as "INSERT" | "UPDATE" | "DELETE";
}

function remapStdbSqlError(error: unknown): never {
  const message = error instanceof Error ? error.message : String(error);

  if (/is not authorized to run SQL DML statements/i.test(message)) {
    throw new PgwireError("Permission denied for SQL DML on this SpacetimeDB database", {
      code: "42501",
      detail: message,
      hint:
        "Use a bearer token whose identity is authorized for DML on this database, or perform the write through an approved reducer/procedure instead.",
    });
  }

  throw error instanceof Error
    ? error
    : new Error(message);
}

function settingsRows(): Array<Record<string, unknown>> {
  return [
    { name: "bytea_output", setting: "hex", context: "user", vartype: "enum", min_val: null, max_val: null, enumvals: "{hex,escape}" },
    { name: "client_encoding", setting: "UTF8", context: "user", vartype: "string", min_val: null, max_val: null, enumvals: null },
    { name: "DateStyle", setting: "ISO, MDY", context: "user", vartype: "string", min_val: null, max_val: null, enumvals: null },
    { name: "server_version", setting: "17.0-spacetimedb-shim", context: "internal", vartype: "string", min_val: null, max_val: null, enumvals: null },
    { name: "default_table_access_method", setting: "heap", context: "user", vartype: "enum", min_val: null, max_val: null, enumvals: "{heap}" },
    { name: "autovacuum_vacuum_threshold", setting: "50", context: "superuser", vartype: "integer", min_val: "0", max_val: null, enumvals: null },
    { name: "autovacuum_analyze_threshold", setting: "50", context: "superuser", vartype: "integer", min_val: "0", max_val: null, enumvals: null },
    { name: "autovacuum_vacuum_scale_factor", setting: "0.2", context: "superuser", vartype: "real", min_val: "0", max_val: null, enumvals: null },
    { name: "autovacuum_analyze_scale_factor", setting: "0.1", context: "superuser", vartype: "real", min_val: "0", max_val: null, enumvals: null },
    { name: "autovacuum_vacuum_cost_delay", setting: "2", context: "superuser", vartype: "integer", min_val: "-1", max_val: null, enumvals: null },
    { name: "autovacuum_vacuum_cost_limit", setting: "-1", context: "superuser", vartype: "integer", min_val: "-1", max_val: null, enumvals: null },
    { name: "autovacuum_freeze_max_age", setting: "200000000", context: "superuser", vartype: "integer", min_val: "100000", max_val: null, enumvals: null },
    { name: "vacuum_freeze_min_age", setting: "50000000", context: "superuser", vartype: "integer", min_val: "0", max_val: null, enumvals: null },
    { name: "vacuum_freeze_table_age", setting: "150000000", context: "superuser", vartype: "integer", min_val: "0", max_val: null, enumvals: null },
  ];
}

function languageRows(): Array<Record<string, unknown>> {
  return [
    { oid: "1", lanname: "spacetimedb", label: "spacetimedb", value: "spacetimedb" },
  ];
}

function procedureTypeRows(): Array<Record<string, unknown>> {
  return [
    { typname: "boolean", elemoid: "16", typlen: "1", typtype: "b", oid: "16", nspname: "pg_catalog", isdup: "f", is_collatable: "f" },
    { typname: "text", elemoid: "25", typlen: "-1", typtype: "b", oid: "25", nspname: "pg_catalog", isdup: "f", is_collatable: "t" },
    { typname: "double precision", elemoid: "701", typlen: "8", typtype: "b", oid: "701", nspname: "pg_catalog", isdup: "f", is_collatable: "f" },
    { typname: "numeric", elemoid: "1700", typlen: "-1", typtype: "b", oid: "1700", nspname: "pg_catalog", isdup: "f", is_collatable: "f" },
    { typname: "serial", elemoid: "0", typlen: "4", typtype: "b", oid: "0", nspname: "pg_catalog", isdup: "f", is_collatable: "f" },
    { typname: "smallserial", elemoid: "0", typlen: "2", typtype: "b", oid: "0", nspname: "pg_catalog", isdup: "f", is_collatable: "f" },
    { typname: "bigserial", elemoid: "0", typlen: "8", typtype: "b", oid: "0", nspname: "pg_catalog", isdup: "f", is_collatable: "f" },
  ];
}

function filterSettingsRows(
  sql: string,
  rows: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const whereClause = extractTopLevelWhereClause(sql);
  if (!whereClause) {
    return rows;
  }

  const exactMatch = whereClause.match(/\b(?:\w+\.)?name\s*=\s*'([^']+)'/i);
  if (exactMatch) {
    return rows.filter((row) => row.name === exactMatch[1]);
  }

  const inMatch = whereClause.match(/\b(?:\w+\.)?name\s+in\s*\(([^)]+)\)/i);
  if (inMatch) {
    const requestedNames = new Set(
      [...inMatch[1].matchAll(/'([^']+)'/g)].map((match) => match[1])
    );
    return rows.filter((row) => requestedNames.has(String(row.name)));
  }

  return rows;
}

function databaseRows(
  databaseNames: string[],
  currentDatabaseName: string
): Array<Record<string, unknown>> {
  const names = new Set<string>(["postgres", currentDatabaseName, ...databaseNames]);
  const orderedNames = [...names].sort((left, right) => left.localeCompare(right));

  return orderedNames.map((name, index) => ({
    oid: String(name === "postgres" ? 5 : 20_000 + index),
    datname: name,
    datallowconn: "t",
    encoding: "UTF8",
    cancreate: "f",
    datistemplate: "f",
  }));
}

function namespaceRows(): Array<Record<string, unknown>> {
  return [
    {
      oid: PUBLIC_SCHEMA_OID,
      nspname: "public",
      name: "public",
      schema_name: "public",
      is_catalog: "f",
      db_support: "t",
      can_create: "f",
      has_usage: "t",
      description: null,
    },
  ];
}

function accessMethodRows(): Array<Record<string, unknown>> {
  return [{ oid: "2", amname: "heap", amtype: "t" }];
}

function namespaceBrowserRows(
  databaseName: string
): Array<Record<string, unknown>> {
  if (databaseName === "postgres") {
    return [
      {
        oid: "11",
        nspname: "pg_catalog",
        name: "PostgreSQL Catalog (pg_catalog)",
        schema_name: "pg_catalog",
        is_catalog: "t",
        db_support: "t",
        can_create: "f",
        has_usage: "t",
        description: null,
      },
      {
        oid: "12",
        nspname: "information_schema",
        name: "ANSI (information_schema)",
        schema_name: "information_schema",
        is_catalog: "t",
        db_support: "f",
        can_create: "f",
        has_usage: "t",
        description: null,
      },
      ...namespaceRows(),
    ];
  }

  return namespaceRows();
}

function filterNamespaceRows(
  sql: string,
  rows: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const whereClause = extractTopLevelWhereClause(sql);
  if (!whereClause) {
    return rows;
  }

  let filteredRows = rows;
  const oidMatch = whereClause.match(/\b(?:nsp\.)?oid\s*=\s*(\d+)(?:::\w+)?/i);
  if (oidMatch) {
    return filteredRows.filter((row) => String(row.oid) === oidMatch[1]);
  }

  if (/\bnspname\s+not\s+like\s+e?'pg\\\\_%'/i.test(whereClause)) {
    filteredRows = filteredRows.filter(
      (row) => !String(row.nspname).startsWith("pg_")
    );
  }

  if (
    /\bnot\s*\(/i.test(whereClause) &&
    /\bnspname\s*=\s*'pg_catalog'/i.test(whereClause) &&
    /\bnspname\s*=\s*'information_schema'/i.test(whereClause)
  ) {
    const excludedNames = new Set(["pg_catalog", "pgagent", "information_schema"]);
    filteredRows = filteredRows.filter(
      (row) => !excludedNames.has(String(row.nspname))
    );
  }

  const schemaNames = [...whereClause.matchAll(/\b(?:nsp\.)?nspname\s*=\s*'([^']+)'/gi)].map(
    (match) => match[1]
  );
  if (schemaNames.length > 0) {
    const isSystemExclusionOnly =
      /\bnot\s*\(/i.test(whereClause) &&
      schemaNames.every((name) =>
        ["pg_catalog", "pgagent", "information_schema"].includes(name)
      );
    if (isSystemExclusionOnly) {
      return filteredRows;
    }

    return filteredRows.filter((row) => schemaNames.includes(String(row.nspname)));
  }

  return filteredRows;
}

function filterRelationRows(
  sql: string,
  rows: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const whereClause = extractTopLevelWhereClause(sql);
  if (!whereClause) {
    return rows;
  }

  let filteredRows = rows;
  const oidMatch = whereClause.match(/\b(?:rel|c)\.oid\s*=\s*(\d+)(?:::\w+)?/i);
  if (oidMatch) {
    filteredRows = filteredRows.filter((row) => String(row.oid) === oidMatch[1]);
  }

  const relNameMatch = whereClause.match(/\b(?:rel|c)\.relname\s*=\s*'([^']+)'/i);
  if (relNameMatch) {
    filteredRows = filteredRows.filter((row) => String(row.relname) === relNameMatch[1]);
  }

  return filteredRows;
}

function roleRows(): Array<Record<string, unknown>> {
  return [
    {
      rolname: "shim",
      usename: "shim",
      rolcanlogin: "t",
      rolsuper: "f",
      usesuper: "f",
    },
  ];
}

function typeRows(): Array<Record<string, unknown>> {
  return [
    { oid: "16", typname: "boolean" },
    { oid: "23", typname: "integer" },
    { oid: "25", typname: "text" },
    { oid: "114", typname: "json" },
    { oid: "701", typname: "double precision" },
    { oid: "1184", typname: "timestamp with time zone" },
    { oid: "1700", typname: "numeric" },
    { oid: VOID_TYPE_OID, typname: "void" },
  ];
}

function lookupTypeNameByOid(typeOid: string): string {
  const row = typeRows().find((candidate) => String(candidate.oid) === String(typeOid));
  return row ? String(row.typname) : "text";
}

function formatTextArrayLiteral(values: string[]): string {
  const encoded = values.map((value) => {
    const needsQuotes = /[\s,{}"\\]/.test(value);
    const escaped = value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
    return needsQuotes ? `"${escaped}"` : escaped;
  });
  return `{${encoded.join(",")}}`;
}

function extractTypeOidsForEditQueries(sql: string): string[] {
  const collected = new Set<string>();
  const patterns = [
    /\bcastsource\s+in\s*\(([^)]+)\)/gi,
    /\btypbasetype\s+in\s*\(([^)]+)\)/gi,
  ];

  for (const pattern of patterns) {
    for (const match of sql.matchAll(pattern)) {
      for (const oid of match[1].matchAll(/\d+/g)) {
        collected.add(oid[0]);
      }
    }
  }

  for (const pattern of [/\bcastsource\s*=\s*(\d+)/gi, /\btypbasetype\s*=\s*(\d+)/gi]) {
    for (const match of sql.matchAll(pattern)) {
      collected.add(match[1]);
    }
  }

  return [...collected];
}

function attributeTypeMetadata(pgType: string): {
  oid: string;
  attlen: string;
  storage: string;
} {
  const normalizedType = pgType.toLowerCase();

  switch (normalizedType) {
    case "boolean":
      return { oid: String(toTypeOid("boolean")), attlen: "1", storage: "p" };
    case "integer":
      return { oid: String(toTypeOid("integer")), attlen: "4", storage: "p" };
    case "double precision":
      return { oid: String(toTypeOid("double precision")), attlen: "8", storage: "p" };
    case "timestamp with time zone":
      return { oid: String(toTypeOid("timestamp with time zone")), attlen: "8", storage: "p" };
    case "numeric":
      return { oid: String(toTypeOid("numeric")), attlen: "-1", storage: "m" };
    case "json":
      return { oid: String(toTypeOid("json")), attlen: "-1", storage: "x" };
    case "text":
    default:
      return { oid: String(toTypeOid("text")), attlen: "-1", storage: "x" };
  }
}

function formatRoutineArguments(routine: StdbRoutineRow): string {
  return routine.parameters
    .map((parameter, index) => {
      const name = parameter.name?.trim() || `arg${index + 1}`;
      return `${name} ${parameter.pgType}`;
    })
    .join(", ");
}

function routineRows(
  routines: StdbRoutineRow[],
  databaseName: string
): Array<Record<string, unknown>> {
  return routines.map((routine, index) => {
    const oid = String(20_000 + index);
    const argumentTypes = routine.parameters
      .map((parameter) => String(toTypeOid(parameter.pgType)))
      .join(" ");
    const allArgumentTypes = `{${routine.parameters
      .map((parameter) => String(toTypeOid(parameter.pgType)))
      .join(",")}}`;
    const argumentNames = `{${routine.parameters
      .map((parameter) => `"${(parameter.name ?? "").replace(/"/g, '\\"')}"`)
      .join(",")}}`;
    const argumentModes = `{${routine.parameters.map(() => "i").join(",")}}`;
    const argumentsText = formatRoutineArguments(routine);
    const displayName = argumentsText
      ? `${routine.routineName}(${argumentsText})`
      : routine.routineName;

    return {
      oid,
      xmin: "0",
      proname: routine.routineName,
      schema: "public",
      pronamespace: PUBLIC_SCHEMA_OID,
      nspname: "public",
      typnsp: "pg_catalog",
      prokind: "p",
      proiswindow: "f",
      prorettype: VOID_TYPE_OID,
      prorettypename: "void",
      proretset: "f",
      prolang: "1",
      procost: "100",
      prorows: "0",
      pronargs: String(routine.parameters.length),
      proargtypes: argumentTypes,
      proallargtypes: routine.parameters.length > 0 ? allArgumentTypes : null,
      proargmodes: routine.parameters.length > 0 ? argumentModes : null,
      proargnames: argumentNames,
      proargtypenames: routine.parameters.map((parameter) => parameter.pgType).join(", "),
      proargdefaultvals: null,
      prosrc: routine.routineName,
      prosrc_c: routine.routineName,
      prosrc_sql: null,
      probin: null,
      proacl: null,
      prosecdef: "f",
      proleakproof: "f",
      proisstrict: "f",
      provolatile: "v",
      proparallel: "u",
      is_pure_sql: "f",
      pronargdefaults: "0",
      proconfig: null,
      proowner: "10",
      visible: "t",
      executable: "t",
      name: displayName,
      name_with_args: displayName,
      routine_kind: "proc",
      lanname: "spacetimedb",
      funcowner: "shim",
      description: null,
      dependsonextensions: null,
      seclabels: null,
      function_result: "void",
      function_arguments: argumentsText,
      function_identity_arguments: argumentsText,
      specific_catalog: databaseName,
      specific_schema: "public",
      specific_name: routine.routineName,
      routine_catalog: databaseName,
      routine_schema: "public",
      routine_name: routine.routineName,
      routine_type: routine.routineType,
      data_type: null,
      type_udt_catalog: null,
      type_udt_schema: null,
      type_udt_name: null,
    };
  });
}

function filterRoutineRows(
  sql: string,
  rows: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const whereClause = extractTopLevelWhereClause(sql);
  if (!whereClause) {
    return rows;
  }

  let filteredRows = rows;
  const oidMatch = whereClause.match(/\b(?:pr|db)?\.?oid\s*=\s*(\d+)(?:::\w+)?/i);
  if (oidMatch) {
    filteredRows = filteredRows.filter((row) => String(row.oid) === oidMatch[1]);
  }

  const nameMatch = whereClause.match(/\b(?:pr|db)?\.?proname\s*=\s*'([^']+)'/i);
  if (nameMatch) {
    filteredRows = filteredRows.filter((row) => String(row.proname) === nameMatch[1]);
  }

  const namespaceMatch = whereClause.match(/\bpronamespace\s*=\s*(\d+)(?:::\w+)?/i);
  if (namespaceMatch) {
    filteredRows = filteredRows.filter(
      (row) => String(row.pronamespace) === namespaceMatch[1]
    );
  }

  return filteredRows;
}

function splitTopLevelExpressions(source: string): string[] {
  const parts: string[] = [];
  let current = "";
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;

  for (let index = 0; index < source.length; index += 1) {
    const char = source[index];
    const next = source[index + 1];

    if (inSingleQuote) {
      current += char;
      if (char === "'" && next === "'") {
        current += next;
        index += 1;
        continue;
      }
      if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }

    if (inDoubleQuote) {
      current += char;
      if (char === '"' && next === '"') {
        current += next;
        index += 1;
        continue;
      }
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      current += char;
      continue;
    }

    if (char === '"') {
      inDoubleQuote = true;
      current += char;
      continue;
    }

    if (char === "(") {
      depth += 1;
      current += char;
      continue;
    }

    if (char === ")") {
      depth = Math.max(0, depth - 1);
      current += char;
      continue;
    }

    if (char === "," && depth === 0) {
      const trimmed = current.trim();
      if (trimmed) {
        parts.push(trimmed);
      }
      current = "";
      continue;
    }

    current += char;
  }

  const trimmed = current.trim();
  if (trimmed) {
    parts.push(trimmed);
  }

  return parts;
}

function findTopLevelKeyword(source: string, keyword: string): number {
  const needle = keyword.toLowerCase();
  let depth = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;

  for (let index = 0; index <= source.length - needle.length; index += 1) {
    const char = source[index];
    const next = source[index + 1];

    if (inSingleQuote) {
      if (char === "'" && next === "'") {
        index += 1;
        continue;
      }
      if (char === "'") {
        inSingleQuote = false;
      }
      continue;
    }

    if (inDoubleQuote) {
      if (char === '"' && next === '"') {
        index += 1;
        continue;
      }
      if (char === '"') {
        inDoubleQuote = false;
      }
      continue;
    }

    if (char === "'") {
      inSingleQuote = true;
      continue;
    }

    if (char === '"') {
      inDoubleQuote = true;
      continue;
    }

    if (char === "(") {
      depth += 1;
      continue;
    }

    if (char === ")") {
      depth = Math.max(0, depth - 1);
      continue;
    }

    if (depth !== 0) {
      continue;
    }

    const prefix = index === 0 ? " " : source[index - 1];
    const candidate = source.slice(index, index + needle.length).toLowerCase();
    const suffix = source[index + needle.length] ?? " ";
    if (!/\w/.test(prefix) && candidate === needle && !/\w/.test(suffix)) {
      return index;
    }
  }

  return -1;
}

function extractTopLevelWhereClause(sql: string): string | null {
  const lowerSql = sql.toLowerCase();
  const whereIndex = findTopLevelKeyword(lowerSql, "where");
  if (whereIndex < 0) {
    return null;
  }

  const endIndex =
    [findTopLevelKeyword(lowerSql, "order by"), findTopLevelKeyword(lowerSql, "limit"), findTopLevelKeyword(lowerSql, "offset")]
      .filter((value) => value > whereIndex)
      .sort((left, right) => left - right)[0] ?? sql.length;

  return sql.slice(whereIndex + "where".length, endIndex).trim();
}

function extractTopLevelOrderByClause(sql: string): string | null {
  const lowerSql = sql.toLowerCase();
  const orderIndex = findTopLevelKeyword(lowerSql, "order by");
  if (orderIndex < 0) {
    return null;
  }

  const endIndex =
    [findTopLevelKeyword(lowerSql, "limit"), findTopLevelKeyword(lowerSql, "offset")]
      .filter((value) => value > orderIndex)
      .sort((left, right) => left - right)[0] ?? sql.length;

  return sql.slice(orderIndex + "order by".length, endIndex).trim();
}

function extractTopLevelLimitValue(sql: string): number | undefined {
  const lowerSql = sql.toLowerCase();
  const limitIndex = findTopLevelKeyword(lowerSql, "limit");
  if (limitIndex < 0) {
    return undefined;
  }

  const match = sql.slice(limitIndex).match(/^limit\s+(\d+)/i);
  return match ? Number(match[1]) : undefined;
}

function extractTopLevelOffsetValue(sql: string): number {
  const lowerSql = sql.toLowerCase();
  const offsetIndex = findTopLevelKeyword(lowerSql, "offset");
  if (offsetIndex < 0) {
    return 0;
  }

  const match = sql.slice(offsetIndex).match(/^offset\s+(\d+)/i);
  return match ? Number(match[1]) : 0;
}

function extractTopLevelFromTarget(sql: string): string | null {
  const lowerSql = sql.toLowerCase();
  const fromIndex = findTopLevelKeyword(lowerSql, "from");
  if (fromIndex < 0) {
    return null;
  }

  const afterFrom = sql.slice(fromIndex + "from".length).trimStart();
  const match = afterFrom.match(
    /^((?:"[^"]+"|[a-zA-Z_][a-zA-Z0-9_]*)(?:\.(?:"[^"]+"|[a-zA-Z_][a-zA-Z0-9_]*))?(?:\(\))?)/
  );
  return match ? match[1].replace(/"/g, "").toLowerCase() : null;
}

function filterTypeRows(
  sql: string,
  rows: Array<Record<string, unknown>>
): Array<Record<string, unknown>> {
  const whereClause = extractTopLevelWhereClause(sql);
  if (!whereClause) {
    return rows;
  }
  const oidValues = new Set<string>();

  for (const match of whereClause.matchAll(/\boid\s*=\s*(\d+)\b/gi)) {
    oidValues.add(match[1]);
  }

  const anyMatch = whereClause.match(/\boid\s*=\s*any\s*\(([\s\S]+)\)/i);
  if (anyMatch) {
    for (const match of anyMatch[1].matchAll(/\d+/g)) {
      oidValues.add(match[0]);
    }
  }

  if (oidValues.size === 0) {
    return rows;
  }

  return rows.filter((row) => oidValues.has(String(row.oid)));
}

function projectMetadataRows(
  sql: string,
  availableRows: Array<Record<string, unknown>>,
  fallbackFields: PgwireField[]
): PgwireQueryResult {
  const selectMatch = sql.match(/^\s*select\s+([\s\S]+?)\s+from\s+/i);
  if (!selectMatch) {
    return resultFromRows(
      fallbackFields,
      availableRows.map((row) =>
        fallbackFields.map((field) => {
          const value = row[field.name];
          return value === null || value === undefined ? null : String(value);
        })
      )
    );
  }

  const fields = selectMatch[1]
    .trim()
    .replace(/\s+from\s+$/i, "")
    .replace(/^distinct\s+/i, "")
    .trim();

  const projectedFields = splitTopLevelExpressions(fields)
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const aliasMatch = entry.match(/\s+as\s+("?)([a-z_][a-z0-9_]*)\1$/i);
      const alias = aliasMatch?.[2];
      const baseExpression = entry
        .replace(/\s+as\s+("?)[a-z_][a-z0-9_]*\1$/i, "")
        .trim();
      const isNumericCastProjection =
        /::\s*numeric\b/i.test(baseExpression) ||
        /(?:^|\s)cast\([\s\S]+?\s+as\s+numeric\)/i.test(baseExpression);
      const isEncodingNameProjection =
        /^(?:pg_catalog\.)?pg_encoding_to_char\([^)]+\)$/i.test(baseExpression);
      const normalized = baseExpression
        .replace(/\s+as\s+("?)[a-z_][a-z0-9_]*\1$/i, "")
        .replace(/::numeric\b/gi, "")
        .replace(/^pg_encoding_to_char\([^)]+\)$/i, "encoding")
        .replace(/^has_database_privilege\([^)]+\)$/i, "cancreate")
        .replace(/^(?:pg_catalog\.)?pg_get_function_result\([^)]+\)$/i, "function_result")
        .replace(
          /^(?:pg_catalog\.)?pg_get_function_arguments\([^)]+\)$/i,
          "function_arguments"
        )
        .replace(
          /^(?:pg_catalog\.)?pg_get_function_identity_arguments\([^)]+\)$/i,
          "function_identity_arguments"
        )
        .replace(/^(?:pg_catalog\.)?pg_function_is_visible\([^)]+\)$/i, "visible")
        .replace(/^(?:pg_catalog\.)?has_function_privilege\([^)]+\)$/i, "executable")
        .replace(
          /^(?:pg_catalog\.)?format_type\(\s*[^)]*prorettype[^)]*\)$/i,
          "function_result"
        )
        .replace(/^current_database\(\)$/i, "datname")
        .replace(/^(?:pg_catalog\.)?format_type\(\s*oid\s*,\s*null\s*\)$/i, "typname")
        .replace(/\bdb\./gi, "")
        .replace(/"/g, "")
        .trim()
        .toLowerCase();
      const fieldName = normalized.split(".").at(-1) ?? normalized;
      const textArrayFields = new Set([
        "proargnames",
        "proconfig",
        "dependsonextensions",
        "seclabels",
      ]);
      const charArrayFields = new Set(["proargmodes"]);
      const oidArrayFields = new Set(["proallargtypes"]);
      const booleanFields = new Set([
        "datallowconn",
        "cancreate",
        "datistemplate",
        "rolcanlogin",
        "rolsuper",
        "usesuper",
        "proretset",
        "prosecdef",
        "proisstrict",
        "visible",
        "executable",
      ]);
      const integerFields = new Set([
        "oid",
        "encoding",
        "datdba",
        "proowner",
        "pronamespace",
        "prorettype",
        "pronargs",
      ]);
      const fallbackType =
        textArrayFields.has(fieldName)
          ? textArrayField(alias ?? fieldName)
          : charArrayFields.has(fieldName)
          ? charArrayField(alias ?? fieldName)
          : oidArrayFields.has(fieldName)
          ? oidArrayField(alias ?? fieldName)
          : isEncodingNameProjection
          ? textField(alias ?? fieldName)
          : isNumericCastProjection
          ? numericField(alias ?? fieldName)
          : integerFields.has(fieldName)
          ? integerField(alias ?? fieldName)
          : booleanFields.has(fieldName)
            ? booleanField(alias ?? fieldName)
            : textField(alias ?? fieldName);
      const sourceName =
        [alias, fieldName]
          .filter((value): value is string => Boolean(value))
          .find((candidate) => availableRows.some((row) => candidate in row)) ?? fieldName;

      return {
        sourceName,
        field: fallbackType,
      };
    });

  return resultFromRows(
    projectedFields.map((entry) => entry.field),
    availableRows.map((row) =>
      projectedFields.map((entry) => {
        const value = row[entry.sourceName];
        return value === null || value === undefined ? null : String(value);
      })
    )
  );
}

function findAllMatches(pattern: RegExp, source: string): string[] {
  const matches: string[] = [];
  for (const match of source.matchAll(pattern)) {
    if (match[1]) {
      matches.push(match[1].replace(/"/g, ""));
    }
  }
  return matches;
}

function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, " ").trim();
}

function isPgAttributeMetadataQuery(sql: string): boolean {
  return /\bfrom\s+(?:pg_catalog\.)?pg_attribute\b/i.test(sql);
}

function extractAttributeRelationOid(sql: string): string | undefined {
  const castMatch = sql.match(
    /\batt\.attrelid\s*=\s*cast\(\s*(\d+)\s+as\s+oid\s*\)/i
  );
  if (castMatch) {
    return castMatch[1];
  }

  const directMatch = sql.match(/\batt\.attrelid\s*=\s*(\d+)(?:::\s*oid)?/i);
  return directMatch?.[1];
}

function rewriteTableReferences(sql: string): string {
  return sql
    .replace(/::text\b/gi, "")
    .replace(/::varchar(?:\(\d+\))?/gi, "")
    .replace(
      /\b(count|sum|avg|min|max)\s*\(([^)]+)\)\s*(?=(from\b|where\b|group\b|order\b|limit\b|offset\b|,|$))/gi,
      (_, fn: string, inner: string) => {
        const aliasBase = `${fn.toLowerCase()}_${inner
          .replace(/[*"\s]+/g, "_")
          .replace(/[^a-zA-Z0-9_]+/g, "_")
          .replace(/^_+|_+$/g, "")
          .toLowerCase() || "value"}`;
        return `${fn}(${inner}) as ${aliasBase} `;
      }
    )
    .replace(/\bpublic\."([^"]+)"/gi, (_, tableName) => `"${tableName}"`)
    .replace(/\bpublic\.([a-zA-Z_][a-zA-Z0-9_]*)/g, (_, tableName) => `"${tableName}"`);
}

function stripOrderLimitOffset(sql: string): string {
  return sql
    .replace(/\s+order\s+by\s+[\s\S]*?(?=(\s+limit\b|\s+offset\b|$))/i, "")
    .replace(/\s+limit\s+\d+/i, "")
    .replace(/\s+offset\s+\d+/i, "")
    .trim();
}

function stripSelectPrefix(sql: string): string {
  return sql.replace(/^\s*select\s+/i, "");
}

function parseAggregateExpressions(selectClause: string): Array<{
  fn: string;
  source: string;
  alias: string;
}> {
  return splitTopLevelExpressions(selectClause)
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const match = part.match(
        /^(count|sum|avg|min|max)\s*\(([^)]+)\)(?:\s+as\s+([a-z_][a-z0-9_]*|"[^"]+"))?(?:\s+([a-z_][a-z0-9_]*|"[^"]+"))?$/i
      );
      if (!match) {
        throw new Error("Unsupported aggregate query shape");
      }

      const fn = match[1].toLowerCase();
      const source = match[2].trim().replace(/"/g, "");
      const aliasToken = (match[3] ?? match[4] ?? `${fn}_${source}`)
        .replace(/"/g, "")
        .trim();

      return {
        fn,
        source,
        alias: aliasToken || `${fn}_${source}`,
      };
    });
}

function tryParseAggregateExpressions(
  sql: string
): Array<{
  fn: string;
  source: string;
  alias: string;
}> | null {
  const match = sql.match(/^\s*select\s+(.+?)\s+from\s+/i);
  if (!match || /\bgroup\s+by\b/i.test(sql)) {
    return null;
  }

  try {
    return parseAggregateExpressions(match[1].trim());
  } catch {
    return null;
  }
}

function parseSimpleAggregateQuery(sql: string): {
  tableName: string;
  selectClause: string;
  trailingClause: string;
} | null {
  const rewritten = rewriteTableReferences(stripOrderLimitOffset(sql));
  const match = rewritten.match(
    /^\s*select\s+(.+?)\s+from\s+"?([a-zA-Z_][a-zA-Z0-9_]*)"?(\s+where\s+[\s\S]+)?$/i
  );

  if (!match) {
    return null;
  }

  if (!/^(count|sum|avg|min|max)\s*\(/i.test(match[1].trim())) {
    return null;
  }

  if (/\bgroup\s+by\b/i.test(sql)) {
    return null;
  }

  return {
    tableName: match[2],
    selectClause: match[1].trim(),
    trailingClause: match[3]?.trim() ?? "",
  };
}

function resultFromRows(
  fields: PgwireField[],
  rows: Array<Array<string | null>>
): PgwireQueryResult {
  return {
    fields,
    rows,
    commandTag: `SELECT ${rows.length}`,
  };
}

function resultFromMetadataRows(
  fields: PgwireField[],
  rows: Array<Record<string, unknown>>
): PgwireQueryResult {
  return resultFromRows(
    fields,
    rows.map((row) =>
      fields.map((field) => {
        const value = row[field.name];
        return value === null || value === undefined ? null : String(value);
      })
    )
  );
}

function aggregateOverPgwireResult(
  sql: string,
  result: PgwireQueryResult
): PgwireQueryResult | null {
  const expressions = tryParseAggregateExpressions(sql);
  if (!expressions) {
    return null;
  }

  const rowObjects = result.rows.map((row) =>
    Object.fromEntries(result.fields.map((field, index) => [field.name, row[index]]))
  );

  return {
    fields: expressions.map((expression) => ({
      name: expression.alias,
      typeOid: toTypeOid("numeric"),
    })),
    rows: [
      expressions.map((expression) =>
        computeAggregateValue(expression.fn, expression.source, rowObjects)
      ),
    ],
    commandTag: "SELECT 1",
  };
}

function stdbResultToPgwire(result: StdbQueryResult): PgwireQueryResult {
  const normalized = normalizeResult("result", result);
  const fields = normalized.columns.map((column) => ({
    name: column.pgName,
    typeOid: toTypeOid(column.pgType),
  }));
  const rows = normalized.rows.map((row) =>
    normalized.columns.map((column) => {
      const value = row[column.pgName];
      if (value === null || value === undefined) {
        return null;
      }
      return String(normalizeValue(value));
    })
  );

  return {
    fields,
    rows,
    commandTag: `SELECT ${rows.length}`,
  };
}

function stdbDmlResultToPgwire(
  sql: string,
  result: StdbQueryResult
): PgwireQueryResult | null {
  const verb = parseDmlVerb(sql);
  if (!verb) {
    return null;
  }

  if (/returning\b/i.test(sql)) {
    throw new PgwireError("RETURNING is not supported by this pgwire shim yet", {
      code: "0A000",
      detail:
        "The shim can pass through INSERT, UPDATE, and DELETE, but it does not currently synthesize PostgreSQL RETURNING result sets.",
      hint:
        "Run the write without RETURNING, then issue a separate SELECT if you need the changed rows.",
    });
  }

  const rowCount =
    verb === "INSERT"
      ? result.stats.rows_inserted
      : verb === "UPDATE"
        ? result.stats.rows_updated
        : result.stats.rows_deleted;

  return emptyResult(verb === "INSERT" ? `INSERT 0 ${rowCount}` : `${verb} ${rowCount}`);
}

function stdbResultToNormalizedRows(result: StdbQueryResult): Array<Record<string, unknown>> {
  return normalizeResult("result", result).rows;
}

async function buildTableRows(
  stdbClient: StdbClient,
  databaseName: string
): Promise<PgwireQueryResult> {
  const tables = await stdbClient.listPublicTables(databaseName);
  const rows = tables.map((table) => [
    "public",
    table.tableName,
    "BASE TABLE",
  ]);

  return resultFromRows(
    [textField("table_schema"), textField("table_name"), textField("table_type")],
    rows
  );
}

async function buildColumnRows(
  stdbClient: StdbClient,
  databaseName: string
): Promise<PgwireQueryResult> {
  const tables = await stdbClient.listPublicTables(databaseName);
  const rows: Array<Array<string | null>> = [];

  for (const table of tables) {
    const described = normalizeResult(
      table.tableName,
      await stdbClient.describeTable(table.tableName, databaseName)
    );

    described.columns.forEach((column, index) => {
      rows.push([
        "public",
        table.tableName,
        column.pgName,
        String(index + 1),
        column.pgType,
        column.pgType === "boolean" ? "YES" : "YES",
      ]);
    });
  }

  return resultFromRows(
    [
      textField("table_schema"),
      textField("table_name"),
      textField("column_name"),
      numericField("ordinal_position"),
      textField("data_type"),
      textField("is_nullable"),
    ],
    rows
  );
}

async function buildAttributeRows(
  stdbClient: StdbClient,
  databaseName: string,
  relationOid: string
): Promise<Array<Record<string, unknown>>> {
  const tables = await stdbClient.listPublicTables(databaseName);
  const targetTable = tables.find((table) => String(table.tableId) === relationOid);
  if (!targetTable) {
    return [];
  }

  const described = normalizeResult(
    targetTable.tableName,
    await stdbClient.describeTable(targetTable.tableName, databaseName)
  );

  const primaryKeyIndex = described.columns.findIndex((column) => column.pgName === "id");
  const indkey = primaryKeyIndex >= 0 ? String(primaryKeyIndex + 1) : null;

  return described.columns.map((column, index) => ({
    ...attributeTypeMetadata(column.pgType),
    name: column.pgName,
    oid: String(index + 1),
    OID: String(index + 1),
    atttypid: attributeTypeMetadata(column.pgType).oid,
    attlen: attributeTypeMetadata(column.pgType).attlen,
    attnum: String(index + 1),
    attndims: "0",
    atttypmod: "-1",
    attacl: null,
    attnotnull: column.pgName === "id" ? "t" : "f",
    attoptions: null,
    attfdwoptions: null,
    attstattarget: null,
    attstorage: attributeTypeMetadata(column.pgType).storage,
    attidentity: "",
    defval: null,
    datatype: column.pgType,
    typname: column.pgType,
    displaytypname: column.pgType,
    cltype: column.pgType,
    inheritedfrom: null,
    inheritedid: null,
    elemoid: attributeTypeMetadata(column.pgType).oid,
    typnspname: "pg_catalog",
    defaultstorage: attributeTypeMetadata(column.pgType).storage,
    not_null: "f",
    has_default_val: "f",
    description: null,
    indkey,
    isdup: "f",
    collspcname: "",
    is_fk: "f",
    seclabels: null,
    is_sys_column: "f",
    colconstype: "n",
    genexpr: null,
    relname: targetTable.tableName,
    is_view_only: "f",
    attcompression: null,
    seqtypid: null,
  }));
}

async function buildRelationRows(
  stdbClient: StdbClient,
  databaseName: string
): Promise<Array<Record<string, unknown>>> {
  if (databaseName === "postgres") {
    return [];
  }

  const tables = await stdbClient.listPublicTables(databaseName);
  return [...tables]
    .sort((left, right) => left.tableName.localeCompare(right.tableName))
    .map((table) => ({
      oid: String(table.tableId),
      relname: table.tableName,
      name: table.tableName,
      schema: "public",
      relkind: "r",
      relation_type: "table",
      owner: "shim",
      relnamespace: PUBLIC_SCHEMA_OID,
      triggercount: "0",
      has_enable_triggers: "0",
      is_partitioned: "f",
      is_inherits: "0",
      is_inherited: "0",
      description: null,
    }));
}

async function countTableRows(
  stdbClient: StdbClient,
  databaseName: string,
  tableName: string
): Promise<string> {
  const result = await stdbClient.query(
    `SELECT count(*) as reltuples FROM "${tableName}"`,
    databaseName
  );
  if (result.length !== 1) {
    return "0";
  }

  const normalizedRows = stdbResultToNormalizedRows(result[0]);
  const value = normalizedRows[0]?.reltuples;
  return value === null || value === undefined ? "0" : String(value);
}

async function buildTablePropertyRows(
  stdbClient: StdbClient,
  databaseName: string,
  relationOid: string
): Promise<Array<Record<string, unknown>>> {
  const tables = await stdbClient.listPublicTables(databaseName);
  const targetTable = tables.find((table) => String(table.tableId) === relationOid);
  if (!targetTable) {
    return [];
  }

  const reltuples = await countTableRows(stdbClient, databaseName, targetTable.tableName);

  return [
    {
      oid: String(targetTable.tableId),
      name: targetTable.tableName,
      spcoid: "1663",
      relacl_str: null,
      spcname: "pg_default",
      replica_identity: "default",
      schema: "public",
      relowner: "shim",
      relkind: "r",
      is_partitioned: "f",
      relhassubclass: "f",
      reltuples,
      description: null,
      conname: null,
      conkey: null,
      isrepl: "f",
      triggercount: "0",
      coll_inherits: null,
      inherited_tables_cnt: "0",
      relpersistence: "f",
      default_amname: "heap",
      fillfactor: null,
      parallel_workers: null,
      toast_tuple_target: null,
      autovacuum_enabled: null,
      autovacuum_vacuum_threshold: null,
      autovacuum_vacuum_scale_factor: null,
      autovacuum_analyze_threshold: null,
      autovacuum_analyze_scale_factor: null,
      autovacuum_vacuum_cost_delay: null,
      autovacuum_vacuum_cost_limit: null,
      autovacuum_freeze_min_age: null,
      autovacuum_freeze_max_age: null,
      autovacuum_freeze_table_age: null,
      toast_autovacuum_enabled: null,
      toast_autovacuum_vacuum_threshold: null,
      toast_autovacuum_vacuum_scale_factor: null,
      toast_autovacuum_analyze_threshold: null,
      toast_autovacuum_analyze_scale_factor: null,
      toast_autovacuum_vacuum_cost_delay: null,
      toast_autovacuum_vacuum_cost_limit: null,
      toast_autovacuum_freeze_min_age: null,
      toast_autovacuum_freeze_max_age: null,
      toast_autovacuum_freeze_table_age: null,
      reloptions: null,
      toast_reloptions: null,
      reloftype: null,
      amname: "heap",
      typname: null,
      typoid: null,
      rlspolicy: "f",
      forcerlspolicy: "f",
      hastoasttable: "f",
      seclabels: null,
      is_sys_table: "f",
      partition_scheme: "",
    },
  ];
}

async function buildRoutineRows(
  sql: string,
  stdbClient: StdbClient,
  databaseName: string
): Promise<PgwireQueryResult> {
  const rows = routineRows(await stdbClient.listRoutines(databaseName), databaseName);

  return projectMetadataRows(
    sql,
    rows,
    [
      textField("specific_catalog"),
      textField("specific_schema"),
      textField("specific_name"),
      textField("routine_catalog"),
      textField("routine_schema"),
      textField("routine_name"),
      textField("routine_type"),
      textField("data_type"),
      textField("type_udt_catalog"),
      textField("type_udt_schema"),
      textField("type_udt_name"),
    ]
  );
}

function applyNaiveFilters(
  sql: string,
  result: PgwireQueryResult,
  currentDatabaseName?: string
): PgwireQueryResult {
  const normalizedSql = normalizeSql(sql).toLowerCase();
  const filterSql = extractTopLevelWhereClause(sql) ?? "";
  const applyExactFilter = (fieldName: string, pattern: RegExp): void => {
    const match = filterSql.match(pattern);
    if (!match) {
      return;
    }

    const fieldIndex = result.fields.findIndex((field) => field.name === fieldName);
    if (fieldIndex < 0) {
      return;
    }

    result.rows = result.rows.filter((row) => row[fieldIndex] === match[1]);
  };

  if (
    normalizedSql.includes("from pg_database") ||
    normalizedSql.includes("from pg_catalog.pg_database")
  ) {
    const datnameIndex = result.fields.findIndex((field) => field.name === "datname");
    const oidIndex = result.fields.findIndex(
      (field) => field.name === "oid" || field.name === "did"
    );

    if (
      /\bdatname\s*=\s*current_database\(\)/i.test(sql) &&
      datnameIndex >= 0 &&
      currentDatabaseName
    ) {
      result.rows = result.rows.filter((row) => row[datnameIndex] === currentDatabaseName);
    }

    const browserDatabasesMatch = sql.match(
      /\boid\s*>\s*(\d+)(?:::\w+)?[\s\S]*\bdatname\s+in\s*\(([^)]+)\)/i
    );
    if (browserDatabasesMatch && datnameIndex >= 0 && oidIndex >= 0) {
      const threshold = Number(browserDatabasesMatch[1]);
      const namedDatabases = [...browserDatabasesMatch[2].matchAll(/'([^']+)'/g)].map(
        (match) => match[1]
      );
      result.rows = result.rows.filter((row) => {
        const oidValue = Number(row[oidIndex] ?? 0);
        const datname = String(row[datnameIndex] ?? "");
        return oidValue > threshold || namedDatabases.includes(datname);
      });
    }

    const likeMatch = sql.match(/datname\s+like\s+'([^']+)'/i);
    if (likeMatch) {
      const regex = new RegExp(
        `^${likeMatch[1].replace(/[%_]/g, (token) => (token === "%" ? ".*" : "."))}$`
      );
      if (datnameIndex >= 0) {
        result.rows = result.rows.filter((row) => regex.test(row[datnameIndex] ?? ""));
      }
    }
  }

  applyExactFilter("table_schema", /table_schema\s*=\s*'([^']+)'/i);
  applyExactFilter("table_name", /table_name\s*=\s*'([^']+)'/i);
  applyExactFilter("routine_schema", /routine_schema\s*=\s*'([^']+)'/i);
  applyExactFilter("routine_name", /routine_name\s*=\s*'([^']+)'/i);
  applyExactFilter("specific_schema", /specific_schema\s*=\s*'([^']+)'/i);
  applyExactFilter("specific_name", /specific_name\s*=\s*'([^']+)'/i);
  applyExactFilter("proname", /proname\s*=\s*'([^']+)'/i);
  applyExactFilter("nspname", /nspname\s*=\s*'([^']+)'/i);
  applyExactFilter("prokind", /prokind\s*=\s*'([^']+)'/i);

  const orderSpecs = splitTopLevelExpressions(extractTopLevelOrderByClause(sql) ?? "")
    .map((part) => part.trim())
    .map((part) => part.match(/^([a-z_\."]+|\d+)(?:\s+(asc|desc))?$/i))
    .filter((match): match is RegExpMatchArray => Boolean(match))
    .map((match) => {
      const ordinal = Number(match[1]);
      const rawName = match[1].split(".").at(-1)?.replace(/"/g, "") ?? "";
      const fieldIndex =
        Number.isInteger(ordinal) && ordinal > 0
          ? ordinal - 1
          : result.fields.findIndex((field) => field.name === rawName);
      return {
        fieldIndex,
        direction: (match[2] ?? "asc").toLowerCase(),
      };
    })
    .filter((entry) => entry.fieldIndex >= 0);
  if (orderSpecs.length > 0) {
    result.rows.sort((left, right) => {
      for (const spec of orderSpecs) {
        const typeOid = result.fields[spec.fieldIndex]?.typeOid;
        const compareValue = comparePgwireValues(
          left[spec.fieldIndex],
          right[spec.fieldIndex],
          typeOid
        );
        if (compareValue !== 0) {
          return spec.direction === "desc" ? compareValue * -1 : compareValue;
        }
      }
      return 0;
    });
  }

  const offset = extractTopLevelOffsetValue(sql);
  const limit = extractTopLevelLimitValue(sql);
  result.rows = result.rows.slice(offset, limit ? offset + limit : undefined);
  result.commandTag = `SELECT ${result.rows.length}`;
  return result;
}

function comparePgwireValues(
  left: string | null,
  right: string | null,
  typeOid: number | undefined
): number {
  if (left === right) {
    return 0;
  }

  if (left === null) {
    return -1;
  }

  if (right === null) {
    return 1;
  }

  const leftNumber = Number(left);
  const rightNumber = Number(right);
  const bothNumeric = Number.isFinite(leftNumber) && Number.isFinite(rightNumber);

  if (
    bothNumeric ||
    typeOid === toTypeOid("numeric") ||
    typeOid === toTypeOid("double precision")
  ) {
    return leftNumber - rightNumber;
  }

  return left.localeCompare(right);
}

function compareUnknownValues(left: unknown, right: unknown): number {
  if (left === right) {
    return 0;
  }

  if (left === null || left === undefined) {
    return -1;
  }

  if (right === null || right === undefined) {
    return 1;
  }

  const leftNumber = Number(left);
  const rightNumber = Number(right);
  const bothNumeric = Number.isFinite(leftNumber) && Number.isFinite(rightNumber);
  if (bothNumeric) {
    return leftNumber - rightNumber;
  }

  return String(left).localeCompare(String(right));
}

function computeAggregateValue(
  fn: string,
  source: string,
  rows: Array<Record<string, unknown>>
): string {
  const values =
    source === "*"
      ? rows.map(() => 1)
      : rows
          .map((row) => row[source.replace(/"/g, "")])
          .filter((value) => value !== null && value !== undefined);

  switch (fn) {
    case "count":
      return String(source === "*" ? rows.length : values.length);
    case "sum":
      return String(
        values.reduce<number>((sum, value) => sum + Number(value), 0)
      );
    case "avg":
      return values.length === 0
        ? "0"
        : String(
            values.reduce<number>((sum, value) => sum + Number(value), 0) /
              values.length
          );
    case "min": {
      if (values.length === 0) {
        return "";
      }
      return String(values.reduce((best, value) => (compareUnknownValues(value, best) < 0 ? value : best)));
    }
    case "max": {
      if (values.length === 0) {
        return "";
      }
      return String(values.reduce((best, value) => (compareUnknownValues(value, best) > 0 ? value : best)));
    }
    default:
      throw new Error(`Unsupported aggregate function: ${fn}`);
  }
}

function withAggregateFallback(
  sql: string,
  result: PgwireQueryResult,
  currentDatabaseName?: string
): PgwireQueryResult {
  return (
    aggregateOverPgwireResult(sql, result) ??
    applyNaiveFilters(sql, result, currentDatabaseName)
  );
}

async function buildAggregateResult(
  stdbClient: StdbClient,
  sql: string,
  databaseName: string
): Promise<PgwireQueryResult | null> {
  const parsed = parseSimpleAggregateQuery(sql);
  if (!parsed) {
    return null;
  }

  const expressions = parseAggregateExpressions(parsed.selectClause);
  const neededColumns = [...new Set(
    expressions
      .map((expression) => expression.source)
      .filter((source) => source !== "*")
  )];
  const selectList = neededColumns.length > 0 ? neededColumns.join(", ") : "*";
  const sourceSql = `SELECT ${selectList} FROM "${parsed.tableName}"${
    parsed.trailingClause ? ` ${parsed.trailingClause}` : ""
  }`;

  const result = await stdbClient.query(sourceSql, databaseName);
  if (result.length !== 1) {
    throw new Error("Expected a single result set");
  }

  const normalizedRows = stdbResultToNormalizedRows(result[0]);
  return {
    fields: expressions.map((expression) => ({
      name: expression.alias,
      typeOid: toTypeOid("numeric"),
    })),
    rows: [
      expressions.map((expression) =>
        computeAggregateValue(expression.fn, expression.source, normalizedRows)
      ),
    ],
    commandTag: "SELECT 1",
  };
}

export class PgwireQueryRouter {
  constructor(private readonly stdbClient = new StdbClient()) {}

  async listDatabases(): Promise<string[]> {
    const databases = await this.stdbClient.listDatabases();
    const reachable: string[] = [];

    for (const databaseName of databases) {
      if (await this.stdbClient.databaseExists(databaseName)) {
        reachable.push(databaseName);
      }
    }

    return reachable;
  }

  async describeQuery(sql: string, databaseName: string): Promise<PgwireField[]> {
    const result = await this.execute(sql, databaseName, true);
    return result.fields;
  }

  async execute(
    sql: string,
    databaseName: string,
    describeOnly = false
  ): Promise<PgwireQueryResult> {
    const trimmedSql = sql.trim().replace(/;+\s*$/, "");
    if (!trimmedSql) {
      return emptyResult("EMPTY");
    }

    if (UNSUPPORTED_STATEMENT_PATTERN.test(trimmedSql)) {
      throw new PgwireError("This pgwire shim does not support that statement type", {
        code: "0A000",
        detail:
          "DDL and administrative statements are not translated by the shim. Only SELECT, INSERT, UPDATE, and DELETE are supported.",
        hint:
          "Use ad hoc SQL reads/writes against tables, or extend the shim for DDL and administrative commands.",
      });
    }

    if (TRANSACTION_PATTERN.test(trimmedSql)) {
      return emptyResult(trimmedSql.split(/\s+/)[0].toUpperCase());
    }

    if (SET_PATTERN.test(trimmedSql)) {
      return emptyResult("SET");
    }

    if (SHOW_PATTERN.test(trimmedSql)) {
      const showTarget = trimmedSql.replace(/^show\s+/i, "").trim().toLowerCase();
      if (showTarget === "transaction_read_only") {
        return singleValueResult(textField("transaction_read_only"), "on");
      }
      if (showTarget === "server_version") {
        return singleValueResult(textField("server_version"), "17.0-spacetimedb-shim");
      }
      return singleValueResult(textField(showTarget), "");
    }

    if (/select\s+current_database\(\)/i.test(trimmedSql)) {
      return singleValueResult(textField("current_database"), databaseName);
    }

    if (/select\s+current_schema\(\)/i.test(trimmedSql)) {
      return singleValueResult(textField("current_schema"), "public");
    }

    if (/select\s+current_user/i.test(trimmedSql)) {
      return singleValueResult(textField("current_user"), "shim");
    }

    if (/select\s+version\(\)/i.test(trimmedSql)) {
      return singleValueResult(
        textField("version"),
        "PostgreSQL 17.0 (SpacetimeDB pgwire shim)"
      );
    }

    if (
      /from\s+pg_catalog\.pg_extension/i.test(trimmedSql) &&
      /from\s+pg_replication_slots/i.test(trimmedSql) &&
      /\bend\s+as\s+type\b/i.test(trimmedSql)
    ) {
      return resultFromRows([textField("type")], [[null]]);
    }

    if (
      /from\s+pg_catalog\.pg_user/i.test(trimmedSql) &&
      /pg_is_in_recovery\(\)/i.test(trimmedSql)
    ) {
      return resultFromRows(
        [booleanField("inrecovery"), booleanField("isreplaypaused")],
        [["f", "f"]]
      );
    }

    if (
      /from\s+pg_catalog\.pg_type/i.test(trimmedSql) &&
      /format_type\s*\(\s*oid\s*,\s*null\s*\)/i.test(trimmedSql)
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, filterTypeRows(trimmedSql, typeRows()), [
          numericField("oid"),
          textField("typname"),
        ])
      );
    }

    if (
      /\barray_agg\s*\(\s*t\.typname\s*\)\s+as\s+edit_types\b/i.test(trimmedSql) &&
      /\bfrom\s*\(\s*select\s+pc\.castsource\s+as\s+main_oid\b/i.test(trimmedSql) &&
      /\bgroup\s+by\s+t\.main_oid\b/i.test(trimmedSql)
    ) {
      const typeOids = extractTypeOidsForEditQueries(trimmedSql);
      return withAggregateFallback(
        trimmedSql,
        resultFromRows(
          [integerField("main_oid"), textArrayField("edit_types")],
          typeOids.map((typeOid) => [typeOid, formatTextArrayLiteral([lookupTypeNameByOid(typeOid)])])
        )
      );
    }

    if (
      /\bselect\s+\*\s+from\s*\(\s*select\s+pg_catalog\.format_type\s*\(\s*t\.oid\s*,\s*null\s*\)\s+as\s+typname\b/i.test(trimmedSql) &&
      /\bas\s+dummy\b/i.test(trimmedSql)
    ) {
      const fields = [
        textField("typname"),
        integerField("elemoid"),
        integerField("typlen"),
        textField("typtype"),
        integerField("oid"),
        textField("nspname"),
        booleanField("isdup"),
        booleanField("is_collatable"),
      ];
      return withAggregateFallback(
        trimmedSql,
        resultFromMetadataRows(fields, procedureTypeRows())
      );
    }

    if (
      /\bselect\s+tt\.oid\s*,\s*pg_catalog\.format_type\s*\(\s*tt\.oid\s*,\s*null\s*\)\s+as\s+typname\b/i.test(trimmedSql) &&
      /\bfrom\s+pg_catalog\.pg_type\s+tt\b/i.test(trimmedSql) &&
      /\bjoin\s+pg_catalog\.pg_cast\s+pc\b/i.test(trimmedSql)
    ) {
      const typeOids = extractTypeOidsForEditQueries(trimmedSql);
      return withAggregateFallback(
        trimmedSql,
        resultFromRows(
          [integerField("oid"), textField("typname")],
          typeOids.map((typeOid) => [typeOid, lookupTypeNameByOid(typeOid)])
        )
      );
    }

    if (
      /\bselect\s+coalesce\s*\(\s*gt\.rolname\s*,\s*'public'\s*\)\s+as\s+grantee\b/i.test(trimmedSql) &&
      /\baclexplode\s*\(\s*db\.proacl\s*\)/i.test(trimmedSql)
    ) {
      return withAggregateFallback(
        trimmedSql,
        resultFromRows(
          [
            textField("grantee"),
            textField("grantor"),
            textField("privileges"),
            textField("grantable"),
          ],
          []
        )
      );
    }

    if (/^\s*select\s+pg_backend_pid\(\)\s*$/i.test(trimmedSql)) {
      return singleValueResult(numericField("pg_backend_pid"), "1");
    }

    if (/^\s*select\s+pg_is_in_recovery\(\)\s*$/i.test(trimmedSql)) {
      return singleValueResult(booleanField("pg_is_in_recovery"), "f");
    }

    if (/^\s*select\s+pg_is_wal_replay_paused\(\)\s*$/i.test(trimmedSql)) {
      return singleValueResult(booleanField("pg_is_wal_replay_paused"), "f");
    }

    if (/select\s+set_config\(/i.test(trimmedSql)) {
      const settingMatch = trimmedSql.match(/set_config\(\s*'([^']+)'\s*,\s*'([^']*)'/i);
      const value = settingMatch?.[2] ?? "";
      return singleValueResult(textField("set_config"), value);
    }

    const normalizedSql = normalizeSql(trimmedSql).toLowerCase();
    const topLevelFromTarget = extractTopLevelFromTarget(trimmedSql);

    if (
      topLevelFromTarget === "pg_show_all_settings()" ||
      topLevelFromTarget === "pg_catalog.pg_show_all_settings()"
    ) {
      const rows = filterSettingsRows(trimmedSql, settingsRows()).filter((row) => {
        const whereClause = extractTopLevelWhereClause(trimmedSql);
        if (!whereClause) {
          return true;
        }

        const contexts = [...whereClause.matchAll(/\bcontext\s+in\s*\(([^)]+)\)/gi)]
          .flatMap((match) => [...match[1].matchAll(/'([^']+)'/g)].map((inner) => inner[1]));
        if (contexts.length === 0) {
          return true;
        }

        return contexts.includes(String(row.context));
      });
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, rows, [
          textField("name"),
          textField("setting"),
          textField("vartype"),
          textField("min_val"),
          textField("max_val"),
          textField("enumvals"),
        ])
      );
    }

    if (
      /\bselect\s+'relacl'\s+as\s+deftype\b/i.test(trimmedSql) &&
      /\baclexplode\s*\(\s*rel\.relacl\s*\)/i.test(trimmedSql)
    ) {
      return withAggregateFallback(
        trimmedSql,
        resultFromRows(
          [
            textField("deftype"),
            textField("grantee"),
            textField("grantor"),
            textField("privileges"),
            textField("grantable"),
          ],
          []
        )
      );
    }

    if (
      topLevelFromTarget === "pg_database" ||
      topLevelFromTarget === "pg_catalog.pg_database"
    ) {
      const rows = databaseRows(await this.listDatabases(), databaseName);
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, rows, [
          integerField("oid"),
          textField("datname"),
          booleanField("datallowconn"),
          textField("serverencoding"),
          booleanField("cancreate"),
          booleanField("datistemplate"),
        ]),
        databaseName
      );
    }

    if (topLevelFromTarget === "information_schema.tables") {
      return withAggregateFallback(
        trimmedSql,
        await buildTableRows(this.stdbClient, databaseName)
      );
    }

    if (topLevelFromTarget === "information_schema.columns") {
      return withAggregateFallback(
        trimmedSql,
        await buildColumnRows(this.stdbClient, databaseName)
      );
    }

    if (topLevelFromTarget === "information_schema.routines") {
      return withAggregateFallback(
        trimmedSql,
        await buildRoutineRows(trimmedSql, this.stdbClient, databaseName)
      );
    }

    if (topLevelFromTarget === "information_schema.schemata") {
      return withAggregateFallback(
        trimmedSql,
        resultFromRows([textField("schema_name")], [["public"]])
      );
    }

    if (
      topLevelFromTarget === "pg_namespace" ||
      topLevelFromTarget === "pg_catalog.pg_namespace"
    ) {
      const browserRows = filterNamespaceRows(trimmedSql, namespaceBrowserRows(databaseName));

      if (
        /\bschema_name\b/i.test(trimmedSql) &&
        /\bis_catalog\b/i.test(trimmedSql) &&
        /\bdb_support\b/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              textField("schema_name"),
              booleanField("is_catalog"),
              booleanField("db_support"),
            ],
            browserRows.map((row) => [
              String(row.schema_name),
              String(row.is_catalog),
              String(row.db_support),
            ])
          )
        );
      }

      if (
        /\bhas_schema_privilege\s*\(/i.test(trimmedSql) &&
        /\bcan_create\b/i.test(trimmedSql) &&
        /\bhas_usage\b/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              integerField("oid"),
              textField("name"),
              booleanField("can_create"),
              booleanField("has_usage"),
              textField("description"),
            ],
            browserRows.map((row) => [
              String(row.oid),
              String(row.name),
              String(row.can_create),
              String(row.has_usage),
              row.description === null ? null : String(row.description),
            ])
          )
        );
      }

      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, browserRows, [
          integerField("oid"),
          textField("nspname"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_proc" ||
      topLevelFromTarget === "pg_catalog.pg_proc"
    ) {
      const rows = filterRoutineRows(
        trimmedSql,
        databaseName === "postgres"
          ? []
          : routineRows(await this.stdbClient.listRoutines(databaseName), databaseName)
      );

      if (
        /\bpr\.xmin\b/i.test(trimmedSql) &&
        /\bprosrc_sql\b/i.test(trimmedSql) &&
        /\bname_with_args\b/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              integerField("oid"),
              integerField("xmin"),
              booleanField("proiswindow"),
              textField("prosrc"),
              textField("prosrc_c"),
              integerField("pronamespace"),
              integerField("prolang"),
              numericField("procost"),
              numericField("prorows"),
              textField("prokind"),
              booleanField("prosecdef"),
              booleanField("proleakproof"),
              booleanField("proisstrict"),
              booleanField("proretset"),
              textField("provolatile"),
              textField("proparallel"),
              integerField("pronargs"),
              integerField("prorettype"),
              oidArrayField("proallargtypes"),
              charArrayField("proargmodes"),
              textField("probin"),
              textField("proacl"),
              textField("proname"),
              textField("name"),
              textField("prorettypename"),
              textField("typnsp"),
              textField("lanname"),
              textArrayField("proargnames"),
              textField("proargtypenames"),
              textField("proargdefaultvals"),
              textField("prosrc_sql"),
              booleanField("is_pure_sql"),
              integerField("pronargdefaults"),
              textArrayField("proconfig"),
              textField("funcowner"),
              textField("description"),
              textArrayField("dependsonextensions"),
              textField("name_with_args"),
              textArrayField("seclabels"),
            ],
            rows.map((row) => [
              String(row.oid),
              String(row.xmin),
              String(row.proiswindow),
              String(row.prosrc),
              String(row.prosrc_c),
              String(row.pronamespace),
              String(row.prolang),
              String(row.procost),
              String(row.prorows),
              String(row.prokind),
              String(row.prosecdef),
              String(row.proleakproof),
              String(row.proisstrict),
              String(row.proretset),
              String(row.provolatile),
              String(row.proparallel),
              String(row.pronargs),
              String(row.prorettype),
              row.proallargtypes === null ? null : String(row.proallargtypes),
              row.proargmodes === null ? null : String(row.proargmodes),
              row.probin === null ? null : String(row.probin),
              row.proacl === null ? null : String(row.proacl),
              String(row.proname),
              String(row.proname),
              String(row.prorettypename),
              String(row.typnsp),
              String(row.lanname),
              row.proargnames === null ? null : String(row.proargnames),
              row.proargtypenames === null ? null : String(row.proargtypenames),
              row.proargdefaultvals === null ? null : String(row.proargdefaultvals),
              row.prosrc_sql === null ? null : String(row.prosrc_sql),
              String(row.is_pure_sql),
              String(row.pronargdefaults),
              row.proconfig === null ? null : String(row.proconfig),
              String(row.funcowner),
              row.description === null ? null : String(row.description),
              row.dependsonextensions === null ? null : String(row.dependsonextensions),
              String(row.name_with_args),
              row.seclabels === null ? null : String(row.seclabels),
            ])
          )
        );
      }

      if (
        /\bpg_get_function_result\s*\(/i.test(trimmedSql) &&
        /\bpg_get_function_arguments\s*\(/i.test(trimmedSql) &&
        /\bcase\s+p\.prokind\b/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              textField("Schema"),
              textField("Name"),
              textField("Result data type"),
              textField("Argument data types"),
              textField("Type"),
            ],
            rows.map((row) => [
              String(row.schema),
              String(row.proname),
              String(row.function_result),
              String(row.function_arguments),
              String(row.routine_kind),
            ])
          )
        );
      }

      if (
        /\bpg_get_function_identity_arguments\s*\(/i.test(trimmedSql) &&
        /\blanname\b/i.test(trimmedSql) &&
        /\bfuncowner\b/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              integerField("oid"),
              textField("name"),
              textField("lanname"),
              textField("funcowner"),
              textField("description"),
            ],
            rows.map((row) => [
              String(row.oid),
              String(row.name),
              String(row.lanname),
              String(row.funcowner),
              row.description === null ? null : String(row.description),
            ])
          )
        );
      }

      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, rows, [
          integerField("oid"),
          textField("proname"),
          textField("name"),
          textField("lanname"),
          textField("funcowner"),
          textField("description"),
          integerField("proowner"),
          integerField("pronamespace"),
          textField("prokind"),
          integerField("prorettype"),
          booleanField("proretset"),
          integerField("pronargs"),
          textField("proargtypes"),
          textField("proargnames"),
          textField("prosrc"),
          booleanField("prosecdef"),
          booleanField("proisstrict"),
          textField("provolatile"),
          textField("nspname"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_language" ||
      topLevelFromTarget === "pg_catalog.pg_language"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, languageRows(), [
          integerField("oid"),
          textField("lanname"),
          textField("label"),
          textField("value"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_roles" ||
      topLevelFromTarget === "pg_catalog.pg_roles" ||
      topLevelFromTarget === "pg_user" ||
      topLevelFromTarget === "pg_catalog.pg_user"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, roleRows(), [
          textField("rolname"),
          textField("usename"),
          booleanField("rolcanlogin"),
          booleanField("rolsuper"),
          booleanField("usesuper"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_stat_gssapi" ||
      topLevelFromTarget === "pg_catalog.pg_stat_gssapi"
    ) {
      return withAggregateFallback(
        trimmedSql,
        resultFromRows(
          [booleanField("gss_authenticated"), booleanField("encrypted")],
          [["f", "f"]]
        )
      );
    }

    if (
      topLevelFromTarget === "pg_tablespace" ||
      topLevelFromTarget === "pg_catalog.pg_tablespace"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, [
          { oid: "1663", name: "pg_default", owner: "10", description: null },
          { oid: "1664", name: "pg_global", owner: "10", description: null },
        ], [
          integerField("oid"),
          textField("name"),
          integerField("owner"),
          textField("description"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_am" ||
      topLevelFromTarget === "pg_catalog.pg_am"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, accessMethodRows(), [
          integerField("oid"),
          textField("amname"),
          textField("amtype"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_attribute" ||
      topLevelFromTarget === "pg_catalog.pg_attribute"
    ) {
      const relationOid = extractAttributeRelationOid(trimmedSql);
      const attributeRows = relationOid
        ? await buildAttributeRows(this.stdbClient, databaseName, relationOid)
        : [];

      if (
        /\batttypid\b/i.test(trimmedSql) &&
        /\battlen\b/i.test(trimmedSql) &&
        /\btypname\b/i.test(trimmedSql)
      ) {
        const attributeFields = [
          textField("name"),
          integerField("atttypid"),
          integerField("attlen"),
          integerField("attnum"),
          integerField("attndims"),
          integerField("atttypmod"),
          textField("attacl"),
          booleanField("attnotnull"),
          textField("attoptions"),
          textField("attfdwoptions"),
          integerField("attstattarget"),
          textField("attstorage"),
          textField("attidentity"),
          textField("defval"),
          textField("typname"),
          textField("displaytypname"),
          textField("cltype"),
          textField("inheritedfrom"),
          integerField("inheritedid"),
          integerField("elemoid"),
          textField("typnspname"),
          textField("defaultstorage"),
          textField("description"),
          textField("indkey"),
          booleanField("isdup"),
          textField("collspcname"),
          booleanField("is_fk"),
          textField("seclabels"),
          booleanField("is_sys_column"),
          textField("colconstype"),
          textField("genexpr"),
          textField("relname"),
          booleanField("is_view_only"),
          textField("attcompression"),
          integerField("seqtypid"),
        ];

        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            attributeFields,
            attributeRows.map((row) =>
              attributeFields.map((field) => {
                const value = row[field.name];
                return value === null || value === undefined ? null : String(value);
              })
            )
          )
        );
      }

      return withAggregateFallback(
        trimmedSql,
        resultFromRows(
          [
            textField("name"),
            integerField("oid"),
            textField("datatype"),
            textField("displaytypname"),
            booleanField("not_null"),
            booleanField("has_default_val"),
            textField("description"),
            integerField("seqtypid"),
          ],
          attributeRows.map((row) => [
            String(row.name),
            String(row.oid),
            String(row.datatype),
            String(row.displaytypname),
            String(row.not_null),
            String(row.has_default_val),
            row.description === null ? null : String(row.description),
            row.seqtypid === null ? null : String(row.seqtypid),
          ])
        )
      );
    }

    if (
      /has_table_privilege\s*\(/i.test(trimmedSql) &&
      /pgagent\.pga_job/i.test(trimmedSql)
    ) {
      return resultFromRows([booleanField("has_priviledge")], []);
    }

    if (
      topLevelFromTarget === "pg_class" ||
      topLevelFromTarget === "pg_catalog.pg_class"
    ) {
      if (
        /\bautovacuum_enabled\b/i.test(trimmedSql) &&
        /\brelacl_str\b/i.test(trimmedSql) &&
        /\bdefault_amname\b/i.test(trimmedSql)
      ) {
        const relationOidMatch = trimmedSql.match(/\brel\.oid\s*=\s*(\d+)(?:::\w+)?/i);
        const relationOid = relationOidMatch?.[1];
        const propertyRows = relationOid
          ? await buildTablePropertyRows(this.stdbClient, databaseName, relationOid)
          : [];
        const propertyFields = [
          integerField("oid"),
          textField("name"),
          integerField("spcoid"),
          textField("relacl_str"),
          textField("spcname"),
          textField("replica_identity"),
          textField("schema"),
          textField("relowner"),
          textField("relkind"),
          booleanField("is_partitioned"),
          booleanField("relhassubclass"),
          numericField("reltuples"),
          textField("description"),
          textField("conname"),
          textField("conkey"),
          booleanField("isrepl"),
          numericField("triggercount"),
          textField("coll_inherits"),
          numericField("inherited_tables_cnt"),
          booleanField("relpersistence"),
          textField("default_amname"),
          numericField("fillfactor"),
          numericField("parallel_workers"),
          numericField("toast_tuple_target"),
          booleanField("autovacuum_enabled"),
          numericField("autovacuum_vacuum_threshold"),
          numericField("autovacuum_vacuum_scale_factor"),
          numericField("autovacuum_analyze_threshold"),
          numericField("autovacuum_analyze_scale_factor"),
          numericField("autovacuum_vacuum_cost_delay"),
          numericField("autovacuum_vacuum_cost_limit"),
          numericField("autovacuum_freeze_min_age"),
          numericField("autovacuum_freeze_max_age"),
          numericField("autovacuum_freeze_table_age"),
          booleanField("toast_autovacuum_enabled"),
          numericField("toast_autovacuum_vacuum_threshold"),
          numericField("toast_autovacuum_vacuum_scale_factor"),
          numericField("toast_autovacuum_analyze_threshold"),
          numericField("toast_autovacuum_analyze_scale_factor"),
          numericField("toast_autovacuum_vacuum_cost_delay"),
          numericField("toast_autovacuum_vacuum_cost_limit"),
          numericField("toast_autovacuum_freeze_min_age"),
          numericField("toast_autovacuum_freeze_max_age"),
          numericField("toast_autovacuum_freeze_table_age"),
          textField("reloptions"),
          textField("toast_reloptions"),
          numericField("reloftype"),
          textField("amname"),
          textField("typname"),
          integerField("typoid"),
          booleanField("rlspolicy"),
          booleanField("forcerlspolicy"),
          booleanField("hastoasttable"),
          textField("seclabels"),
          booleanField("is_sys_table"),
          textField("partition_scheme"),
        ];

        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            propertyFields,
            propertyRows.map((row) =>
              propertyFields.map((field) => {
                const value = row[field.name];
                return value === null || value === undefined ? null : String(value);
              })
            )
          )
        );
      }

      const relationRows = filterRelationRows(
        trimmedSql,
        await buildRelationRows(this.stdbClient, databaseName)
      );

      if (
        /\bcase\s+c\.relkind\b/i.test(trimmedSql) &&
        /\bpg_get_userbyid\s*\(\s*c\.relowner\s*\)/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              textField("Schema"),
              textField("Name"),
              textField("Type"),
              textField("Owner"),
            ],
            relationRows.map((row) => [
              String(row.schema),
              String(row.name),
              String(row.relation_type),
              String(row.owner),
            ])
          )
        );
      }

      if (
        /\btriggercount\b/i.test(trimmedSql) &&
        /\bhas_enable_triggers\b/i.test(trimmedSql) &&
        /\bis_partitioned\b/i.test(trimmedSql)
      ) {
        return withAggregateFallback(
          trimmedSql,
          resultFromRows(
            [
              integerField("oid"),
              textField("name"),
              numericField("triggercount"),
              numericField("has_enable_triggers"),
              booleanField("is_partitioned"),
              numericField("is_inherits"),
              numericField("is_inherited"),
              textField("description"),
            ],
            relationRows.map((row) => [
              String(row.oid),
              String(row.name),
              String(row.triggercount),
              String(row.has_enable_triggers),
              String(row.is_partitioned),
              String(row.is_inherits),
              String(row.is_inherited),
              row.description === null ? null : String(row.description),
            ])
          )
        );
      }

      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, relationRows, [
          integerField("oid"),
          textField("relname"),
          textField("name"),
          textField("relkind"),
          integerField("relnamespace"),
          numericField("triggercount"),
          numericField("has_enable_triggers"),
          booleanField("is_partitioned"),
          numericField("is_inherits"),
          numericField("is_inherited"),
          textField("description"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_foreign_data_wrapper" ||
      topLevelFromTarget === "pg_catalog.pg_foreign_data_wrapper"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, [], [
          integerField("oid"),
          textField("name"),
          integerField("fdwhandler"),
          integerField("fdwvalidator"),
          textField("description"),
          textField("fdwoptions"),
          textField("fdwowner"),
          textField("acl"),
          textField("fdwvalue"),
          textField("fdwhan"),
        ])
      );
    }

    if (
      topLevelFromTarget === "pg_subscription" ||
      topLevelFromTarget === "pg_catalog.pg_subscription"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, [], [integerField("oid"), textField("name")])
      );
    }

    if (
      topLevelFromTarget === "pg_publication" ||
      topLevelFromTarget === "pg_catalog.pg_publication"
    ) {
      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, [], [integerField("oid"), textField("name")])
      );
    }

    if (
      topLevelFromTarget === "pg_extension" ||
      topLevelFromTarget === "pg_catalog.pg_extension"
    ) {
      if (/\bcount\s*\(/i.test(trimmedSql)) {
        return singleValueResult(numericField("count"), "0");
      }

      return withAggregateFallback(
        trimmedSql,
        projectMetadataRows(trimmedSql, [], [integerField("oid"), textField("extname")])
      );
    }

    if (databaseName === "postgres") {
      throw new Error("The postgres database is metadata-only; connect to an FMS database for table reads");
    }

    if (DML_PATTERN.test(trimmedSql)) {
      if (describeOnly) {
        return emptyResult(parseDmlVerb(trimmedSql) ?? "OK");
      }
      try {
        const rewrittenSql = rewriteTableReferences(trimmedSql);
        const result = await this.stdbClient.query(rewrittenSql, databaseName, {
          preferAdmin: true,
        });
        if (result.length !== 1) {
          throw new Error("Expected a single result set");
        }
        return stdbDmlResultToPgwire(trimmedSql, result[0]) ?? emptyResult("OK");
      } catch (error) {
        remapStdbSqlError(error);
      }
    }

    const aggregateResult = await buildAggregateResult(
      this.stdbClient,
      trimmedSql,
      databaseName
    );
    if (aggregateResult) {
      return aggregateResult;
    }

    const referencedTables = [
      ...findAllMatches(/\bfrom\s+public\."([^"]+)"/gi, trimmedSql),
      ...findAllMatches(/\bfrom\s+public\.([a-zA-Z_][a-zA-Z0-9_]*)/gi, trimmedSql),
    ];

    if (describeOnly && referencedTables.length === 1) {
      const described = await this.stdbClient.describeTable(referencedTables[0], databaseName);
      return stdbResultToPgwire(described);
    }

    const rewrittenSql = rewriteTableReferences(trimmedSql);

    try {
      const result = await this.stdbClient.query(rewrittenSql, databaseName);
      if (result.length !== 1) {
        throw new Error("Expected a single result set");
      }
      return stdbResultToPgwire(result[0]);
    } catch (error) {
      const usesOrder = /\border\s+by\b/i.test(trimmedSql);
      if (!usesOrder) {
        throw error;
      }

      const fallbackSql = stripOrderLimitOffset(rewrittenSql);
      const result = await this.stdbClient.query(fallbackSql, databaseName);
      if (result.length !== 1) {
        throw new Error("Expected a single result set");
      }
      return applyNaiveFilters(trimmedSql, stdbResultToPgwire(result[0]));
    }
  }
}
