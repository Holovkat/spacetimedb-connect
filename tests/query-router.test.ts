import { beforeAll, describe, expect, it } from "vitest";
import type { StdbTableRow } from "../src/shim/types.js";

const CONTROL_DATABASE = "example-control-db";
const PRIMARY_DATABASE = "example-app-db";
const SECONDARY_DATABASE = "example-ops-db";
let PgwireQueryRouter: typeof import("../src/pgwire/query-router.js").PgwireQueryRouter;

class FakeStdbClient {
  async listDatabases(): Promise<string[]> {
    return [CONTROL_DATABASE, PRIMARY_DATABASE];
  }

  async databaseExists(): Promise<boolean> {
    return true;
  }

  async listPublicTables(): Promise<StdbTableRow[]> {
    return [
      { tableId: 1, tableName: "orders", tableType: "user", tableAccess: "public" },
      { tableId: 2, tableName: "users", tableType: "user", tableAccess: "public" },
    ];
  }

  async listRoutines(): Promise<any[]> {
    return [
      {
        routineName: "update_order_status",
        routineType: "PROCEDURE",
        parameters: [
          { name: "orderId", pgType: "numeric" },
          { name: "status", pgType: "text" },
        ],
      },
      {
        routineName: "cancel_order",
        routineType: "PROCEDURE",
        parameters: [{ name: "orderId", pgType: "numeric" }],
      },
    ];
  }

  async describeTable(): Promise<any> {
    return {
      schema: {
        elements: [
          { name: { some: "id" }, algebraic_type: { U64: [] } },
          { name: { some: "status" }, algebraic_type: { String: [] } },
        ],
      },
      rows: [],
      total_duration_micros: 0,
      stats: { rows_inserted: 0, rows_deleted: 0, rows_updated: 0 },
    };
  }

  async query(sql: string): Promise<any[]> {
    if (/^\s*insert\b/i.test(sql)) {
      return [
        {
          schema: { elements: [] },
          rows: [],
          total_duration_micros: 0,
          stats: { rows_inserted: 1, rows_deleted: 0, rows_updated: 0 },
        },
      ];
    }

    if (/^\s*update\b/i.test(sql)) {
      return [
        {
          schema: { elements: [] },
          rows: [],
          total_duration_micros: 0,
          stats: { rows_inserted: 0, rows_deleted: 0, rows_updated: 2 },
        },
      ];
    }

    if (/^\s*delete\b/i.test(sql)) {
      return [
        {
          schema: { elements: [] },
          rows: [],
          total_duration_micros: 0,
          stats: { rows_inserted: 0, rows_deleted: 3, rows_updated: 0 },
        },
      ];
    }

    if (sql.includes("\"orders\"")) {
      if (/count\(\*\)\s+as\s+reltuples/i.test(sql)) {
        return [
          {
            schema: {
              elements: [{ name: { some: "reltuples" }, algebraic_type: { U64: [] } }],
            },
            rows: [[1]],
            total_duration_micros: 0,
            stats: { rows_inserted: 0, rows_deleted: 0, rows_updated: 0 },
          },
        ];
      }

      return [
        {
          schema: {
            elements: [
              { name: { some: "id" }, algebraic_type: { U64: [] } },
              { name: { some: "status" }, algebraic_type: { String: [] } },
            ],
          },
          rows: [[1, "draft"]],
          total_duration_micros: 0,
          stats: { rows_inserted: 0, rows_deleted: 0, rows_updated: 0 },
        },
      ];
    }

    throw new Error(`Unexpected query: ${sql}`);
  }
}

class UnauthorizedDmlStdbClient extends FakeStdbClient {
  override async query(sql: string): Promise<any[]> {
    if (/^\s*(insert|update|delete)\b/i.test(sql)) {
      throw new Error(
        "Spacetime SQL failed (400 Bad Request): Caller c2009b2abffe109335153b554f3d7c66cec87d10252e462df2fca10ebe8518a3 is not authorized to run SQL DML statements"
      );
    }

    return super.query(sql);
  }
}

describe("pgwire query router", () => {
  let router: InstanceType<typeof PgwireQueryRouter>;

  beforeAll(async () => {
    process.env.STDB_BASE_URL ??= "http://localhost:3000";
    process.env.STDB_AUTH_TOKEN ??= "test-token";
    process.env.STDB_SOURCE_DATABASE ??= PRIMARY_DATABASE;
    ({ PgwireQueryRouter } = await import("../src/pgwire/query-router.js"));
    router = new PgwireQueryRouter(new FakeStdbClient() as never);
  });

  it("lists databases through pg_database", async () => {
    const result = await router.execute(
      "select datname from pg_database order by datname",
      "postgres"
    );

    expect(result.rows.map((row) => row[0])).toEqual([
      PRIMARY_DATABASE,
      CONTROL_DATABASE,
      "postgres",
    ]);
  });

  it("types pg_database oid aliases as integers for pgAdmin", async () => {
    const result = await router.execute(
      "select db.oid as did, db.datname as name from pg_catalog.pg_database db order by datname",
      "postgres"
    );

    expect(result.fields.map((field) => [field.name, field.typeOid])).toEqual([
      ["did", 23],
      ["name", 25],
    ]);
  });

  it("types serverencoding as text when projecting pg_encoding_to_char", async () => {
    const result = await router.execute(
      "select pg_encoding_to_char(db.encoding) as serverencoding from pg_catalog.pg_database db order by datname",
      "postgres"
    );

    expect(result.fields.map((field) => [field.name, field.typeOid])).toEqual([
      ["serverencoding", 25],
    ]);
    expect(result.rows[0]).toEqual(["UTF8"]);
  });

  it("filters pg_database current_database() probes to the actual connected database", async () => {
    const result = await router.execute(
      "SELECT db.oid as did, db.datname FROM pg_catalog.pg_database db WHERE db.datname = current_database()",
      "postgres"
    );

    expect(result.rows).toEqual([["5", "postgres"]]);
  });

  it("returns discovered databases as user databases for pgAdmin browser listing", async () => {
    const result = await router.execute(
      "SELECT db.oid as did, db.datname as name FROM pg_catalog.pg_database db WHERE db.oid > 16383::OID OR db.datname IN ('postgres', 'edb') ORDER BY datname",
      "postgres"
    );

    expect(result.rows).toEqual([
      ["20000", PRIMARY_DATABASE],
      ["20001", CONTROL_DATABASE],
      ["5", "postgres"],
    ]);
  });

  it("filters pg_database detail probes by oid", async () => {
    const result = await router.execute(
      "SELECT db.oid as did, db.datname FROM pg_catalog.pg_database db WHERE db.oid = 20000",
      "postgres"
    );

    expect(result.rows).toEqual([["20000", PRIMARY_DATABASE]]);
  });

  it("returns schema browser metadata for pgAdmin schema support probes", async () => {
    const result = await router.execute(
      "SELECT nsp.nspname as schema_name, (CASE WHEN nspname LIKE 'pg\\_%' OR nspname = 'information_schema' THEN true ELSE false END) as is_catalog, CASE WHEN EXISTS(SELECT 1 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace WHERE pg_proc.pronamespace = pg_namespace.oid AND proname = 'edb_gen_shobj_ddl' AND nspname = 'sys' ) THEN true WHEN EXISTS(SELECT 1 FROM pg_catalog.pg_proc, pg_catalog.pg_namespace WHERE pg_proc.pronamespace = pg_namespace.oid AND proname = 'dbms_metadata_get_ddl' AND nspname = 'pg_catalog' ) THEN true ELSE false END as db_support FROM pg_catalog.pg_namespace nsp WHERE nsp.oid = 2200::OID",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "schema_name",
      "is_catalog",
      "db_support",
    ]);
    expect(result.rows).toEqual([["public", "f", "t"]]);
  });

  it("returns schema browser privilege metadata for pgAdmin", async () => {
    const result = await router.execute(
      "SELECT nsp.oid, nsp.nspname as name, has_schema_privilege(nsp.oid, 'CREATE') as can_create, has_schema_privilege(nsp.oid, 'USAGE') as has_usage, des.description FROM pg_catalog.pg_namespace nsp LEFT OUTER JOIN pg_description des ON (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass) WHERE has_schema_privilege(nsp.oid, 'CREATE, USAGE') ORDER BY nsp.nspname",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "oid",
      "name",
      "can_create",
      "has_usage",
      "description",
    ]);
    expect(result.rows).toEqual([["2200", "public", "f", "t", null]]);
  });

  it("lists non-system schemas without filtering public away", async () => {
    const result = await router.execute(
      "SELECT nsp.oid, nsp.nspname as name, pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create, pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage, des.description FROM pg_catalog.pg_namespace nsp LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass) WHERE nspname NOT LIKE E'pg\\\\_%' AND NOT ((nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1))) ORDER BY nspname",
      PRIMARY_DATABASE
    );

    expect(result.rows).toEqual([["2200", "public", "f", "t", null]]);
  });

  it("filters postgres schema metadata by oid without collapsing to case-expression names", async () => {
    const result = await router.execute(
      "SELECT nsp.nspname as schema_name, (nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1)) AS is_catalog, CASE WHEN nsp.nspname = ANY('{information_schema}') THEN false ELSE true END AS db_support FROM pg_catalog.pg_namespace nsp WHERE nsp.oid = 2200::OID",
      "postgres"
    );

    expect(result.rows).toEqual([["public", "f", "t"]]);
  });

  it("lists postgres system schemas for pgAdmin without leaking public into the catalog branch", async () => {
    const result = await router.execute(
      "SELECT nsp.oid, nsp.nspname as name, pg_catalog.has_schema_privilege(nsp.oid, 'CREATE') as can_create, pg_catalog.has_schema_privilege(nsp.oid, 'USAGE') as has_usage, des.description FROM pg_catalog.pg_namespace nsp LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=nsp.oid AND des.classoid='pg_namespace'::regclass) WHERE ((nsp.nspname = 'pg_catalog' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pg_class' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'pgagent' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'pga_job' AND relnamespace = nsp.oid LIMIT 1)) OR (nsp.nspname = 'information_schema' AND EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relname = 'tables' AND relnamespace = nsp.oid LIMIT 1))) ORDER BY 2",
      "postgres"
    );

    expect(result.rows).toEqual([
      ["12", "ANSI (information_schema)", "f", "t", null],
      ["11", "PostgreSQL Catalog (pg_catalog)", "f", "t", null],
    ]);
  });

  it("lists information_schema tables", async () => {
    const result = await router.execute(
      "select table_name from information_schema.tables where table_schema = 'public' order by table_name",
      PRIMARY_DATABASE
    );

    expect(result.rows.map((row) => row[1] ?? row[0])).toContain("orders");
  });

  it("returns an empty ACL result for pgAdmin table privilege introspection", async () => {
    const result = await router.execute(
      "SELECT 'relacl' as deftype, COALESCE(gt.rolname, 'PUBLIC') grantee, g.rolname grantor, pg_catalog.array_agg(privilege_type) as privileges, pg_catalog.array_agg(is_grantable) as grantable FROM (SELECT d.grantee, d.grantor, d.is_grantable, CASE d.privilege_type WHEN 'SELECT' THEN 'r' ELSE 'UNKNOWN' END AS privilege_type FROM (SELECT rel.relacl FROM pg_catalog.pg_class rel WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid AND rel.oid = 1::oid) acl, (SELECT (d).grantee AS grantee, (d).grantor AS grantor, (d).is_grantable AS is_grantable, (d).privilege_type AS privilege_type FROM (SELECT aclexplode(rel.relacl) as d FROM pg_catalog.pg_class rel WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid AND rel.oid = 1::oid) a ORDER BY privilege_type) d ) d LEFT JOIN pg_catalog.pg_roles g ON (d.grantor = g.oid) LEFT JOIN pg_catalog.pg_roles gt ON (d.grantee = gt.oid) GROUP BY g.rolname, gt.rolname ORDER BY grantee",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "deftype",
      "grantee",
      "grantor",
      "privileges",
      "grantable",
    ]);
    expect(result.rows).toEqual([]);
  });

  it("returns empty pg_tablespace metadata instead of failing in postgres", async () => {
    const result = await router.execute(
      "select ts.oid AS oid, spcname AS name, spcowner as owner, pg_catalog.shobj_description(oid, 'pg_tablespace') AS description from pg_catalog.pg_tablespace ts order by name",
      "postgres"
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "oid",
      "name",
      "owner",
      "description",
    ]);
    expect(result.rows).toEqual([
      ["1663", "pg_default", "10", null],
      ["1664", "pg_global", "10", null],
    ]);
  });

  it("returns numeric vacuum settings from pg_show_all_settings for pgAdmin properties", async () => {
    const result = await router.execute(
      "SELECT name, setting::numeric AS setting FROM pg_catalog.pg_show_all_settings() WHERE name IN('autovacuum_vacuum_threshold','autovacuum_analyze_threshold','autovacuum_vacuum_scale_factor','autovacuum_analyze_scale_factor','autovacuum_vacuum_cost_delay','autovacuum_vacuum_cost_limit','autovacuum_freeze_max_age','vacuum_freeze_min_age','vacuum_freeze_table_age') ORDER BY name",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => [field.name, field.typeOid])).toEqual([
      ["name", 25],
      ["setting", 1700],
    ]);
    expect(result.rows).toEqual([
      ["autovacuum_analyze_scale_factor", "0.1"],
      ["autovacuum_analyze_threshold", "50"],
      ["autovacuum_freeze_max_age", "200000000"],
      ["autovacuum_vacuum_cost_delay", "2"],
      ["autovacuum_vacuum_cost_limit", "-1"],
      ["autovacuum_vacuum_scale_factor", "0.2"],
      ["autovacuum_vacuum_threshold", "50"],
      ["vacuum_freeze_min_age", "50000000"],
      ["vacuum_freeze_table_age", "150000000"],
    ]);
  });

  it("returns empty pg_class table metadata in postgres instead of failing", async () => {
    const result = await router.execute(
      "SELECT rel.oid, rel.relname AS name, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers, (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned, (SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhrelid=rel.oid LIMIT 1) as is_inherits, (SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhparent=rel.oid LIMIT 1) as is_inherited, des.description FROM pg_catalog.pg_class rel LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rel.oid AND des.objsubid=0 AND des.classoid='pg_class'::regclass) WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid AND NOT rel.relispartition ORDER BY rel.relname",
      "postgres"
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "oid",
      "name",
      "triggercount",
      "has_enable_triggers",
      "is_partitioned",
      "is_inherits",
      "is_inherited",
      "description",
    ]);
    expect(result.rows).toEqual([]);
  });

  it("lists table access methods through pg_am for pgAdmin", async () => {
    const result = await router.execute(
      "SELECT oid, amname FROM pg_catalog.pg_am WHERE amtype = 't'",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual(["oid", "amname"]);
    expect(result.rows).toEqual([["2", "heap"]]);
  });

  it("maps public Spacetime tables into pg_class metadata for object browsing", async () => {
    const result = await router.execute(
      "SELECT rel.oid, rel.relname AS name, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers, (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned, (SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhrelid=rel.oid LIMIT 1) as is_inherits, (SELECT count(1) FROM pg_catalog.pg_inherits WHERE inhparent=rel.oid LIMIT 1) as is_inherited, des.description FROM pg_catalog.pg_class rel LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rel.oid AND des.objsubid=0 AND des.classoid='pg_class'::regclass) WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid AND NOT rel.relispartition ORDER BY rel.relname",
      PRIMARY_DATABASE
    );

    expect(result.rows).toEqual([
      ["1", "orders", "0", "0", "f", "0", "0", null],
      ["2", "users", "0", "0", "f", "0", "0", null],
    ]);
  });

  it("returns table property metadata with autovacuum fields for pgAdmin", async () => {
    const result = await router.execute(
      "SELECT rel.oid, rel.relname AS name, rel.reltablespace AS spcoid, rel.relacl AS relacl_str, (CASE WHEN length(spc.spcname::text) > 0 OR rel.relkind = 'p' THEN spc.spcname ELSE (SELECT sp.spcname FROM pg_catalog.pg_database dtb JOIN pg_catalog.pg_tablespace sp ON dtb.dattablespace=sp.oid WHERE dtb.oid = 20003::oid) END) as spcname, (CASE rel.relreplident WHEN 'd' THEN 'default' WHEN 'n' THEN 'nothing' WHEN 'f' THEN 'full' WHEN 'i' THEN 'index' END) as replica_identity, (select nspname FROM pg_catalog.pg_namespace WHERE oid = 2200::oid ) as schema, pg_catalog.pg_get_userbyid(rel.relowner) AS relowner, rel.relkind, (CASE WHEN rel.relkind = 'p' THEN true ELSE false END) AS is_partitioned, rel.relhassubclass, rel.reltuples::bigint, des.description, con.conname, con.conkey, EXISTS(select 1 FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid) AS isrepl, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, (SELECT count(*) FROM pg_catalog.pg_inherits i WHERE i.inhrelid = rel.oid) AS inherited_tables_cnt, (CASE WHEN rel.relpersistence = 'u' THEN true ELSE false END) AS relpersistence, (SELECT st.setting from pg_catalog.pg_show_all_settings() st WHERE st.name = 'default_table_access_method') as default_amname, (substring(pg_catalog.array_to_string(rel.reloptions, ',') FROM 'autovacuum_enabled=([a-z|0-9]*)'))::BOOL AS autovacuum_enabled, (substring(pg_catalog.array_to_string(tst.reloptions, ',') FROM 'autovacuum_enabled=([a-z|0-9]*)'))::BOOL AS toast_autovacuum_enabled, rel.reloptions AS reloptions, tst.reloptions AS toast_reloptions, am.amname, rel.relrowsecurity as rlspolicy, rel.relforcerowsecurity as forcerlspolicy, (CASE WHEN rel.reltoastrelid = 0 THEN false ELSE true END) AS hastoasttable, (CASE WHEN rel.oid <= 16383::oid THEN true ElSE false END) AS is_sys_table FROM pg_catalog.pg_class rel LEFT OUTER JOIN pg_catalog.pg_tablespace spc on spc.oid=rel.reltablespace LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=rel.oid AND des.objsubid=0 AND des.classoid='pg_class'::regclass) LEFT OUTER JOIN pg_catalog.pg_constraint con ON con.conrelid=rel.oid AND con.contype='p' LEFT OUTER JOIN pg_catalog.pg_class tst ON tst.oid = rel.reltoastrelid LEFT OUTER JOIN pg_catalog.pg_am am ON am.oid = rel.relam WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid AND NOT rel.relispartition AND rel.oid = 1::oid ORDER BY rel.relname",
      PRIMARY_DATABASE
    );

    const fieldNames = result.fields.map((field) => field.name);
    expect(fieldNames).toContain("name");
    expect(fieldNames).toContain("schema");
    expect(fieldNames).toContain("relowner");
    expect(fieldNames).toContain("spcname");
    expect(fieldNames).toContain("autovacuum_enabled");
    expect(fieldNames).toContain("toast_autovacuum_enabled");
    expect(fieldNames).toContain("amname");

    const row = Object.fromEntries(
      result.fields.map((field, index) => [field.name, result.rows[0]?.[index] ?? null])
    );
    expect(row).toMatchObject({
      name: "orders",
      schema: "public",
      relowner: "shim",
      spcname: "pg_default",
      autovacuum_enabled: null,
      toast_autovacuum_enabled: null,
      amname: "heap",
    });
  });

  it("supports psql-style table listings over pg_class", async () => {
    const result = await router.execute(
      "SELECT n.nspname as \"Schema\", c.relname as \"Name\", CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as \"Type\", pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\" FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam WHERE c.relkind IN ('r','p','t','s','') AND n.nspname OPERATOR(pg_catalog.~) '^(public)$' COLLATE pg_catalog.default ORDER BY 1,2",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "Schema",
      "Name",
      "Type",
      "Owner",
    ]);
    expect(result.rows).toEqual([
      ["public", "orders", "table", "shim"],
      ["public", "users", "table", "shim"],
    ]);
  });

  it("filters pg_class name lookups by relation oid for pgAdmin table actions", async () => {
    const result = await router.execute(
      "SELECT rel.relname AS name FROM pg_catalog.pg_class rel WHERE rel.relkind IN ('r','s','t','p') AND rel.relnamespace = 2200::oid AND rel.oid = 2::oid",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual(["name"]);
    expect(result.rows).toEqual([["users"]]);
  });

  it("returns no rows for pgagent privilege probes in postgres", async () => {
    const result = await router.execute(
      "SELECT has_table_privilege('pgagent.pga_job', 'INSERT, SELECT, UPDATE') has_priviledge WHERE EXISTS(SELECT has_schema_privilege('pgagent', 'USAGE') WHERE EXISTS(SELECT cl.oid FROM pg_catalog.pg_class cl LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid=relnamespace WHERE relname='pga_job' AND nspname='pgagent'))",
      "postgres"
    );

    expect(result.fields.map((field) => field.name)).toEqual(["has_priviledge"]);
    expect(result.rows).toEqual([]);
  });

  it("returns empty postgres metadata for fdw, subscriptions, publications, and extensions", async () => {
    await expect(
      router.execute(
        "SELECT fdw.oid, fdwname as name, fdwhandler, fdwvalidator, description, fdwoptions AS fdwoptions, pg_catalog.pg_get_userbyid(fdwowner) as fdwowner, pg_catalog.array_to_string(fdwacl::text[], ', ') as acl, NULL as fdwvalue, NULL as fdwhan FROM pg_catalog.pg_foreign_data_wrapper fdw ORDER BY fdwname",
        "postgres"
      )
    ).resolves.toMatchObject({
      fields: expect.arrayContaining([{ name: "oid", typeOid: 23 }, { name: "name", typeOid: 25 }]),
      rows: [],
    });

    await expect(
      router.execute(
        "SELECT oid, sub.subname AS name FROM pg_catalog.pg_subscription sub WHERE sub.subdbid = 20003",
        "postgres"
      )
    ).resolves.toMatchObject({
      fields: expect.arrayContaining([{ name: "oid", typeOid: 23 }, { name: "name", typeOid: 25 }]),
      rows: [],
    });

    await expect(
      router.execute("SELECT oid, pubname AS name FROM pg_catalog.pg_publication", "postgres")
    ).resolves.toMatchObject({
      fields: expect.arrayContaining([{ name: "oid", typeOid: 23 }, { name: "name", typeOid: 25 }]),
      rows: [],
    });

    await expect(
      router.execute(
        "SELECT COUNT(*) FROM pg_extension WHERE extname IN ('edb_job_scheduler', 'dbms_scheduler')",
        "postgres"
      )
    ).resolves.toMatchObject({
      fields: [{ name: "count", typeOid: 1700 }],
      rows: [["0"]],
    });
  });

  it("rewrites public schema reads for Spacetime SQL", async () => {
    const result = await router.execute(
      "select * from public.orders limit 5",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual(["id", "status"]);
    expect(result.rows).toEqual([["1", "draft"]]);
  });

  it("serves pg_type format_type introspection without forwarding it to Spacetime", async () => {
    const result = await router.execute(
      "select oid, pg_catalog.format_type(oid, NULL) as typname from pg_catalog.pg_type where oid = any('{23,25}') order by oid",
      "postgres"
    );

    expect(result.fields.map((field) => field.name)).toEqual(["oid", "typname"]);
    expect(result.rows).toEqual([
      ["23", "integer"],
      ["25", "text"],
    ]);
  });

  it("lists information_schema routines", async () => {
    const result = await router.execute(
      "select routine_name, routine_type from information_schema.routines where routine_schema = 'public' order by routine_name",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "routine_name",
      "routine_type",
    ]);
    expect(result.rows).toEqual([
      ["cancel_order", "PROCEDURE"],
      ["update_order_status", "PROCEDURE"],
    ]);
  });

  it("lists pg_proc metadata for Spacetime reducers as procedures", async () => {
    const result = await router.execute(
      "select proname, prokind, pronargs from pg_catalog.pg_proc where proname = 'update_order_status'",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "proname",
      "prokind",
      "pronargs",
    ]);
    expect(result.rows).toEqual([["update_order_status", "p", "2"]]);
  });

  it("supports psql-style routine listings over pg_proc", async () => {
    const result = await router.execute(
      "SELECT n.nspname as \"Schema\", p.proname as \"Name\", pg_catalog.pg_get_function_result(p.oid) as \"Result data type\", pg_catalog.pg_get_function_arguments(p.oid) as \"Argument data types\", CASE p.prokind WHEN 'a' THEN 'agg' WHEN 'w' THEN 'window' WHEN 'p' THEN 'proc' ELSE 'func' END as \"Type\" FROM pg_catalog.pg_proc p LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname OPERATOR(pg_catalog.~) '^(public)$' COLLATE pg_catalog.default ORDER BY 1, 2, 4",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "Schema",
      "Name",
      "Result data type",
      "Argument data types",
      "Type",
    ]);
    expect(result.rows).toEqual([
      ["public", "cancel_order", "void", "orderId numeric", "proc"],
      ["public", "update_order_status", "void", "orderId numeric, status text", "proc"],
    ]);
  });

  it("supports pgAdmin-style column listings over pg_attribute", async () => {
    const result = await router.execute(
      "SELECT DISTINCT att.attname AS name, att.attnum AS OID, pg_catalog.format_type(ty.oid, NULL) AS datatype, pg_catalog.format_type(ty.oid, att.atttypmod) AS displaytypname, att.attnotnull AS not_null, CASE WHEN att.atthasdef OR att.attidentity <> '' OR ty.typdefault IS NOT NULL THEN true ELSE false END AS has_default_val, des.description, seq.seqtypid FROM pg_catalog.pg_attribute AS att JOIN pg_catalog.pg_type AS ty ON ty.oid = atttypid JOIN pg_catalog.pg_namespace AS tn ON tn.oid = ty.typnamespace JOIN pg_catalog.pg_class AS cl ON cl.oid = att.attrelid JOIN pg_catalog.pg_namespace AS na ON na.oid = cl.relnamespace LEFT JOIN pg_catalog.pg_type AS et ON et.oid = ty.typelem LEFT JOIN pg_catalog.pg_attrdef AS def ON adrelid = att.attrelid AND adnum = att.attnum LEFT JOIN (pg_catalog.pg_depend JOIN pg_catalog.pg_class AS cs ON classid = CAST('pg_class' AS REGCLASS) AND objid = cs.oid AND cs.relkind = 'S') ON refobjid = att.attrelid AND refobjsubid = att.attnum LEFT JOIN pg_catalog.pg_namespace AS ns ON ns.oid = cs.relnamespace LEFT JOIN pg_catalog.pg_index AS pi ON pi.indrelid = att.attrelid AND indisprimary LEFT JOIN pg_catalog.pg_description AS des ON (des.objoid = att.attrelid AND des.objsubid = att.attnum AND des.classoid = CAST('pg_class' AS REGCLASS)) LEFT JOIN pg_catalog.pg_sequence AS seq ON cs.oid = seq.seqrelid WHERE att.attrelid = CAST(1 AS oid) AND att.attnum > 0 AND att.attisdropped IS FALSE",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "name",
      "oid",
      "datatype",
      "displaytypname",
      "not_null",
      "has_default_val",
      "description",
      "seqtypid",
    ]);
    expect(result.rows).toEqual([
      ["id", "1", "numeric", "numeric", "f", "f", null, null],
      ["status", "2", "text", "text", "f", "f", null, null],
    ]);
  });

  it("supports pg_attribute column listings without requiring AS alias syntax", async () => {
    const result = await router.execute(
      "SELECT DISTINCT att.attname AS name, att.attnum AS OID, pg_catalog.format_type(ty.oid, NULL) AS datatype FROM pg_catalog.pg_attribute att JOIN pg_catalog.pg_type ty ON ty.oid = atttypid WHERE att.attrelid = CAST(1 AS oid) AND att.attnum > 0 AND att.attisdropped IS FALSE",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "name",
      "oid",
      "datatype",
      "displaytypname",
      "not_null",
      "has_default_val",
      "description",
      "seqtypid",
    ]);
    expect(result.rows).toEqual([
      ["id", "1", "numeric", "numeric", "f", "f", null, null],
      ["status", "2", "text", "text", "f", "f", null, null],
    ]);
  });

  it("returns rich pg_attribute metadata for pgAdmin column properties", async () => {
    const result = await router.execute(
      "WITH INH_TABLES AS (SELECT at.attname AS name, ph.inhparent AS inheritedid, ph.inhseqno, pg_catalog.concat(nmsp_parent.nspname, '.',parent.relname ) AS inheritedfrom FROM pg_catalog.pg_attribute at JOIN pg_catalog.pg_inherits ph ON ph.inhparent = at.attrelid AND ph.inhrelid = 33896::oid JOIN pg_catalog.pg_class parent ON ph.inhparent = parent.oid JOIN pg_catalog.pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace GROUP BY at.attname, ph.inhparent, ph.inhseqno, inheritedfrom ORDER BY at.attname, ph.inhparent, ph.inhseqno, inheritedfrom) SELECT DISTINCT ON (att.attnum) att.attname as name, att.atttypid, att.attlen, att.attnum, att.attndims, att.atttypmod, att.attacl, att.attnotnull, att.attoptions, att.attfdwoptions, att.attstattarget, att.attstorage, att.attidentity, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval, pg_catalog.format_type(ty.oid,NULL) AS typname, pg_catalog.format_type(ty.oid,att.atttypmod) AS displaytypname, pg_catalog.format_type(ty.oid,att.atttypmod) AS cltype, inh.inheritedfrom, inh.inheritedid, CASE WHEN ty.typelem > 0 THEN ty.typelem ELSE ty.oid END as elemoid, (SELECT nspname FROM pg_catalog.pg_namespace WHERE oid = ty.typnamespace) as typnspname, ty.typstorage AS defaultstorage, description, pi.indkey, (SELECT count(1) FROM pg_catalog.pg_type t2 WHERE t2.typname=ty.typname) > 1 AS isdup, CASE WHEN length(coll.collname::text) > 0 AND length(nspc.nspname::text) > 0 THEN pg_catalog.concat(pg_catalog.quote_ident(nspc.nspname),'.',pg_catalog.quote_ident(coll.collname)) ELSE '' END AS collspcname, EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=att.attrelid AND contype='f' AND att.attnum=ANY(conkey)) As is_fk, (SELECT pg_catalog.array_agg(provider || '=' || label) FROM pg_catalog.pg_seclabels sl1 WHERE sl1.objoid=att.attrelid AND sl1.objsubid=att.attnum) AS seclabels, (CASE WHEN (att.attnum < 1) THEN true ElSE false END) AS is_sys_column, (CASE WHEN (att.attidentity in ('a', 'd')) THEN 'i' WHEN (att.attgenerated in ('s')) THEN 'g' ELSE 'n' END) AS colconstype, (CASE WHEN (att.attgenerated in ('s')) THEN pg_catalog.pg_get_expr(def.adbin, def.adrelid) END) AS genexpr, tab.relname as relname, (CASE WHEN tab.relkind = 'v' THEN true ELSE false END) AS is_view_only, (CASE WHEN att.attcompression = 'p' THEN 'pglz' WHEN att.attcompression = 'l' THEN 'lz4' END) AS attcompression, seq.* FROM pg_catalog.pg_attribute att JOIN pg_catalog.pg_type ty ON ty.oid=atttypid LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid=att.attrelid AND adnum=att.attnum LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=att.attrelid AND des.objsubid=att.attnum AND des.classoid='pg_class'::regclass) LEFT OUTER JOIN (pg_catalog.pg_depend dep JOIN pg_catalog.pg_class cs ON dep.classid='pg_class'::regclass AND dep.objid=cs.oid AND cs.relkind='S') ON dep.refobjid=att.attrelid AND dep.refobjsubid=att.attnum LEFT OUTER JOIN pg_catalog.pg_index pi ON pi.indrelid=att.attrelid AND indisprimary LEFT OUTER JOIN pg_catalog.pg_collation coll ON att.attcollation=coll.oid LEFT OUTER JOIN pg_catalog.pg_namespace nspc ON coll.collnamespace=nspc.oid LEFT OUTER JOIN pg_catalog.pg_sequence seq ON cs.oid=seq.seqrelid LEFT OUTER JOIN pg_catalog.pg_class tab on tab.oid = att.attrelid LEFT OUTER join INH_TABLES as INH ON att.attname = INH.name WHERE att.attrelid = 1::oid AND att.attnum > 0 AND att.attisdropped IS FALSE ORDER BY att.attnum",
      PRIMARY_DATABASE
    );

    const fieldNames = result.fields.map((field) => field.name);
    expect(fieldNames).toContain("atttypid");
    expect(fieldNames).toContain("attnum");
    expect(fieldNames).toContain("typname");
    expect(fieldNames).toContain("indkey");

    const firstRow = Object.fromEntries(
      result.fields.map((field, index) => [field.name, result.rows[0]?.[index] ?? null])
    );
    expect(firstRow).toMatchObject({
      name: "id",
      atttypid: "1700",
      attnum: "1",
      typname: "numeric",
      relname: "orders",
      indkey: "1",
    });
  });

  it("returns grouped edit-mode types for pgAdmin column formatter", async () => {
    const result = await router.execute(
      "SELECT t.main_oid, pg_catalog.ARRAY_AGG(t.typname) as edit_types FROM (SELECT pc.castsource AS main_oid, pg_catalog.format_type(tt.oid,NULL) AS typname FROM pg_catalog.pg_type tt JOIN pg_catalog.pg_cast pc ON tt.oid=pc.casttarget WHERE pc.castsource IN (1700,701,25,16) AND pc.castcontext IN ('i', 'a') UNION SELECT tt.typbasetype AS main_oid, pg_catalog.format_type(tt.oid,NULL) AS typname FROM pg_catalog.pg_type tt WHERE tt.typbasetype IN (1700,701,25,16)) t GROUP BY t.main_oid",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => [field.name, field.typeOid])).toEqual([
      ["main_oid", 23],
      ["edit_types", 1009],
    ]);
    expect(result.rows).toEqual([
      ["1700", "{numeric}"],
      ["701", "{\"double precision\"}"],
      ["25", "{text}"],
      ["16", "{boolean}"],
    ]);
  });

  it("returns single edit-mode type rows for pgAdmin column formatter", async () => {
    const result = await router.execute(
      "SELECT tt.oid, pg_catalog.format_type(tt.oid,NULL) AS typname FROM pg_catalog.pg_type tt JOIN pg_catalog.pg_cast pc ON tt.oid=pc.casttarget WHERE pc.castsource= 1700 AND pc.castcontext IN ('i', 'a') UNION SELECT tt.oid, pg_catalog.format_type(tt.oid,NULL) AS typname FROM pg_catalog.pg_type tt WHERE tt.typbasetype = 1700",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual(["oid", "typname"]);
    expect(result.rows).toEqual([["1700", "numeric"]]);
  });

  it("returns structured procedure type rows for pgAdmin procedure properties", async () => {
    const result = await router.execute(
      "SELECT * FROM ( SELECT pg_catalog.format_type(t.oid, NULL) AS typname, CASE WHEN typelem > 0 THEN typelem ELSE t.oid END AS elemoid, typlen, typtype, t.oid, nspname, (SELECT COUNT(1) FROM pg_catalog.pg_type t2 WHERE t2.typname = t.typname) > 1 AS isdup, CASE WHEN t.typcollation <> 0 THEN true ELSE false END AS is_collatable FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace nsp ON typnamespace=nsp.oid WHERE (NOT (typname = 'unknown' AND nspname = 'pg_catalog')) AND typisdefined AND typtype IN ('b', 'c', 'd', 'e', 'r', 'm') AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace = typnamespace AND relname = typname AND relkind <> 'c') AND (typname NOT LIKE '_%' OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace = typnamespace AND relname = CAST(SUBSTRING(typname FROM 2) AS name) AND relkind <> 'c')) AND nsp.nspname <> 'information_schema' UNION SELECT 'smallserial', 0, 2, 'b', 0, 'pg_catalog', false, false UNION SELECT 'bigserial', 0, 8, 'b', 0, 'pg_catalog', false, false UNION SELECT 'serial', 0, 4, 'b', 0, 'pg_catalog', false, false ) AS dummy ORDER BY 1",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "typname",
      "elemoid",
      "typlen",
      "typtype",
      "oid",
      "nspname",
      "isdup",
      "is_collatable",
    ]);
    expect(result.rows).toContainEqual(["boolean", "16", "1", "b", "16", "pg_catalog", "f", "f"]);
  });

  it("returns rich pg_proc metadata for pgAdmin procedure properties", async () => {
    const result = await router.execute(
      "SELECT pr.oid, pr.xmin, CASE WHEN pr.prokind = 'w' THEN true ELSE false END AS proiswindow, pr.prosrc, pr.prosrc AS prosrc_c, pr.pronamespace, pr.prolang, pr.procost, pr.prorows, pr.prokind, pr.prosecdef, pr.proleakproof, pr.proisstrict, pr.proretset, pr.provolatile, pr.proparallel, pr.pronargs, pr.prorettype, pr.proallargtypes, pr.proargmodes, pr.probin, pr.proacl, pr.proname, pr.proname AS name, pg_catalog.pg_get_function_result(pr.oid) AS prorettypename, typns.nspname AS typnsp, lanname, proargnames, pg_catalog.oidvectortypes(proargtypes) AS proargtypenames, pg_catalog.pg_get_expr(proargdefaults, 'pg_catalog.pg_class'::regclass) AS proargdefaultvals, pg_catalog.pg_get_function_sqlbody(pr.oid) AS prosrc_sql, CASE WHEN pr.prosqlbody IS NOT NULL THEN true ELSE false END as is_pure_sql, pr.pronargdefaults, proconfig, pg_catalog.pg_get_userbyid(proowner) AS funcowner, description, (SELECT array_agg(DISTINCT e.extname) FROM pg_depend d JOIN pg_extension e ON e.oid = d.refobjid WHERE d.classid = 'pg_proc'::regclass AND d.objid = pr.oid AND d.refclassid = 'pg_extension'::regclass) AS dependsonextensions, (CASE WHEN pg_catalog.pg_get_function_identity_arguments(pr.oid) <> '' THEN pr.proname || '(' || pg_catalog.pg_get_function_identity_arguments(pr.oid) || ')' ELSE pr.proname::text END) as name_with_args, (SELECT pg_catalog.array_agg(provider || '=' || label) FROM pg_catalog.pg_seclabel sl1 WHERE sl1.objoid=pr.oid) AS seclabels FROM pg_catalog.pg_proc pr JOIN pg_catalog.pg_type typ ON typ.oid=prorettype JOIN pg_catalog.pg_namespace typns ON typns.oid=typ.typnamespace JOIN pg_catalog.pg_language lng ON lng.oid=prolang LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=pr.oid AND des.classoid='pg_proc'::regclass) WHERE pr.prokind = 'p' AND typname NOT IN ('trigger', 'event_trigger') AND pr.oid = 20000::oid ORDER BY proname",
      PRIMARY_DATABASE
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "oid",
      "xmin",
      "proiswindow",
      "prosrc",
      "prosrc_c",
      "pronamespace",
      "prolang",
      "procost",
      "prorows",
      "prokind",
      "prosecdef",
      "proleakproof",
      "proisstrict",
      "proretset",
      "provolatile",
      "proparallel",
      "pronargs",
      "prorettype",
      "proallargtypes",
      "proargmodes",
      "probin",
      "proacl",
      "proname",
      "name",
      "prorettypename",
      "typnsp",
      "lanname",
      "proargnames",
      "proargtypenames",
      "proargdefaultvals",
      "prosrc_sql",
      "is_pure_sql",
      "pronargdefaults",
      "proconfig",
      "funcowner",
      "description",
      "dependsonextensions",
      "name_with_args",
      "seclabels",
    ]);
    expect(result.rows).toEqual([
      [
        "20000",
        "0",
        "f",
        "update_order_status",
        "update_order_status",
        "2200",
        "1",
        "100",
        "0",
        "p",
        "f",
        "f",
        "f",
        "f",
        "v",
        "u",
        "2",
        "2278",
        "{1700,25}",
        "{i,i}",
        null,
        null,
        "update_order_status",
        "update_order_status",
        "void",
        "pg_catalog",
        "spacetimedb",
        "{\"orderId\",\"status\"}",
        "numeric, text",
        null,
        null,
        "f",
        "0",
        null,
        "shim",
        null,
        null,
        "update_order_status(orderId numeric, status text)",
        null,
      ],
    ]);
  });

  it("returns empty procedure listings in postgres instead of failing", async () => {
    const result = await router.execute(
      "SELECT pr.oid, CASE WHEN pg_catalog.pg_get_function_identity_arguments(pr.oid) <> '' THEN pr.proname || '(' || pg_catalog.pg_get_function_identity_arguments(pr.oid) || ')' ELSE pr.proname::text END AS name, lanname, pg_catalog.pg_get_userbyid(proowner) AS funcowner, description FROM pg_catalog.pg_proc pr JOIN pg_catalog.pg_type typ ON typ.oid=prorettype JOIN pg_catalog.pg_language lng ON lng.oid=prolang LEFT OUTER JOIN pg_catalog.pg_description des ON (des.objoid=pr.oid AND des.classoid='pg_proc'::regclass) WHERE pr.prokind = 'p'::char AND pronamespace = 2200::oid AND typname NOT IN ('trigger', 'event_trigger') ORDER BY proname",
      "postgres"
    );

    expect(result.fields.map((field) => field.name)).toEqual([
      "oid",
      "name",
      "lanname",
      "funcowner",
      "description",
    ]);
    expect(result.rows).toEqual([]);
  });

  it("passes through insert/update/delete with postgres command tags", async () => {
    await expect(
      router.execute("insert into public.orders (id) values (1)", PRIMARY_DATABASE)
    ).resolves.toMatchObject({ fields: [], rows: [], commandTag: "INSERT 0 1" });

    await expect(
      router.execute("update public.orders set status = 'done' where id = 1", PRIMARY_DATABASE)
    ).resolves.toMatchObject({ fields: [], rows: [], commandTag: "UPDATE 2" });

    await expect(
      router.execute("delete from public.orders where id = 1", PRIMARY_DATABASE)
    ).resolves.toMatchObject({ fields: [], rows: [], commandTag: "DELETE 3" });
  });

  it("rejects returning on writes with a stable unsupported-feature error", async () => {
    await expect(
      router.execute(
        "insert into public.orders (id) values (1) returning id",
        PRIMARY_DATABASE
      )
    ).rejects.toMatchObject({
      code: "0A000",
      message: "RETURNING is not supported by this pgwire shim yet",
    });
  });

  it("maps upstream DML authorization failures to insufficient_privilege", async () => {
    const unauthorizedRouter = new PgwireQueryRouter(
      new UnauthorizedDmlStdbClient() as never
    );

    await expect(
      unauthorizedRouter.execute(
        "update public.orders set status = 'done' where id = 1",
        SECONDARY_DATABASE
      )
    ).rejects.toMatchObject({
      code: "42501",
      message: "Permission denied for SQL DML on this SpacetimeDB database",
    });
  });
});
