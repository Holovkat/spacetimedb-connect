# spacetimedb-connect

`spacetimedb-connect` is a SpacetimeDB connector for SQL tooling and interoperability.
It currently includes:

- a pgwire server that lets Postgres clients query SpacetimeDB directly

The examples below use placeholder database names such as `example-app-db`.
The live integration tests in this repo currently run against an FMS-GLM environment, but the connector itself is intended to discover and bridge general SpacetimeDB databases.
For normal day-to-day use of the connector, local Postgres is not required.

## Pgwire-first MVP

If you want to connect a SQL tool directly to SpacetimeDB, the pgwire server is the primary path:

- SpacetimeDB database: `example-app-db`
- Client connection database: `postgres` for metadata or any discovered source database such as `example-app-db`

The pgwire server lets normal Postgres clients connect without materializing row copies first.

- Host: `127.0.0.1`
- Port: `45434`
- User: `shim`
- Password: `shim`
- Database: `postgres` for metadata or any discovered source database such as `example-app-db`

Current pgwire scope:

- startup/auth handshake
- `pg_database` database listing
- `information_schema.tables`
- `information_schema.columns`
- `SELECT`
- authorized `INSERT`, `UPDATE`, and `DELETE`
- simple query protocol
- extended query protocol
- parameter interpolation for extended-query reads and writes
- client-side fallback for `ORDER BY`, `LIMIT`, `OFFSET` when Spacetime SQL does not support them directly
- compatibility handling for common `BEGIN` / `COMMIT`, `SET`, and `SHOW` probes from Postgres clients

Not supported yet:

- `RETURNING`
- DDL and admin statements such as `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `COPY`, and `CALL`
- full PostgreSQL catalog compatibility
- joins across databases
- full DDL/transaction semantics

## Why this shape

This keeps client impact minimal:

- point a Postgres UI at the pgwire server for direct SpacetimeDB access
- optionally compare against a local Postgres mirror when you are debugging or checking alignment

Longer-term, the same shim can mirror additional SpacetimeDB databases into additional Postgres databases, one connection per logical source database.

Database discovery is now generic:

- first, use `spacetime list` against the configured runtime to get database identities
- then resolve each identity through `GET /v1/database/:identity/names`
- if discovery is incomplete or unavailable, provide explicit names with `STDB_DATABASES`

## Quick start: pgwire

1. Copy `.env.example` to `.env`
2. Fill in `STDB_AUTH_TOKEN`
   Optional:
   - `STDB_ADMIN_AUTH_TOKEN` for DML if it differs from the read token
   - database-specific `*_DB` / `*_TOKEN` pairs in `~/.secure/.env`
3. Install dependencies:

```bash
npm install
```

4. Run the pgwire server:

```bash
npm run serve:pgwire
```

5. Optionally validate discovery against the live source:

```bash
npm run test:live
```

6. List all currently discovered databases:

```bash
npm run list-databases
```

## Notes

- This does not emulate the Postgres wire protocol.
- The pgwire path can pass through direct table `INSERT`, `UPDATE`, and `DELETE` statements when the supplied bearer token is authorized for DML on the target SpacetimeDB database.
- The pgwire path does not yet synthesize PostgreSQL `RETURNING` result sets or broader DDL/admin semantics.
- Table discovery comes from Spacetime system tables, specifically `st_table`.
- By default the shim mirrors every `user` + `public` table in the source database.
- Database discovery is generic and comes from `spacetime list` plus the HTTP names endpoint.
- `STDB_DATABASES=db_a,db_b` can be used as an explicit override or supplement when needed.
- The shim loads `~/.secure/.env` as a fallback secret source and recognizes paired `*_DB` / `*_TOKEN` entries for per-database auth mapping.
- If `STDB_ADMIN_AUTH_TOKEN` is present, the shim prefers it for DML while keeping the normal token path for reads.
- Use `SHIM_INCLUDE_TABLES=table_a,table_b` or `SHIM_EXCLUDE_TABLES=table_c,table_d` to narrow the set.
- The pgwire server currently listens on `PGWIRE_HOST` / `PGWIRE_PORT` and is intended for live database tooling first.
- Footnote for debugging/alignment only: if you want a local Postgres copy to compare against SpacetimeDB behavior, start Postgres with `npm run postgres:up` and use `npm run sync` or `npm run sync-all`.
- In that optional mirror mode, the shim recreates tables in Postgres and adds `_shim_source_database`, `_shim_synced_at`, and `_shim_row_hash` metadata columns.
