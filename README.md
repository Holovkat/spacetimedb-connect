# SpacetimeDB Postgres Shim

This repo now has two paths:

- a snapshot mirror into real Postgres databases
- a read-only pgwire pass-through server for live browsing without copying rows

## Current MVP

First source:

- SpacetimeDB database: `fms-glm-org-tt`
- Target Postgres database: `fms-glm-org-tt`
- Target schema: `public`

Current mirrored tables:

1. `conversation_items`
2. `conversation_sessions`
3. `work_requisitions`
4. `users`
5. `orders`
6. `daily_plans`
7. `customers`
8. `vehicles`
9. `load_plans`
10. `inventory`

The current implementation is:

- read-only
- full refresh only
- discovers all public user tables by default
- can be narrowed with include and exclude filters
- can mirror one source database or every discovered database on the current runtime

## Pass-through MVP

The live path is a pgwire server that lets normal Postgres clients connect without materializing row copies first.

- Host: `127.0.0.1`
- Port: `45434`
- User: `shim`
- Password: `shim`
- Database: `postgres` for metadata or any discovered database like `fms-glm-org-tt`

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

This keeps UI impact minimal:

- point any Postgres UI at one normal Postgres database
- browse the mirrored data under `public`

Longer-term, the same shim can mirror additional SpacetimeDB databases into additional Postgres databases, one connection per logical source database.

Database discovery is now generic:

- first, use `spacetime list` against the configured runtime to get database identities
- then resolve each identity through `GET /v1/database/:identity/names`
- if discovery is incomplete or unavailable, provide explicit names with `STDB_DATABASES`

## Quick start

1. Copy `.env.example` to `.env`
2. Fill in `STDB_AUTH_TOKEN`
   Optional:
   - `STDB_ADMIN_AUTH_TOKEN` for DML if it differs from the read token
   - database-specific `*_DB` / `*_TOKEN` pairs in `~/.secure/.env`
3. Start Postgres:

```bash
npm run postgres:up
```

4. Install dependencies:

```bash
npm install
```

5. Run a full sync:

```bash
npm run sync
```

6. Optionally validate discovery against the live source:

```bash
npm run test:live
```

7. List all currently discovered databases:

```bash
npm run list-databases
```

8. Mirror all discovered databases into Postgres:

```bash
npm run sync-all
```

9. Run the live pgwire pass-through server:

```bash
npm run serve:pgwire
```

## Output model

For each mirrored table the shim:

- recreates the table in Postgres
- adds typed columns where possible
- adds metadata columns:
  - `_shim_source_database`
  - `_shim_synced_at`
  - `_shim_row_hash`

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
- The pgwire server currently listens on `PGWIRE_HOST` / `PGWIRE_PORT` and is intended for live read-only browsing first.
