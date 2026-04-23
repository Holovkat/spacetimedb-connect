# AGENTS.md instructions for /Users/tonyholovka/workspace/spacetimedb-ui

<INSTRUCTIONS>
This repository is the source project for `spacetimedb-connect`: a TypeScript bridge that lets Postgres tooling work against SpacetimeDB through two paths:

- snapshot sync into a real Postgres database
- a pgwire server that exposes SpacetimeDB through a Postgres-compatible interface

## First read
- Read `README.md` before changing behavior.
- If `/docs`, `/design`, or `/features` exists, read the relevant files before editing code.
- When docs and tests disagree, trust the code and tests first, then fix the docs explicitly.

## Project notes
- Keep durable project notes in AFFiNE under `Projects/spacetimedb-connect`.
- Record code check-ins after commits and capture reusable lessons when behavior, compatibility, or workflow changes.
- Use `jcodemunch` and `jdocmunch` for code/doc lookup when those indexes are available, especially before broad structural changes.

## Architecture map
- `src/main.ts`: CLI entrypoint and top-level command dispatch for `sync`, discovery, and pgwire serving.
- `src/config.ts`: env loading, `~/.secure/.env` fallback, token resolution, and config validation.
- `src/shim/*`: SpacetimeDB discovery/querying, row normalization, and Postgres table refresh/materialization.
- `src/pgwire/*`: protocol framing, server connection state, SQL routing, metadata emulation, and Postgres-client compatibility.
- `tests/*`: executable support matrix. Treat tests as the current behavior contract.

## Behavioral guardrails
- Do not assume pgwire is read-only. Current code and tests support metadata queries, `SELECT`, extended query flow, and authorized `INSERT` / `UPDATE` / `DELETE`.
- Current unsupported surface includes DDL/admin statements, `CALL`, `COPY`, `TRUNCATE`, `RETURNING`, and full PostgreSQL catalog or transaction semantics.
- `BEGIN` / `COMMIT` / `ROLLBACK`, `SET`, and `SHOW` are compatibility shims. Do not accidentally turn them into real transaction handling without an explicit requirement.
- Keep database discovery generic. Do not hardcode `fms-glm` names into production logic unless the user explicitly asks for tenant-specific behavior.
- Preserve support for paired `*_DB` / `*_TOKEN` env mappings and the `~/.secure/.env` fallback path.
- Never commit secrets, `.env`, `node_modules`, or generated `dist`.

## Change rules
- Keep command names and operator workflows stable unless the user asks for a breaking change.
- Isolate pgwire compatibility changes to `src/pgwire/*` and sync/materialization changes to `src/shim/*` whenever possible.
- Prefer small dispatching helpers and table-driven routing over growing nested `if` / `else` chains.
- When adding compatibility for a Postgres client probe, add or update a focused test that locks the behavior down.
- Keep error remapping explicit. If a SpacetimeDB error is translated into PostgreSQL semantics, preserve the SQLSTATE, detail, and hint behavior intentionally.

## Validation
- Run `npm run build` after any code change.
- Run `npm test` for normal validation.
- If config parsing changes, verify `tests/config.test.ts`.
- If type normalization or schema mapping changes, verify `tests/normalize.test.ts`.
- If pgwire protocol, routing, or metadata emulation changes, verify `tests/protocol.test.ts` and `tests/query-router.test.ts`.
- Use `npm run test:live` and `tests/pgwire-live.test.ts` only when live SpacetimeDB credentials are available.
- If Docker or Postgres bootstrap changes, verify `docker-compose.yml` and `npm run postgres:up`.

## Documentation expectations
- Keep `README.md` aligned with the actual tested support matrix.
- When behavior changes for DB clients, document the exact supported and unsupported SQL surface.
- When this project is moved into the `Holovkat/spacetimedb-connect` repo, preserve this file and the remote `LICENSE`.
</INSTRUCTIONS>
