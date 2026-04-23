import net from "node:net";
import { env } from "../config.js";
import { toPgwireError } from "./error.js";
import {
  buildBindComplete,
  buildCloseComplete,
  buildErrorMessage,
  buildErrorResponse,
  buildExtendedExecuteResponse,
  buildExtendedPortalDescribeResponse,
  buildExtendedStatementDescribeResponse,
  buildParseComplete,
  buildReadyForQuery,
  buildSimpleQueryResponse,
  buildSslDenyResponse,
  buildStartupOkResponse,
  parseBindMessage,
  parseCloseMessage,
  parseDescribeMessage,
  parseExecuteMessage,
  parseMessage,
  parseParseMessage,
  parseQueryMessage,
  parseStartupMessage,
} from "./protocol.js";
import { PgwireQueryRouter } from "./query-router.js";
import type { BoundPortal, PreparedStatement } from "./types.js";

interface ConnectionState {
  databaseName: string;
  userName: string;
  statements: Map<string, PreparedStatement>;
  portals: Map<string, BoundPortal>;
  lastSql?: string;
}

function createInitialState(): ConnectionState {
  return {
    databaseName: "postgres",
    userName: "shim",
    statements: new Map(),
    portals: new Map(),
  };
}

function splitSimpleStatements(sql: string): string[] {
  return sql
    .split(";")
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function quoteBoundValue(value: string | null): string {
  if (value === null) {
    return "NULL";
  }

  if (/^-?\d+(\.\d+)?$/.test(value)) {
    return value;
  }

  if (/^(true|false)$/i.test(value)) {
    return value.toLowerCase();
  }

  return `'${value.replace(/'/g, "''")}'`;
}

function applyBoundParameters(
  sql: string,
  parameterValues: Array<string | null>
): string {
  let result = sql;
  for (let index = parameterValues.length; index >= 1; index -= 1) {
    const pattern = new RegExp(`\\$${index}(?!\\d)`, "g");
    result = result.replace(pattern, quoteBoundValue(parameterValues[index - 1]));
  }
  return result;
}

async function handleClientMessage(
  messageBuffer: Buffer,
  socket: net.Socket,
  state: ConnectionState,
  router: PgwireQueryRouter
): Promise<void> {
  const message = parseMessage(messageBuffer);
  if (message.name === "InsufficientData") {
    return;
  }

  if (message.name === "Unknown") {
    socket.end(buildErrorResponse("Unsupported protocol message"));
    return;
  }

  if (message.name === "Terminate" || message.name === "CancelRequest") {
    socket.end();
    return;
  }

  try {
    switch (message.name) {
      case "SSLRequest":
      case "GSSENCRequest":
        socket.write(buildSslDenyResponse());
        return;
      case "StartupMessage": {
        const startup = parseStartupMessage(message);
        state.databaseName = startup.databaseName;
        state.userName = startup.userName;
        socket.write(buildStartupOkResponse());
        return;
      }
      case "Query": {
        const sql = parseQueryMessage(message);
        state.lastSql = sql;
        console.log(`[pgwire] simple query on ${state.databaseName}: ${sql}`);
        const statements = splitSimpleStatements(sql);
        const responses = [];
        for (const statement of statements) {
          state.lastSql = statement;
          const result = await router.execute(statement, state.databaseName);
          responses.push(buildSimpleQueryResponse(result, false));
        }
        responses.push(buildReadyForQuery());
        socket.write(Buffer.concat(responses));
        return;
      }
      case "Parse": {
        const statement = parseParseMessage(message);
        state.lastSql = statement.sql;
        console.log(
          `[pgwire] parse on ${state.databaseName}: ${statement.sql}`
        );
        statement.fields = await router.describeQuery(statement.sql, state.databaseName);
        state.statements.set(statement.name, statement);
        socket.write(buildParseComplete());
        return;
      }
      case "Bind": {
        const portal = parseBindMessage(message);
        if (!state.statements.has(portal.statementName)) {
          throw new Error(`Prepared statement not found: ${portal.statementName || "<unnamed>"}`);
        }
        state.portals.set(portal.name, portal);
        socket.write(buildBindComplete());
        return;
      }
      case "Describe": {
        const describe = parseDescribeMessage(message);
        const portal =
          describe.target === "portal"
            ? state.portals.get(describe.name)
            : undefined;
        const statement =
          describe.target === "statement"
            ? state.statements.get(describe.name)
            : state.statements.get(portal?.statementName ?? "");
        if (!statement) {
          throw new Error(`Describe target not found: ${describe.name || "<unnamed>"}`);
        }
        if (portal) {
          portal.rowDescriptionSent = true;
          socket.write(buildExtendedPortalDescribeResponse(statement.fields));
          return;
        }
        socket.write(buildExtendedStatementDescribeResponse(statement.fields));
        return;
      }
      case "Execute": {
        const execute = parseExecuteMessage(message);
        const portal = state.portals.get(execute.portalName);
        const statement = state.statements.get(portal?.statementName ?? "");
        if (!statement) {
          throw new Error(`Portal not found: ${execute.portalName || "<unnamed>"}`);
        }
        const sql = applyBoundParameters(
          statement.sql,
          portal?.parameterValues ?? []
        );
        state.lastSql = sql;
        const result = await router.execute(sql, state.databaseName);
        if (execute.maxRows > 0) {
          result.rows = result.rows.slice(0, execute.maxRows);
          result.commandTag = `SELECT ${result.rows.length}`;
        }
        const includeRowDescription =
          result.fields.length > 0 && !(portal?.rowDescriptionSent ?? false);
        if (portal) {
          portal.rowDescriptionSent = true;
        }
        socket.write(buildExtendedExecuteResponse(result, includeRowDescription));
        return;
      }
      case "Sync":
        socket.write(buildReadyForQuery());
        return;
      case "Flush":
        return;
      case "Close": {
        const close = parseCloseMessage(message);
        if (close.target === "statement") {
          state.statements.delete(close.name);
        } else {
          state.portals.delete(close.name);
        }
        socket.write(buildCloseComplete());
        return;
      }
      default:
        socket.write(buildErrorResponse(`Unsupported message: ${message.name}`));
    }
  } catch (error) {
    const pgwireError = toPgwireError(error);
    console.error(
      `[pgwire] error on ${state.databaseName}${state.lastSql ? ` for SQL: ${state.lastSql}` : ""}: ${pgwireError.message}`
    );
    socket.write(
      message.name === "Query"
        ? buildErrorResponse(pgwireError)
        : buildErrorMessage(pgwireError)
    );
  }
}

export function createPgwireServer(router = new PgwireQueryRouter()): net.Server {
  return net.createServer((socket) => {
    const state = createInitialState();
    let clientBuffer = Buffer.alloc(0);

    socket.on("data", async (chunk) => {
      clientBuffer = Buffer.concat([clientBuffer, chunk]);

      while (clientBuffer.length > 0) {
        const parsed = parseMessage(clientBuffer);
        if (parsed.name === "InsufficientData") {
          break;
        }

        const current = Buffer.from(clientBuffer.subarray(0, parsed.length));
        clientBuffer = Buffer.from(clientBuffer.subarray(parsed.length));
        await handleClientMessage(current, socket, state, router);
      }
    });

    socket.on("error", () => {
      socket.destroy();
    });
  });
}

export async function listenPgwireServer(server = createPgwireServer()): Promise<net.Server> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(env.PGWIRE_PORT, env.PGWIRE_HOST, () => {
      server.off("error", reject);
      resolve();
    });
  });
  return server;
}
