import type {
  BoundPortal,
  ParsedMessage,
  PgwireField,
  PgwireQueryResult,
  PreparedStatement,
  StartupParams,
} from "./types.js";
import { PgwireError, toPgwireError } from "./error.js";

const IDENT_LENGTH = 1;
const TYPE_OIDS = {
  bool: 16,
  int4: 23,
  text: 25,
  json: 114,
  float8: 701,
  numeric: 1700,
  timestamptz: 1184,
} as const;

const IDENT_TO_MESSAGE_NAME: Record<number, string> = {
  ["B".charCodeAt(0)]: "Bind",
  ["C".charCodeAt(0)]: "Close",
  ["D".charCodeAt(0)]: "Describe",
  ["E".charCodeAt(0)]: "Execute",
  ["H".charCodeAt(0)]: "Flush",
  ["P".charCodeAt(0)]: "Parse",
  ["p".charCodeAt(0)]: "PasswordMessage",
  ["Q".charCodeAt(0)]: "Query",
  ["S".charCodeAt(0)]: "Sync",
  ["X".charCodeAt(0)]: "Terminate",
};

const UNKNOWN_MESSAGE: ParsedMessage = {
  name: "Unknown",
  length: 0,
  buffer: Buffer.alloc(0),
};

const INSUFFICIENT_DATA: ParsedMessage = {
  name: "InsufficientData",
  length: 0,
  buffer: Buffer.alloc(0),
};

function isCancelRequest(buffer: Buffer): boolean {
  return (
    buffer.at(4) === 4 &&
    buffer.at(5) === 210 &&
    buffer.at(6) === 22 &&
    buffer.at(7) === 46
  );
}

function isGssEncRequest(buffer: Buffer): boolean {
  return (
    buffer.at(4) === 4 &&
    buffer.at(5) === 210 &&
    buffer.at(6) === 22 &&
    buffer.at(7) === 48
  );
}

function isSslRequest(buffer: Buffer): boolean {
  return (
    buffer.at(4) === 4 &&
    buffer.at(5) === 210 &&
    buffer.at(6) === 22 &&
    buffer.at(7) === 47
  );
}

function isStartupMessage(buffer: Buffer): boolean {
  return (
    buffer.at(4) === 0 &&
    buffer.at(5) === 3 &&
    buffer.at(6) === 0 &&
    buffer.at(7) === 0
  );
}

export function parseMessage(buffer: Buffer): ParsedMessage {
  if (buffer.length < 5) {
    return INSUFFICIENT_DATA;
  }

  if (buffer.length >= 8) {
    let unidentifiedName: string | null = null;
    if (isCancelRequest(buffer)) {
      unidentifiedName = "CancelRequest";
    } else if (isGssEncRequest(buffer)) {
      unidentifiedName = "GSSENCRequest";
    } else if (isSslRequest(buffer)) {
      unidentifiedName = "SSLRequest";
    } else if (isStartupMessage(buffer)) {
      unidentifiedName = "StartupMessage";
    }

    if (unidentifiedName) {
      const length = buffer.readUint32BE(0);
      if (buffer.length < length) {
        return INSUFFICIENT_DATA;
      }
      return {
        name: unidentifiedName,
        length,
        buffer: Buffer.from(buffer.subarray(0, length)),
      };
    }
  }

  const name = IDENT_TO_MESSAGE_NAME[buffer.at(0) ?? 0];
  if (!name) {
    if (buffer.length < 8) {
      return INSUFFICIENT_DATA;
    }
    return UNKNOWN_MESSAGE;
  }

  const length = buffer.readUint32BE(1) + IDENT_LENGTH;
  if (buffer.length < length) {
    return INSUFFICIENT_DATA;
  }

  return {
    name,
    length,
    buffer: Buffer.from(buffer.subarray(0, length)),
  };
}

class GrowableOffsetBuffer {
  #buffer = Buffer.alloc(64);
  #offset = 0;

  writeText(value: string): void {
    const byteLength = Buffer.byteLength(value);
    this.#ensureCapacity(byteLength);
    this.#buffer.write(value, this.#offset);
    this.#offset += byteLength;
  }

  writeCString(value: string): void {
    this.writeText(value);
    this.writeUint8(0);
  }

  writeBuffer(value: Buffer): void {
    this.#ensureCapacity(value.length);
    value.copy(this.#buffer, this.#offset);
    this.#offset += value.length;
  }

  writeInt16BE(value: number): void {
    this.#ensureCapacity(2);
    this.#buffer.writeInt16BE(value, this.#offset);
    this.#offset += 2;
  }

  writeInt32BE(value: number): void {
    this.#ensureCapacity(4);
    this.#buffer.writeInt32BE(value, this.#offset);
    this.#offset += 4;
  }

  writeUint8(value: number): void {
    this.#ensureCapacity(1);
    this.#buffer.writeUint8(value, this.#offset);
    this.#offset += 1;
  }

  toBuffer(): Buffer {
    return Buffer.from(this.#buffer.subarray(0, this.#offset));
  }

  #ensureCapacity(chunkLength: number): void {
    while (this.#buffer.byteLength < this.#offset + chunkLength) {
      const next = Buffer.alloc(this.#buffer.byteLength * 2);
      this.#buffer.copy(next, 0, 0, this.#offset);
      this.#buffer = next;
    }
  }
}

function buildMessage(type: string, writeBody?: (buffer: GrowableOffsetBuffer) => void): Buffer {
  const body = new GrowableOffsetBuffer();
  if (writeBody) {
    writeBody(body);
  }

  const bodyBuffer = body.toBuffer();
  const message = new GrowableOffsetBuffer();
  message.writeText(type);
  message.writeInt32BE(bodyBuffer.length + 4);
  message.writeBuffer(bodyBuffer);
  return message.toBuffer();
}

export function buildSslDenyResponse(): Buffer {
  return Buffer.from("N");
}

export function buildStartupOkResponse(): Buffer {
  return Buffer.concat([
    buildMessage("R", (buffer) => buffer.writeInt32BE(0)),
    buildMessage("S", (buffer) => {
      buffer.writeCString("server_version");
      buffer.writeCString("17.0-spacetimedb-shim");
    }),
    buildMessage("S", (buffer) => {
      buffer.writeCString("client_encoding");
      buffer.writeCString("UTF8");
    }),
    buildMessage("K", (buffer) => {
      buffer.writeInt32BE(1);
      buffer.writeInt32BE(2);
    }),
    buildReadyForQuery(),
  ]);
}

export function buildReadyForQuery(): Buffer {
  return buildMessage("Z", (buffer) => buffer.writeText("I"));
}

export function buildCommandComplete(commandTag: string): Buffer {
  return buildMessage("C", (buffer) => buffer.writeCString(commandTag));
}

export function buildParseComplete(): Buffer {
  return buildMessage("1");
}

export function buildBindComplete(): Buffer {
  return buildMessage("2");
}

export function buildCloseComplete(): Buffer {
  return buildMessage("3");
}

export function buildNoData(): Buffer {
  return buildMessage("n");
}

export function buildParameterDescription(count = 0): Buffer {
  return buildMessage("t", (buffer) => {
    buffer.writeInt16BE(count);
  });
}

export function buildRowDescription(fields: PgwireField[]): Buffer {
  return buildMessage("T", (buffer) => {
    buffer.writeInt16BE(fields.length);
    for (const field of fields) {
      buffer.writeCString(field.name);
      buffer.writeInt32BE(0);
      buffer.writeInt16BE(0);
      buffer.writeInt32BE(field.typeOid);
      buffer.writeInt16BE(-1);
      buffer.writeInt32BE(-1);
      buffer.writeInt16BE(0);
    }
  });
}

export function buildDataRow(values: Array<string | null>): Buffer {
  return buildMessage("D", (buffer) => {
    buffer.writeInt16BE(values.length);
    for (const value of values) {
      if (value === null) {
        buffer.writeInt32BE(-1);
        continue;
      }
      const valueBuffer = Buffer.from(value);
      buffer.writeInt32BE(valueBuffer.length);
      buffer.writeBuffer(valueBuffer);
    }
  });
}

export function buildErrorResponse(error: string | PgwireError): Buffer {
  return Buffer.concat([buildErrorMessage(error), buildReadyForQuery()]);
}

export function buildErrorMessage(error: string | PgwireError): Buffer {
  const pgwireError =
    typeof error === "string" ? new PgwireError(error, { code: "0A000" }) : toPgwireError(error);

  return Buffer.concat([
    buildMessage("E", (buffer) => {
      buffer.writeText("S");
      buffer.writeCString(pgwireError.severity);
      buffer.writeText("C");
      buffer.writeCString(pgwireError.code);
      buffer.writeText("M");
      buffer.writeCString(pgwireError.message);
      if (pgwireError.detail) {
        buffer.writeText("D");
        buffer.writeCString(pgwireError.detail);
      }
      if (pgwireError.hint) {
        buffer.writeText("H");
        buffer.writeCString(pgwireError.hint);
      }
      buffer.writeUint8(0);
    }),
  ]);
}

export function buildSimpleQueryResponse(
  result: PgwireQueryResult,
  includeReadyForQuery = true
): Buffer {
  const parts: Buffer[] = [];
  if (result.fields.length > 0) {
    parts.push(buildRowDescription(result.fields));
    for (const row of result.rows) {
      parts.push(buildDataRow(row));
    }
  }
  parts.push(buildCommandComplete(result.commandTag));
  if (includeReadyForQuery) {
    parts.push(buildReadyForQuery());
  }
  return Buffer.concat(parts);
}

export function buildExtendedStatementDescribeResponse(fields: PgwireField[]): Buffer {
  return Buffer.concat([
    buildParameterDescription(0),
    fields.length > 0 ? buildRowDescription(fields) : buildNoData(),
  ]);
}

export function buildExtendedPortalDescribeResponse(fields: PgwireField[]): Buffer {
  return fields.length > 0 ? buildRowDescription(fields) : buildNoData();
}

export function buildExtendedExecuteResponse(
  result: PgwireQueryResult,
  includeRowDescription = false
): Buffer {
  const parts: Buffer[] = [];
  if (result.fields.length > 0) {
    if (includeRowDescription) {
      parts.push(buildRowDescription(result.fields));
    }
    for (const row of result.rows) {
      parts.push(buildDataRow(row));
    }
  }
  parts.push(buildCommandComplete(result.commandTag));
  return Buffer.concat(parts);
}

function readCString(buffer: Buffer, startOffset: number): { value: string; nextOffset: number } {
  const endOffset = buffer.indexOf(0, startOffset);
  if (endOffset === -1) {
    throw new Error("Malformed protocol message");
  }

  return {
    value: buffer.toString("utf8", startOffset, endOffset),
    nextOffset: endOffset + 1,
  };
}

export function parseStartupMessage(message: ParsedMessage): StartupParams {
  let offset = 8;
  const params = new Map<string, string>();
  while (offset < message.buffer.length - 1) {
    const key = readCString(message.buffer, offset);
    offset = key.nextOffset;
    if (!key.value) {
      break;
    }
    const value = readCString(message.buffer, offset);
    offset = value.nextOffset;
    params.set(key.value, value.value);
  }

  return {
    databaseName: params.get("database") || "postgres",
    userName: params.get("user") || "postgres",
  };
}

export function parseQueryMessage(message: ParsedMessage): string {
  return message.buffer.toString("utf8", 5, message.buffer.length - 1).trim();
}

export function parseParseMessage(message: ParsedMessage): PreparedStatement {
  let offset = 5;
  const statement = readCString(message.buffer, offset);
  offset = statement.nextOffset;
  const query = readCString(message.buffer, offset);
  offset = query.nextOffset;
  const parameterCount = message.buffer.readInt16BE(offset);
  offset += 2 + parameterCount * 4;

  return {
    name: statement.value,
    sql: query.value.trim(),
    fields: [],
  };
}

export function parseBindMessage(message: ParsedMessage): BoundPortal {
  let offset = 5;
  const portal = readCString(message.buffer, offset);
  offset = portal.nextOffset;
  const statement = readCString(message.buffer, offset);
  offset = statement.nextOffset;

  const formatCount = message.buffer.readInt16BE(offset);
  offset += 2;
  const formatCodes: number[] = [];
  for (let index = 0; index < formatCount; index += 1) {
    formatCodes.push(message.buffer.readInt16BE(offset));
    offset += 2;
  }

  const valueCount = message.buffer.readInt16BE(offset);
  offset += 2;
  const parameterValues: Array<string | null> = [];
  for (let index = 0; index < valueCount; index += 1) {
    const valueLength = message.buffer.readInt32BE(offset);
    offset += 4;
    if (valueLength === -1) {
      parameterValues.push(null);
      continue;
    }

    const valueBuffer = message.buffer.subarray(offset, offset + valueLength);
    offset += valueLength;
    const formatCode =
      formatCodes.length === 0
        ? 0
        : formatCodes.length === 1
          ? formatCodes[0]
          : formatCodes[index] ?? 0;

    parameterValues.push(
      formatCode === 1 ? valueBuffer.toString("hex") : valueBuffer.toString("utf8")
    );
  }

  const resultFormatCount = message.buffer.readInt16BE(offset);
  offset += 2 + resultFormatCount * 2;

  return {
    name: portal.value,
    statementName: statement.value,
    parameterValues,
    rowDescriptionSent: false,
  };
}

export function parseDescribeMessage(message: ParsedMessage): {
  target: "statement" | "portal";
  name: string;
} {
  const targetByte = message.buffer.toString("utf8", 5, 6);
  const { value } = readCString(message.buffer, 6);
  return {
    target: targetByte === "S" ? "statement" : "portal",
    name: value,
  };
}

export function parseExecuteMessage(message: ParsedMessage): {
  portalName: string;
  maxRows: number;
} {
  const portal = readCString(message.buffer, 5);
  const maxRows = message.buffer.readInt32BE(portal.nextOffset);
  return {
    portalName: portal.value,
    maxRows,
  };
}

export function parseCloseMessage(message: ParsedMessage): {
  target: "statement" | "portal";
  name: string;
} {
  const targetByte = message.buffer.toString("utf8", 5, 6);
  const { value } = readCString(message.buffer, 6);
  return {
    target: targetByte === "S" ? "statement" : "portal",
    name: value,
  };
}

export function toTypeOid(pgType: string): number {
  switch (pgType) {
    case "boolean":
      return TYPE_OIDS.bool;
    case "double precision":
      return TYPE_OIDS.float8;
    case "numeric":
      return TYPE_OIDS.numeric;
    case "jsonb":
      return TYPE_OIDS.json;
    case "timestamptz":
      return TYPE_OIDS.timestamptz;
    case "integer":
      return TYPE_OIDS.int4;
    default:
      return TYPE_OIDS.text;
  }
}
