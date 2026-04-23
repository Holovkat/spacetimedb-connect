import { describe, expect, it } from "vitest";
import {
  buildErrorMessage,
  buildExtendedExecuteResponse,
  buildExtendedPortalDescribeResponse,
  buildExtendedStatementDescribeResponse,
  buildSimpleQueryResponse,
} from "../src/pgwire/protocol.js";
import { PgwireError } from "../src/pgwire/error.js";
import type { PgwireField, PgwireQueryResult } from "../src/pgwire/types.js";

function readMessageTypes(buffer: Buffer): string[] {
  const messageTypes: string[] = [];
  let offset = 0;

  while (offset < buffer.length) {
    const type = buffer.toString("utf8", offset, offset + 1);
    const length = buffer.readInt32BE(offset + 1);
    messageTypes.push(type);
    offset += 1 + length;
  }

  return messageTypes;
}

function readErrorFields(buffer: Buffer): Record<string, string> {
  const fields: Record<string, string> = {};
  let offset = 5;

  while (offset < buffer.length) {
    const fieldType = buffer.toString("utf8", offset, offset + 1);
    offset += 1;
    if (fieldType === "\u0000") {
      break;
    }

    const nextOffset = buffer.indexOf(0, offset);
    fields[fieldType] = buffer.toString("utf8", offset, nextOffset);
    offset = nextOffset + 1;
  }

  return fields;
}

describe("pgwire protocol framing", () => {
  const fields: PgwireField[] = [{ name: "id", typeOid: 23 }];
  const result: PgwireQueryResult = {
    fields,
    rows: [["1"]],
    commandTag: "SELECT 1",
  };

  it("returns parameter and row descriptions for statement describe", () => {
    expect(readMessageTypes(buildExtendedStatementDescribeResponse(fields))).toEqual([
      "t",
      "T",
    ]);
  });

  it("returns only row description for portal describe", () => {
    expect(readMessageTypes(buildExtendedPortalDescribeResponse(fields))).toEqual(["T"]);
  });

  it("can prepend row description on execute when the portal was not described", () => {
    expect(readMessageTypes(buildExtendedExecuteResponse(result, true))).toEqual([
      "T",
      "D",
      "C",
    ]);
  });

  it("omits row description on execute after describe", () => {
    expect(readMessageTypes(buildExtendedExecuteResponse(result, false))).toEqual([
      "D",
      "C",
    ]);
  });

  it("can omit ReadyForQuery for intermediate simple-query batch statements", () => {
    expect(readMessageTypes(buildSimpleQueryResponse(result, false))).toEqual([
      "T",
      "D",
      "C",
    ]);
  });

  it("serializes structured pgwire errors with a stable SQLSTATE", () => {
    const message = buildErrorMessage(
      new PgwireError("Read-only pgwire shim: write statements are not supported", {
        code: "25006",
        detail: "Writes are blocked.",
        hint: "Use SELECT only.",
      })
    );

    expect(readMessageTypes(message)).toEqual(["E"]);
    expect(readErrorFields(message)).toMatchObject({
      S: "ERROR",
      C: "25006",
      M: "Read-only pgwire shim: write statements are not supported",
      D: "Writes are blocked.",
      H: "Use SELECT only.",
    });
  });
});
