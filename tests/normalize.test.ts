import { describe, expect, it } from "vitest";
import {
  mapAlgebraicTypeToPgType,
  mapStdbTypeToPgType,
  normalizeResult,
  normalizeValue,
  toPgIdentifier,
  toUniquePgIdentifier,
} from "../src/shim/normalize.js";

describe("normalize helpers", () => {
  it("maps identifiers to postgres-friendly names", () => {
    expect(toPgIdentifier("sessionId")).toBe("session_id");
    expect(toPgIdentifier("sequence-number")).toBe("sequence_number");
  });

  it("unwraps option-like values", () => {
    expect(normalizeValue({ some: "abc" })).toBe("abc");
    expect(normalizeValue({ none: [] })).toBeNull();
  });

  it("maps option-like algebraic types to the wrapped postgres type", () => {
    expect(
      mapAlgebraicTypeToPgType({
        Sum: {
          variants: [
            { name: { some: "some" }, algebraic_type: { F64: [] } },
            { name: { some: "none" }, algebraic_type: { Product: { elements: [] } } },
          ],
        },
      })
    ).toBe("double precision");
  });

  it("maps Spacetime types to usable postgres types", () => {
    expect(mapStdbTypeToPgType("String")).toBe("text");
    expect(mapStdbTypeToPgType("U64")).toBe("numeric");
    expect(mapStdbTypeToPgType("Bool")).toBe("boolean");
    expect(mapStdbTypeToPgType("Sum")).toBe("jsonb");
  });

  it("decodes array-encoded option sums into scalar values and nulls", () => {
    const normalized = normalizeResult("orders", {
      schema: {
        elements: [
          { name: { some: "id" }, algebraic_type: { U64: [] } },
          {
            name: { some: "order_number" },
            algebraic_type: {
              Sum: {
                variants: [
                  { name: { some: "some" }, algebraic_type: { F64: [] } },
                  { name: { some: "none" }, algebraic_type: { Product: { elements: [] } } },
                ],
              },
            },
          },
          {
            name: { some: "customer_po" },
            algebraic_type: {
              Sum: {
                variants: [
                  { name: { some: "some" }, algebraic_type: { String: [] } },
                  { name: { some: "none" }, algebraic_type: { Product: { elements: [] } } },
                ],
              },
            },
          },
        ],
      },
      rows: [
        [37, [0, 1017], [1, []]],
        [38, [1, []], [0, "PO-1"]],
      ],
      total_duration_micros: 0,
      stats: { rows_inserted: 0, rows_deleted: 0, rows_updated: 0 },
    });

    expect(normalized.columns.map((column) => column.pgType)).toEqual([
      "numeric",
      "double precision",
      "text",
    ]);
    expect(normalized.rows).toEqual([
      { id: 37, order_number: 1017, customer_po: null },
      { id: 38, order_number: null, customer_po: "PO-1" },
    ]);
  });

  it("deduplicates postgres identifiers", () => {
    const used = new Set<string>();
    expect(toUniquePgIdentifier("field-name", used, 0)).toBe("field_name");
    expect(toUniquePgIdentifier("field_name", used, 1)).toBe("field_name_2");
  });
});
