import type {
  NormalizedColumn,
  NormalizedTable,
  StdbAlgebraicType,
  StdbQueryResult,
} from "./types.js";

type SumVariant = {
  name?: { some?: string; none?: unknown } | null;
  algebraic_type: StdbAlgebraicType;
};

type ProductElement = {
  name?: { some?: string; none?: unknown } | null;
  algebraic_type: StdbAlgebraicType;
};

export function normalizeResult(
  tableName: string,
  result: StdbQueryResult
): NormalizedTable {
  const usedPgNames = new Set<string>();
  const columns = result.schema.elements.map((element, index) => {
    const sourceName = element.name?.some ?? `column_${index + 1}`;
    const stdbType = Object.keys(element.algebraic_type)[0] ?? "Unknown";
    const pgName = toUniquePgIdentifier(
      sourceName || `column_${index + 1}`,
      usedPgNames,
      index
    );

    return {
      sourceName,
      pgName,
      pgType: mapAlgebraicTypeToPgType(element.algebraic_type),
      stdbType,
    } satisfies NormalizedColumn;
  });

  const rows = result.rows.map((values) => {
    const row: Record<string, unknown> = {};
    for (const [index, value] of values.entries()) {
      row[columns[index].pgName] = normalizeTypedValue(
        value,
        result.schema.elements[index]?.algebraic_type
      );
    }
    return row;
  });

  return { tableName, columns, rows };
}

export function toPgIdentifier(value: string): string {
  const normalized = value
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[^a-zA-Z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toLowerCase();

  return normalized || "column";
}

export function toUniquePgIdentifier(
  value: string,
  usedPgNames: Set<string>,
  index: number
): string {
  const base = toPgIdentifier(value);

  if (!usedPgNames.has(base)) {
    usedPgNames.add(base);
    return base;
  }

  const withIndex = `${base}_${index + 1}`;
  usedPgNames.add(withIndex);
  return withIndex;
}

export function mapStdbTypeToPgType(stdbType: string): string {
  switch (stdbType) {
    case "Bool":
      return "boolean";
    case "F32":
    case "F64":
      return "double precision";
    case "I8":
    case "I16":
    case "I32":
    case "I64":
    case "I128":
    case "I256":
    case "U8":
    case "U16":
    case "U32":
    case "U64":
    case "U128":
    case "U256":
      return "numeric";
    case "String":
      return "text";
    default:
      return "jsonb";
  }
}

export function mapAlgebraicTypeToPgType(algebraicType: StdbAlgebraicType): string {
  const optionValueType = getOptionValueType(algebraicType);
  if (optionValueType) {
    return mapAlgebraicTypeToPgType(optionValueType);
  }

  return mapStdbTypeToPgType(getAlgebraicTypeName(algebraicType));
}

function getAlgebraicTypeName(algebraicType?: StdbAlgebraicType): string {
  return Object.keys(algebraicType ?? {})[0] ?? "Unknown";
}

function getSumVariants(algebraicType?: StdbAlgebraicType): SumVariant[] {
  const sumValue =
    algebraicType && "Sum" in algebraicType
      ? (algebraicType.Sum as { variants?: SumVariant[] })
      : undefined;

  return sumValue?.variants ?? [];
}

function getProductElements(algebraicType?: StdbAlgebraicType): ProductElement[] {
  const productValue =
    algebraicType && "Product" in algebraicType
      ? (algebraicType.Product as { elements?: ProductElement[] })
      : undefined;

  return productValue?.elements ?? [];
}

function isUnitType(algebraicType?: StdbAlgebraicType): boolean {
  return getAlgebraicTypeName(algebraicType) === "Product" &&
    getProductElements(algebraicType).length === 0;
}

function getOptionValueType(
  algebraicType?: StdbAlgebraicType
): StdbAlgebraicType | null {
  const variants = getSumVariants(algebraicType);
  if (variants.length !== 2) {
    return null;
  }

  const someVariant = variants.find((variant) => variant.name?.some === "some");
  const noneVariant = variants.find((variant) => variant.name?.some === "none");

  if (!someVariant || !noneVariant || !isUnitType(noneVariant.algebraic_type)) {
    return null;
  }

  return someVariant.algebraic_type;
}

function normalizeSumValue(
  value: unknown,
  algebraicType?: StdbAlgebraicType
): unknown {
  const variants = getSumVariants(algebraicType);
  if (variants.length === 0) {
    return normalizeValue(value);
  }

  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 1) {
      const [variantName, inner] = entries[0];
      const variant = variants.find((entry) => entry.name?.some === variantName);
      if (variantName === "none") {
        return null;
      }
      if (variantName === "some") {
        return normalizeTypedValue(inner, variant?.algebraic_type);
      }
      if (variant) {
        if (isUnitType(variant.algebraic_type)) {
          return variantName;
        }
        return {
          [variantName]: normalizeTypedValue(inner, variant.algebraic_type),
        };
      }
    }
  }

  if (!Array.isArray(value) || value.length !== 2 || typeof value[0] !== "number") {
    return normalizeValue(value);
  }

  const variantIndex = value[0];
  const variant = variants[variantIndex];
  if (!variant) {
    return normalizeValue(value);
  }

  const variantName = variant.name?.some ?? `variant_${variantIndex}`;
  if (variantName === "none") {
    return null;
  }

  if (variantName === "some") {
    return normalizeTypedValue(value[1], variant.algebraic_type);
  }

  if (isUnitType(variant.algebraic_type)) {
    return variantName;
  }

  return {
    [variantName]: normalizeTypedValue(value[1], variant.algebraic_type),
  };
}

function normalizeProductValue(
  value: unknown,
  algebraicType?: StdbAlgebraicType
): unknown {
  const elements = getProductElements(algebraicType);
  if (elements.length === 0) {
    return null;
  }

  if (!Array.isArray(value)) {
    return normalizeValue(value);
  }

  const normalizedValues = elements.map((element, index) =>
    normalizeTypedValue(value[index], element.algebraic_type)
  );
  const namedElements = elements.filter((element) => element.name?.some);

  if (namedElements.length === elements.length) {
    return Object.fromEntries(
      elements.map((element, index) => [element.name?.some ?? `field_${index}`, normalizedValues[index]])
    );
  }

  return normalizedValues;
}

function normalizeTypedValue(
  value: unknown,
  algebraicType?: StdbAlgebraicType
): unknown {
  const typeName = getAlgebraicTypeName(algebraicType);

  if (typeName === "Sum") {
    return normalizeSumValue(value, algebraicType);
  }

  if (typeName === "Product") {
    return normalizeProductValue(value, algebraicType);
  }

  return normalizeValue(value);
}

export function normalizeValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return null;
  }

  if (Array.isArray(value)) {
    return JSON.stringify(value.map((entry) => normalizeValue(entry)));
  }

  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 1) {
      const [tag, inner] = entries[0];
      if (tag === "some") {
        return normalizeValue(inner);
      }
      if (tag === "none") {
        return null;
      }
    }

    return JSON.stringify(value);
  }

  return value;
}
