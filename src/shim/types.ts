export type StdbAlgebraicType = Record<string, unknown>;

export interface StdbQueryResult {
  schema: {
    elements: Array<{
      name: { some?: string; none?: unknown } | null;
      algebraic_type: StdbAlgebraicType;
    }>;
  };
  rows: unknown[][];
  total_duration_micros: number;
  stats: {
    rows_inserted: number;
    rows_deleted: number;
    rows_updated: number;
  };
}

export interface StdbTableRow {
  tableId: number;
  tableName: string;
  tableType: string;
  tableAccess: string;
}

export interface StdbReducerParam {
  name: string | null;
  pgType: string;
}

export interface StdbRoutineRow {
  routineName: string;
  routineType: "PROCEDURE";
  parameters: StdbReducerParam[];
}

export interface StdbDatabaseSchema {
  reducers?: Array<{
    name: string;
    params?: {
      elements?: Array<{
        name?: { some?: string; none?: unknown } | null;
        algebraic_type: StdbAlgebraicType;
      }>;
    };
  }>;
}

export interface NormalizedColumn {
  sourceName: string;
  pgName: string;
  pgType: string;
  stdbType: string;
}

export interface NormalizedTable {
  tableName: string;
  columns: NormalizedColumn[];
  rows: Record<string, unknown>[];
}
