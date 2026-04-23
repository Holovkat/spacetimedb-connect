export interface PgwireField {
  name: string;
  typeOid: number;
}

export interface PgwireQueryResult {
  fields: PgwireField[];
  rows: Array<Array<string | null>>;
  commandTag: string;
}

export interface StartupParams {
  databaseName: string;
  userName: string;
}

export interface ParsedMessage {
  name: string;
  length: number;
  buffer: Buffer;
}

export interface PreparedStatement {
  name: string;
  sql: string;
  fields: PgwireField[];
}

export interface BoundPortal {
  name: string;
  statementName: string;
  parameterValues: Array<string | null>;
  rowDescriptionSent: boolean;
}
