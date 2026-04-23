export interface PgwireErrorOptions {
  code: string;
  detail?: string;
  hint?: string;
  severity?: "ERROR" | "FATAL";
}

export class PgwireError extends Error {
  readonly code: string;
  readonly detail?: string;
  readonly hint?: string;
  readonly severity: "ERROR" | "FATAL";

  constructor(message: string, options: PgwireErrorOptions) {
    super(message);
    this.name = "PgwireError";
    this.code = options.code;
    this.detail = options.detail;
    this.hint = options.hint;
    this.severity = options.severity ?? "ERROR";
  }
}

export function toPgwireError(error: unknown): PgwireError {
  if (error instanceof PgwireError) {
    return error;
  }

  if (error instanceof Error) {
    return new PgwireError(error.message, { code: "0A000" });
  }

  return new PgwireError("Unexpected pgwire error", { code: "0A000" });
}
