export const XMON_RPC_METHODS = ["session.ping", "history-summary.query", "availability.query", "sla-history.query"] as const;

export type XmonRpcMethod = (typeof XMON_RPC_METHODS)[number];

export type XmonRpcError = {
  code: string;
  message: string;
  retryable?: boolean | undefined;
  details?: unknown | undefined;
};

export type XmonRpcRequestEnvelope = {
  requestId: string;
  method: XmonRpcMethod;
  ts: string;
  params?: Record<string, unknown> | undefined;
};

export type XmonRpcResponseEnvelope = {
  requestId: string;
  method: XmonRpcMethod;
  ts: string;
  ok: boolean;
  result?: unknown | undefined;
  error?: XmonRpcError | undefined;
};

export function isXmonRpcMethod(value: unknown): value is XmonRpcMethod {
  return typeof value === "string" && (XMON_RPC_METHODS as readonly string[]).includes(value);
}
