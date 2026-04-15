import type { AppConfig } from "./config.js";
import type { DB } from "./db.js";
import { getAvailabilityChart, getAvailabilityHistorySummary } from "./db.js";
import { versionInfo } from "./version.js";
import { isXmonRpcMethod, type XmonRpcError, type XmonRpcMethod, type XmonRpcRequestEnvelope, type XmonRpcResponseEnvelope } from "./xmon-rpc.js";

type CloudConfig = Pick<AppConfig, "xmonApiBase" | "xmonCollectorId" | "xmonApiKey">;

type RpcSessionConnectResponse = {
  ok?: boolean;
  sessionId?: string;
  pollWaitMs?: number;
};

type RpcSessionPullResponse = {
  ok?: boolean;
  request?: unknown;
};

const DEFAULT_POLL_WAIT_MS = 25_000;
const CONNECT_RETRY_BASE_MS = 2_000;
const CONNECT_RETRY_CAP_MS = 15_000;

function sanitizeApiBase(value: string | null | undefined) {
  return String(value ?? "")
    .trim()
    .replace(/\/+$/, "");
}

function backoffDelay(attempt: number, baseMs: number, capMs: number) {
  const exp = Math.min(capMs, baseMs * Math.pow(2, attempt));
  const jitter = Math.random() * 0.2 * exp;
  return Math.round(exp + jitter);
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeDeviceId(value: unknown) {
  const n = Number.parseInt(String(value ?? ""), 10);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function parseRpcRequest(raw: unknown): XmonRpcRequestEnvelope | null {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return null;
  const record = raw as Record<string, unknown>;
  if (typeof record.requestId !== "string" || !record.requestId.trim()) return null;
  if (!isXmonRpcMethod(record.method)) return null;
  const ts = typeof record.ts === "string" && record.ts ? record.ts : new Date().toISOString();
  const params = record.params && typeof record.params === "object" && !Array.isArray(record.params) ? (record.params as Record<string, unknown>) : {};
  return {
    requestId: record.requestId,
    method: record.method,
    ts,
    params,
  };
}

function buildRpcErrorResponse(request: XmonRpcRequestEnvelope, error: XmonRpcError): XmonRpcResponseEnvelope {
  return {
    requestId: request.requestId,
    method: request.method,
    ts: new Date().toISOString(),
    ok: false,
    error,
  };
}

function handleRpcRequest(db: DB, collectorId: string, request: XmonRpcRequestEnvelope): XmonRpcResponseEnvelope {
  try {
    if (request.method === "session.ping") {
      return {
        requestId: request.requestId,
        method: request.method,
        ts: new Date().toISOString(),
        ok: true,
        result: {
          collectorId,
          service: versionInfo.service,
          phase: versionInfo.phase,
          version: versionInfo.version,
          collectorTime: new Date().toISOString(),
        },
      };
    }

    if (request.method === "history-summary.query") {
      const deviceId = normalizeDeviceId(request.params?.deviceId);
      if (!deviceId) {
        return buildRpcErrorResponse(request, { code: "invalid_device_id", message: "deviceId is required" });
      }
      const summary = getAvailabilityHistorySummary(db, deviceId);
      if (!summary) {
        return buildRpcErrorResponse(request, { code: "not_found", message: "device_not_found" });
      }
      return {
        requestId: request.requestId,
        method: request.method,
        ts: new Date().toISOString(),
        ok: true,
        result: summary,
      };
    }

    if (request.method === "availability.query") {
      const deviceId = normalizeDeviceId(request.params?.deviceId);
      const rangeRaw = String(request.params?.range ?? "");
      const resolutionRaw = String(request.params?.resolution ?? "");
      const isValidPair =
        (rangeRaw === "24h" && resolutionRaw === "5m") ||
        (rangeRaw === "7d" && resolutionRaw === "1h") ||
        (rangeRaw === "30d" && resolutionRaw === "1d");
      if (!deviceId || !isValidPair) {
        return buildRpcErrorResponse(request, {
          code: "invalid_query",
          message: "availability query requires deviceId + valid range/resolution pair",
        });
      }
      const chart = getAvailabilityChart(db, deviceId, rangeRaw as "24h" | "7d" | "30d", resolutionRaw as "5m" | "1h" | "1d");
      if (!chart) {
        return buildRpcErrorResponse(request, { code: "not_found", message: "availability_not_found" });
      }
      return {
        requestId: request.requestId,
        method: request.method,
        ts: new Date().toISOString(),
        ok: true,
        result: chart,
      };
    }

    if (request.method === "sla-history.query") {
      return buildRpcErrorResponse(request, {
        code: "not_implemented",
        message: "sla history rpc is not implemented on collector yet",
      });
    }

    return buildRpcErrorResponse(request, { code: "unsupported_method", message: `unsupported method ${request.method}` });
  } catch (err: any) {
    return buildRpcErrorResponse(request, {
      code: "internal_error",
      message: err?.message ?? "collector rpc handler failed",
    });
  }
}

async function postJson(
  cfg: CloudConfig,
  path: string,
  payload: unknown,
): Promise<
  | { ok: true; status: number; body: unknown }
  | { ok: false; status: number; message: string }
> {
  const apiBase = sanitizeApiBase(cfg.xmonApiBase);
  const collectorId = String(cfg.xmonCollectorId ?? "").trim();
  const apiKey = String(cfg.xmonApiKey ?? "").trim();
  if (!apiBase || !collectorId || !apiKey) {
    return { ok: false, status: 0, message: "collector_rpc_missing_config" };
  }

  try {
    const res = await fetch(`${apiBase}/collectors/${encodeURIComponent(collectorId)}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-xmon-api-key": apiKey,
      },
      body: JSON.stringify(payload ?? {}),
    });
    const text = await res.text();
    let body: unknown = null;
    if (text) {
      try {
        body = JSON.parse(text);
      } catch {
        body = text;
      }
    }
    if (!res.ok) {
      const message =
        body && typeof body === "object" && !Array.isArray(body) && typeof (body as Record<string, unknown>).error === "string"
          ? String((body as Record<string, unknown>).error)
          : `rpc_http_${res.status}`;
      return { ok: false, status: res.status, message };
    }
    return { ok: true, status: res.status, body };
  } catch (err: any) {
    return {
      ok: false,
      status: 0,
      message: err?.name === "AbortError" ? "collector_rpc_timeout" : "collector_rpc_unreachable",
    };
  }
}

export function startCollectorRpcSession(db: DB, resolveCloudCfg: () => CloudConfig) {
  let stopped = false;
  let sessionId: string | null = null;
  let pollWaitMs = DEFAULT_POLL_WAIT_MS;
  let backoffAttempt = 0;

  async function disconnectCurrentSession() {
    if (!sessionId) return;
    const currentSessionId = sessionId;
    sessionId = null;
    const cfg = resolveCloudCfg();
    await postJson(cfg, "/session/disconnect", { sessionId: currentSessionId });
  }

  async function runLoop() {
    while (!stopped) {
      const cfg = resolveCloudCfg();
      const collectorId = String(cfg.xmonCollectorId ?? "").trim();
      if (!collectorId || !cfg.xmonApiKey || !cfg.xmonApiBase) {
        sessionId = null;
        await sleep(5_000);
        continue;
      }

      if (!sessionId) {
        const connect = await postJson(cfg, "/session/connect", {
          capabilities: { methods: ["session.ping", "history-summary.query", "availability.query", "sla-history.query"] },
          metadata: {
            service: versionInfo.service,
            phase: versionInfo.phase,
            version: versionInfo.version,
          },
        });
        if (!connect.ok) {
          backoffAttempt += 1;
          await sleep(backoffDelay(backoffAttempt, CONNECT_RETRY_BASE_MS, CONNECT_RETRY_CAP_MS));
          continue;
        }

        const body = (connect.body ?? {}) as RpcSessionConnectResponse;
        if (!body.sessionId || typeof body.sessionId !== "string") {
          backoffAttempt += 1;
          await sleep(backoffDelay(backoffAttempt, CONNECT_RETRY_BASE_MS, CONNECT_RETRY_CAP_MS));
          continue;
        }

        sessionId = body.sessionId;
        pollWaitMs = Number.isFinite(body.pollWaitMs) ? Math.min(30_000, Math.max(0, Math.trunc(body.pollWaitMs as number))) : DEFAULT_POLL_WAIT_MS;
        backoffAttempt = 0;
      }

      const activeSessionId = sessionId;
      if (!activeSessionId) continue;

      const pulled = await postJson(cfg, "/session/pull", {
        sessionId: activeSessionId,
        waitMs: pollWaitMs,
      });

      if (!pulled.ok) {
        if (pulled.status === 401 || pulled.status === 403 || pulled.status === 404 || pulled.status === 409) {
          sessionId = null;
        }
        backoffAttempt += 1;
        await sleep(backoffDelay(backoffAttempt, CONNECT_RETRY_BASE_MS, CONNECT_RETRY_CAP_MS));
        continue;
      }

      backoffAttempt = 0;
      const pullBody = (pulled.body ?? {}) as RpcSessionPullResponse;
      const request = parseRpcRequest(pullBody.request);
      if (!request) {
        continue;
      }

      const response = handleRpcRequest(db, collectorId, request);
      const ack = await postJson(cfg, "/session/respond", {
        sessionId: activeSessionId,
        response,
      });
      if (!ack.ok && (ack.status === 401 || ack.status === 403 || ack.status === 404 || ack.status === 409)) {
        sessionId = null;
      }
    }
  }

  void runLoop().catch((err) => {
    console.error(
      JSON.stringify({
        level: "error",
        msg: "collector_rpc_session_loop_crashed",
        error: err?.message ?? String(err),
      }),
    );
  });

  return () => {
    stopped = true;
    void disconnectCurrentSession();
  };
}
