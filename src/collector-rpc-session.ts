import type { AppConfig } from "./config.js";
import type { DB } from "./db.js";
import {
  createPollTarget,
  enqueuePollJobForTarget,
  findPollProfileByNameAndKind,
  findPollTargetByDeviceProfile,
  getAvailabilityChart,
  getAvailabilityHistorySummary,
  getDeviceById,
  getDevicePollHealth,
  getPollProfileById,
  getSystemSnapshotsForDevice,
  listInterfaceSnapshotsForDevice,
  listLldpNeighborsForDevice,
  listPollProfiles,
  listPollTargets,
  updatePollTarget,
} from "./db.js";
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
const AGENT_PRIMARY_DEVICE_TYPES = new Set([
  "server",
  "workstation",
  "windows-server",
  "linux-server",
  "desktop",
  "laptop",
  "virtual-machine",
  "vm",
]);
const SNMP_PRIMARY_DEVICE_TYPES = new Set([
  "switch",
  "router",
  "firewall",
  "camera",
  "iot-device",
  "sensor",
  "ups",
  "printer",
  "access-point",
  "controller",
  "storage",
]);

type SnmpManagementMode = "snmp" | "agent" | "unknown";
type SnmpReadiness = "ready" | "needs_profile" | "needs_binding" | "waiting_first_poll" | "poll_failing" | "not_applicable";

type SnmpCardResponse = {
  deviceId: string;
  managementMode: SnmpManagementMode;
  snmp: {
    applicable: boolean;
    configured: boolean;
    hasBinding: boolean;
    profileId: string | null;
    profileName: string | null;
    version: string | null;
    securitySummary: string | null;
    lastPollAt: string | null;
    lastSuccessAt: string | null;
    lastFailureAt: string | null;
    lastError: string | null;
    systemName: string | null;
    systemDescription: string | null;
    systemObjectId: string | null;
    serialNumber: string | null;
    interfacesCount: number;
    lldpNeighborsCount: number;
    readiness: SnmpReadiness;
    nextAction: string;
    updatedAt: string | null;
  };
  updatedAt: string;
};

type SnmpConfigApplyResponse = {
  deviceId: string;
  applied: boolean;
  profileId: string | null;
  targetId: number | null;
  disabledTargetIds: number[];
  enqueuedJobId: number | null;
  warnings?: string[];
  snmpCard: SnmpCardResponse;
  updatedAt: string;
};

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

function normalizeDeviceType(value: string | null | undefined) {
  return String(value ?? "")
    .trim()
    .toLowerCase();
}

function inferManagementMode(deviceType: string | null | undefined, hasSnmpBinding: boolean): SnmpManagementMode {
  if (hasSnmpBinding) return "snmp";
  const normalized = normalizeDeviceType(deviceType);
  if (!normalized) return "unknown";
  if (AGENT_PRIMARY_DEVICE_TYPES.has(normalized)) return "agent";
  if (SNMP_PRIMARY_DEVICE_TYPES.has(normalized)) return "snmp";
  return "unknown";
}

function toTsMs(ts: string | null | undefined) {
  if (!ts) return null;
  const ms = Date.parse(ts);
  return Number.isFinite(ms) ? ms : null;
}

function resolveSnmpVersion(profileConfig: Record<string, unknown> | null | undefined) {
  const raw = String((profileConfig ?? {})["version"] ?? "")
    .trim()
    .toLowerCase();
  if (raw === "3" || raw === "v3") return "v3";
  if (raw === "1" || raw === "v1") return "v1";
  if (!raw || raw === "2" || raw === "2c" || raw === "v2" || raw === "v2c") return "v2c";
  return raw;
}

function resolveSnmpSecuritySummary(
  version: string | null,
  profileConfig: Record<string, unknown> | null | undefined,
): string | null {
  if (!version) return null;
  const cfg = (profileConfig ?? {}) as Record<string, unknown>;
  if (version === "v3") {
    const username = String(cfg.username ?? cfg.user ?? "").trim();
    const securityLevelRaw = String(cfg.securityLevel ?? "").trim();
    const securityLevel = securityLevelRaw || "noAuthNoPriv";
    const userLabel = username ? `user ${username}` : "username missing";
    return `${securityLevel} • ${userLabel}`;
  }
  if (version === "v2c") {
    const hasCommunity = String(cfg.community ?? "").trim().length > 0;
    return hasCommunity ? "Community configured" : "Community missing";
  }
  return null;
}

function resolveSnmpReadiness(input: {
  managementMode: SnmpManagementMode;
  applicable: boolean;
  configured: boolean;
  hasBinding: boolean;
  lastPollAt: string | null;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
}) {
  if (input.managementMode === "agent" || !input.applicable) {
    return {
      readiness: "not_applicable" as const,
      nextAction: "Managed by Agent; SNMP is not the primary management path for this device.",
    };
  }
  if (!input.configured) {
    return {
      readiness: "needs_profile" as const,
      nextAction: "Assign an SNMP profile and apply it to the collector.",
    };
  }
  if (!input.hasBinding) {
    return {
      readiness: "needs_binding" as const,
      nextAction: "Apply SNMP configuration so the collector creates an active SNMP binding.",
    };
  }
  if (!input.lastPollAt) {
    return {
      readiness: "waiting_first_poll" as const,
      nextAction: "Waiting for the first SNMP poll after binding was applied.",
    };
  }
  const lastFailureMs = toTsMs(input.lastFailureAt);
  const lastSuccessMs = toTsMs(input.lastSuccessAt);
  if (lastFailureMs !== null && (lastSuccessMs === null || lastFailureMs > lastSuccessMs)) {
    return {
      readiness: "poll_failing" as const,
      nextAction: "SNMP polling is failing. Verify profile credentials, security level, and network reachability.",
    };
  }
  return {
    readiness: "ready" as const,
    nextAction: "SNMP polling is healthy.",
  };
}

function parseSnmpProfileId(raw: unknown): number | null {
  const value = String(raw ?? "").trim();
  if (!value) return null;
  if (/^\d+$/.test(value)) return Number.parseInt(value, 10);
  const pollerMatch = value.match(/^poller-(\d+)$/i);
  if (pollerMatch) return Number.parseInt(pollerMatch[1], 10);
  return null;
}

function buildSnmpCardResponse(db: DB, deviceId: number): SnmpCardResponse | null {
  const device = getDeviceById(db, deviceId);
  if (!device) return null;

  const pollHealth = getDevicePollHealth(db, deviceId);
  const profiles = listPollProfiles(db);
  const profilesById = new Map(profiles.map((profile) => [profile.id, profile]));
  const targets = listPollTargets(db, { deviceId });
  const snmpTargets = targets
    .filter((target) => profilesById.get(target.profileId)?.kind === "snmp")
    .sort((a, b) => b.id - a.id);
  const activeSnmpTarget = snmpTargets.find((target) => target.enabled) ?? snmpTargets[0] ?? null;
  const activeSnmpProfile = activeSnmpTarget ? profilesById.get(activeSnmpTarget.profileId) ?? null : null;

  const systemSnapshots = getSystemSnapshotsForDevice(db, deviceId);
  const latestSystem = Array.isArray(systemSnapshots) && systemSnapshots.length > 0 ? (systemSnapshots[0] as Record<string, unknown>) : null;
  const interfacesCount = listInterfaceSnapshotsForDevice(db, deviceId).length;
  const lldpNeighborsCount = listLldpNeighborsForDevice(db, deviceId).length;

  const managementMode = inferManagementMode(device.type, pollHealth.hasSnmpBinding);
  const applicable = managementMode !== "agent";
  const configured = Boolean(activeSnmpProfile);
  const profileConfig =
    activeSnmpProfile && activeSnmpProfile.config && typeof activeSnmpProfile.config === "object"
      ? (activeSnmpProfile.config as Record<string, unknown>)
      : null;
  const version = configured ? resolveSnmpVersion(profileConfig) : null;
  const securitySummary = configured ? resolveSnmpSecuritySummary(version, profileConfig) : null;

  const readiness = resolveSnmpReadiness({
    managementMode,
    applicable,
    configured,
    hasBinding: pollHealth.hasSnmpBinding,
    lastPollAt: pollHealth.lastSnmpPollAt,
    lastSuccessAt: pollHealth.lastSnmpSuccessAt,
    lastFailureAt: pollHealth.lastSnmpFailureAt,
  });

  const updatedAtCandidates = [
    pollHealth.lastSnmpPollAt,
    pollHealth.lastSnmpSuccessAt,
    pollHealth.lastSnmpFailureAt,
    typeof latestSystem?.collectedAt === "string" ? latestSystem.collectedAt : null,
    device.updatedAt,
  ]
    .map((value) => (typeof value === "string" ? value : null))
    .filter((value): value is string => Boolean(value));

  const latestUpdatedAtMs = updatedAtCandidates.reduce<number | null>((best, ts) => {
    const ms = toTsMs(ts);
    if (ms === null) return best;
    if (best === null || ms > best) return ms;
    return best;
  }, null);
  const updatedAt = latestUpdatedAtMs !== null ? new Date(latestUpdatedAtMs).toISOString() : new Date().toISOString();

  return {
    deviceId: String(deviceId),
    managementMode,
    snmp: {
      applicable,
      configured,
      hasBinding: pollHealth.hasSnmpBinding,
      profileId: activeSnmpProfile ? String(activeSnmpProfile.id) : null,
      profileName: activeSnmpProfile?.name ?? pollHealth.activeSnmpProfile ?? null,
      version,
      securitySummary,
      lastPollAt: pollHealth.lastSnmpPollAt ?? null,
      lastSuccessAt: pollHealth.lastSnmpSuccessAt ?? null,
      lastFailureAt: pollHealth.lastSnmpFailureAt ?? null,
      lastError: pollHealth.lastSnmpError ?? null,
      systemName: typeof latestSystem?.sysName === "string" ? latestSystem.sysName : null,
      systemDescription: typeof latestSystem?.sysDescr === "string" ? latestSystem.sysDescr : null,
      systemObjectId: typeof latestSystem?.sysObjectId === "string" ? latestSystem.sysObjectId : null,
      serialNumber: device.serialNumber ?? (typeof latestSystem?.serialNumber === "string" ? latestSystem.serialNumber : null),
      interfacesCount,
      lldpNeighborsCount,
      readiness: readiness.readiness,
      nextAction: readiness.nextAction,
      updatedAt,
    },
    updatedAt,
  };
}

function applySnmpConfig(
  db: DB,
  input: { deviceId: number; snmpProfileId: unknown; triggerPoll: boolean },
): SnmpConfigApplyResponse {
  const device = getDeviceById(db, input.deviceId);
  if (!device) {
    throw new Error("device_not_found");
  }

  const requestedProfileRaw =
    input.snmpProfileId === null || input.snmpProfileId === undefined ? null : String(input.snmpProfileId).trim();
  if (requestedProfileRaw !== null && !requestedProfileRaw) {
    throw new Error("invalid_snmp_profile_id");
  }

  const allProfiles = listPollProfiles(db);
  const profilesById = new Map(allProfiles.map((profile) => [profile.id, profile]));
  const snmpProfiles = allProfiles.filter((profile) => profile.kind === "snmp");

  let selectedProfile: (typeof snmpProfiles)[number] | null = null;
  if (requestedProfileRaw !== null) {
    const parsedProfileId = parseSnmpProfileId(requestedProfileRaw);
    if (parsedProfileId !== null) {
      const byId = getPollProfileById(db, parsedProfileId);
      if (byId?.kind === "snmp") selectedProfile = byId;
    }
    if (!selectedProfile) {
      const byName = findPollProfileByNameAndKind(db, { kind: "snmp", name: requestedProfileRaw });
      if (byName) selectedProfile = byName;
    }
    if (!selectedProfile) {
      throw new Error("snmp_profile_not_found");
    }
  }

  const targets = listPollTargets(db, { deviceId: input.deviceId });
  const disabledTargetIds: number[] = [];
  let activeTargetId: number | null = null;

  if (selectedProfile) {
    const existingForProfile = findPollTargetByDeviceProfile(db, input.deviceId, selectedProfile.id);
    let activeTarget = existingForProfile;
    if (!activeTarget) {
      activeTarget = createPollTarget(db, { deviceId: input.deviceId, profileId: selectedProfile.id, enabled: true });
    } else if (!activeTarget.enabled) {
      activeTarget = updatePollTarget(db, { id: activeTarget.id, enabled: true }) ?? activeTarget;
    }
    activeTargetId = activeTarget.id;
  }

  const snmpTargets = targets.filter((target) => profilesById.get(target.profileId)?.kind === "snmp");
  for (const target of snmpTargets) {
    const shouldEnable = activeTargetId !== null && target.id === activeTargetId;
    if (shouldEnable) continue;
    if (target.enabled) {
      const updated = updatePollTarget(db, { id: target.id, enabled: false });
      if (updated) disabledTargetIds.push(target.id);
    }
  }

  const warnings: string[] = [];
  let enqueuedJobId: number | null = null;
  if (activeTargetId !== null && input.triggerPoll) {
    try {
      const job = enqueuePollJobForTarget(db, activeTargetId);
      enqueuedJobId = job?.id ?? null;
    } catch (err: any) {
      warnings.push(err?.message ?? "snmp_poll_enqueue_failed");
    }
  }

  const snmpCard = buildSnmpCardResponse(db, input.deviceId);
  if (!snmpCard) {
    throw new Error("device_not_found");
  }

  return {
    deviceId: String(input.deviceId),
    applied: true,
    profileId: selectedProfile ? String(selectedProfile.id) : null,
    targetId: activeTargetId,
    disabledTargetIds,
    enqueuedJobId,
    ...(warnings.length ? { warnings } : {}),
    snmpCard,
    updatedAt: new Date().toISOString(),
  };
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

    if (request.method === "snmp-card.query") {
      const deviceId = normalizeDeviceId(request.params?.deviceId);
      if (!deviceId) {
        return buildRpcErrorResponse(request, { code: "invalid_device_id", message: "deviceId is required" });
      }
      const snmpCard = buildSnmpCardResponse(db, deviceId);
      if (!snmpCard) {
        return buildRpcErrorResponse(request, { code: "not_found", message: "device_not_found" });
      }
      return {
        requestId: request.requestId,
        method: request.method,
        ts: new Date().toISOString(),
        ok: true,
        result: snmpCard,
      };
    }

    if (request.method === "snmp-config.apply") {
      const deviceId = normalizeDeviceId(request.params?.deviceId);
      if (!deviceId) {
        return buildRpcErrorResponse(request, { code: "invalid_device_id", message: "deviceId is required" });
      }
      try {
        const applied = applySnmpConfig(db, {
          deviceId,
          snmpProfileId: request.params?.snmpProfileId,
          triggerPoll: request.params?.triggerPoll !== false,
        });
        return {
          requestId: request.requestId,
          method: request.method,
          ts: new Date().toISOString(),
          ok: true,
          result: applied,
        };
      } catch (err: any) {
        const message = String(err?.message ?? "");
        if (message === "device_not_found") {
          return buildRpcErrorResponse(request, { code: "not_found", message: "device_not_found" });
        }
        if (message === "snmp_profile_not_found" || message === "invalid_snmp_profile_id") {
          return buildRpcErrorResponse(request, { code: "invalid_snmp_profile", message: message === "snmp_profile_not_found" ? "snmp_profile_not_found" : "invalid_snmp_profile_id" });
        }
        throw err;
      }
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
          capabilities: {
            methods: [
              "session.ping",
              "history-summary.query",
              "availability.query",
              "sla-history.query",
              "snmp-card.query",
              "snmp-config.apply",
            ],
          },
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
