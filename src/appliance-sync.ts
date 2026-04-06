import type { AppConfig } from "./config.js";
import type { DB } from "./db.js";
import { getAllAppConfig } from "./db.js";
import { configState } from "./config-state.js";
import {
  buildApplianceSummary,
  loadApplianceIdentity,
  setLastError,
  setLastHeartbeatAt,
  setLastRegisterAt,
  updateApplianceOrg,
} from "./appliance-state.js";
import { runtimeState } from "./runtime-state.js";

type SyncResult = { ok: boolean; orgId?: string | null };

async function postJson(url: string, payload: unknown, headers: Record<string, string>) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: JSON.stringify(payload ?? {}),
  });
  let body: any = null;
  try {
    body = await res.json();
  } catch {
    /* ignore */
  }
  return { status: res.status, ok: res.ok, body };
}

function resolveXmonFromDb(cfg: AppConfig, db: DB) {
  const saved = getAllAppConfig(db);
  const apiBase = saved["XMON_API_BASE"] || cfg.xmonApiBase;
  const collectorId = saved["XMON_COLLECTOR_ID"] || cfg.xmonCollectorId;
  const apiKey = saved["XMON_API_KEY"] || cfg.xmonApiKey;
  return { apiBase, collectorId, apiKey };
}

async function sendRegister(cfg: AppConfig, db: DB): Promise<SyncResult> {
  const resolved = resolveXmonFromDb(cfg, db);
  if (!resolved.apiBase || !resolved.apiKey) return { ok: false };
  const identity = loadApplianceIdentity(db, configState.orgId);
  const summary = buildApplianceSummary(db, identity);
  const payload = {
    applianceId: identity.applianceId,
    orgId: identity.orgId ?? configState.orgId ?? null,
    collectorId: resolved.collectorId ?? null,
    summary,
  };
  const res = await postJson(
    `${resolved.apiBase.replace(/\/+$/, "")}/appliances/register`,
    payload,
    { "x-xmon-api-key": resolved.apiKey }
  );
  if (res.ok) {
    const nowIso = new Date().toISOString();
    setLastRegisterAt(db, nowIso);
    if (res.body?.orgId && typeof res.body.orgId === "string") {
      updateApplianceOrg(db, res.body.orgId);
    }
    setLastError(db, null);
    return { ok: true, orgId: res.body?.orgId ?? identity.orgId };
  }
  setLastError(db, res.body?.error ?? `register_http_${res.status}`);
  return { ok: false };
}

async function sendHeartbeat(cfg: AppConfig, db: DB): Promise<SyncResult> {
  const resolved = resolveXmonFromDb(cfg, db);
  if (!resolved.apiBase || !resolved.apiKey) return { ok: false };
  const identity = loadApplianceIdentity(db, configState.orgId);
  const summary = buildApplianceSummary(db, identity);
  const payload = {
    applianceId: identity.applianceId,
    orgId: identity.orgId ?? configState.orgId ?? null,
    collectorId: resolved.collectorId ?? null,
    summary,
  };
  const res = await postJson(
    `${resolved.apiBase.replace(/\/+$/, "")}/appliances/heartbeat`,
    payload,
    { "x-xmon-api-key": resolved.apiKey }
  );
  if (res.ok) {
    const nowIso = new Date().toISOString();
    setLastHeartbeatAt(db, nowIso);
    if (res.body?.orgId && typeof res.body.orgId === "string") {
      updateApplianceOrg(db, res.body.orgId);
    }
    setLastError(db, null);
    return { ok: true, orgId: res.body?.orgId ?? identity.orgId };
  }
  setLastError(db, res.body?.error ?? `heartbeat_http_${res.status}`);
  return { ok: false };
}

export function startApplianceSync(cfg: AppConfig, db: DB) {
  const resolved = resolveXmonFromDb(cfg, db);
  if (!resolved.apiBase || !resolved.apiKey) return () => {};
  const heartbeatMs = Math.max(5000, cfg.xmonHeartbeatMs ?? 15000);
  let stopped = false;

  const identity = loadApplianceIdentity(db, configState.orgId);
  runtimeState.appliance = { ...runtimeState.appliance, applianceId: identity.applianceId, orgId: identity.orgId };

  // initial register fire-and-forget
  sendRegister(cfg, db).catch(() => {});

  async function loop() {
    while (!stopped) {
      await sendHeartbeat(cfg, db).catch(() => {});
      await new Promise((r) => setTimeout(r, heartbeatMs));
    }
  }

  loop();

  return () => {
    stopped = true;
  };
}
