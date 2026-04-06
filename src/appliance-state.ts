import { randomUUID } from "node:crypto";
import { getAllAppConfig, upsertAppConfigEntries, type DB } from "./db.js";
import { runtimeState, setApplianceIdentity, setApplianceTimestamps } from "./runtime-state.js";
import { configState } from "./config-state.js";
import { versionInfo } from "./version.js";

const KEY_APPLIANCE_ID = "APPLIANCE_ID";
const KEY_APPLIANCE_ORG_ID = "APPLIANCE_ORG_ID";
const KEY_LAST_REGISTER_AT = "APPLIANCE_LAST_REGISTER_AT";
const KEY_LAST_HEARTBEAT_AT = "APPLIANCE_LAST_HEARTBEAT_AT";
const KEY_LAST_ERROR = "APPLIANCE_LAST_ERROR";

export type ApplianceIdentity = {
  applianceId: string;
  orgId: string | null;
};

export type ApplianceSummary = {
  applianceId: string;
  orgId: string | null;
  startedAt: string;
  version: typeof versionInfo;
  bootstrap: typeof runtimeState.bootstrap;
  cloud: typeof runtimeState.cloud;
  workers: typeof runtimeState.workers;
  lastRegisterAt: string | null;
  lastHeartbeatAt: string | null;
  lastError: string | null;
  updatedAt: string;
};

function ensureApplianceId(rows: Record<string, string>): string {
  if (rows[KEY_APPLIANCE_ID]) return rows[KEY_APPLIANCE_ID];
  return `appl-${randomUUID().replace(/-/g, "")}`;
}

export function loadApplianceIdentity(db: DB, orgIdHint?: string | null): ApplianceIdentity {
  const rows = getAllAppConfig(db);
  const applianceId = ensureApplianceId(rows);
  const orgId = rows[KEY_APPLIANCE_ORG_ID]?.trim() || orgIdHint?.trim() || null;

  const updates: Record<string, string> = {};
  if (!rows[KEY_APPLIANCE_ID]) updates[KEY_APPLIANCE_ID] = applianceId;
  if (orgId && rows[KEY_APPLIANCE_ORG_ID] !== orgId) updates[KEY_APPLIANCE_ORG_ID] = orgId;
  if (Object.keys(updates).length) upsertAppConfigEntries(db, updates);

  setApplianceIdentity({ applianceId, orgId });
  return { applianceId, orgId };
}

export function updateApplianceOrg(db: DB, orgId: string) {
  upsertAppConfigEntries(db, { [KEY_APPLIANCE_ORG_ID]: orgId });
  setApplianceIdentity({ applianceId: runtimeState.appliance.applianceId, orgId });
}

export function setLastRegisterAt(db: DB, iso: string) {
  upsertAppConfigEntries(db, { [KEY_LAST_REGISTER_AT]: iso });
  setApplianceTimestamps({ lastRegisterAt: iso });
}

export function setLastHeartbeatAt(db: DB, iso: string) {
  upsertAppConfigEntries(db, { [KEY_LAST_HEARTBEAT_AT]: iso });
  setApplianceTimestamps({ lastHeartbeatAt: iso });
}

export function setLastError(db: DB, error: string | null) {
  if (error === null) {
    upsertAppConfigEntries(db, { [KEY_LAST_ERROR]: "" });
  } else {
    upsertAppConfigEntries(db, { [KEY_LAST_ERROR]: error });
  }
  setApplianceTimestamps({ lastError: error });
}

export function buildApplianceSummary(db: DB, identity?: ApplianceIdentity): ApplianceSummary {
  const rows = getAllAppConfig(db);
  const resolved = identity ?? loadApplianceIdentity(db, configState.orgId);
  const lastRegisterAt = rows[KEY_LAST_REGISTER_AT] ?? null;
  const lastHeartbeatAt = rows[KEY_LAST_HEARTBEAT_AT] ?? null;
  const lastError = rows[KEY_LAST_ERROR] ? rows[KEY_LAST_ERROR] : null;

  return {
    applianceId: resolved.applianceId,
    orgId: resolved.orgId ?? configState.orgId ?? null,
    startedAt: runtimeState.startedAt,
    version: versionInfo,
    bootstrap: runtimeState.bootstrap,
    cloud: runtimeState.cloud,
    workers: runtimeState.workers,
    lastRegisterAt,
    lastHeartbeatAt,
    lastError,
    updatedAt: new Date().toISOString(),
  };
}
