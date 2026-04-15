import { setCloudState } from "./runtime-state.js";
import type { AppConfig } from "./config.js";
import type { DB } from "./db.js";
import { setLicenseState } from "./db.js";
import { sendPing, fetchCollectorConfig, type CloudAuthState } from "./xmon-client.js";
import { enqueueTelemetry, startTelemetryQueue } from "./telemetry-queue.js";
import { getAllAppConfig, getDevicePollHealth, listDevices, listPollProfiles, listPollTargets, updateCloudSyncState, upsertAppConfigEntries } from "./db.js";
import { enqueueDeviceSnapshot, enqueueDeviceState, enqueueSnmpProfileSnapshot, enqueueSnmpPollerSnapshot } from "./telemetry-queue.js";
import { startCollectorRpcSession } from "./collector-rpc-session.js";

type BackoffState = { attempts: number };

function backoffDelay(attempts: number, baseMs: number, capMs: number) {
  const exp = Math.min(capMs, baseMs * Math.pow(2, attempts));
  const jitter = Math.random() * 0.25 * exp;
  return exp + jitter;
}

function updateLicenseFromCloud(db: DB, state: CloudAuthState) {
  if (state.collectionAllowed) {
    setLicenseState(db, {
      status: "active",
      subscriptionStatus: "active",
      validatedAt: state.lastCheckedAt ?? new Date().toISOString(),
      expiresAt: state.effectiveUntil ?? null,
      lastError: null,
      lastErrorCode: null,
    });
  } else {
    setLicenseState(db, {
      status: "revoked",
      subscriptionStatus: "inactive",
      validatedAt: state.lastCheckedAt ?? new Date().toISOString(),
      lastError: state.reason ?? "license_or_auth_invalid",
      lastErrorCode: state.reason ?? "license_or_auth_invalid",
      expiresAt: state.effectiveUntil ?? null,
    });
  }
}

export function startCollectorCloudBridge(cfg: AppConfig, db: DB) {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) {
    return;
  }

  const heartbeatBase = Math.max(5000, cfg.xmonHeartbeatMs ?? 15000);
  const configRefreshMs = Math.max(15000, cfg.xmonConfigRefreshMs ?? 60000);
  const backoff: BackoffState = { attempts: 0 };
  let stopped = false;
  startTelemetryQueue(() => cfg);
  let lastAuthState: { authorized?: boolean; collectionAllowed?: boolean } = {};

  // Send initial device snapshots once on start
  try {
    const devices = listDevices(db);
    const profiles = listPollProfiles(db).filter((p) => p.kind?.toLowerCase?.() === "snmp");
    const pollTargets = listPollTargets(db);
    const targetsByProfile = new Map<number, typeof pollTargets>();
    const targetsByDevice = new Map<number, typeof pollTargets>();
    pollTargets.forEach((t) => {
      if (!targetsByProfile.has(t.profileId)) targetsByProfile.set(t.profileId, []);
      targetsByProfile.get(t.profileId)!.push(t);
      if (!targetsByDevice.has(t.deviceId)) targetsByDevice.set(t.deviceId, []);
      targetsByDevice.get(t.deviceId)!.push(t);
    });
    devices.forEach((d) => {
      const ts = new Date().toISOString();
      const deviceTargets = targetsByDevice.get(d.id) ?? [];
      const snmpProfileId =
        deviceTargets.find((t) => profiles.some((p) => p.id === t.profileId))?.profileId ??
        (deviceTargets.length ? deviceTargets[0].profileId : undefined);
      const snmpPollerIds =
        snmpProfileId !== undefined && snmpProfileId !== null
          ? [`poller-${snmpProfileId}`]
          : deviceTargets.map((t) => `poller-${t.profileId}`);
      enqueueDeviceSnapshot({
        deviceId: String(d.id),
        name: d.hostname,
        ip: d.ipAddress,
        deviceType: d.type ?? undefined,
        status: d.enabled ? "unknown" : "down",
        snmpProfileId: snmpProfileId !== undefined ? String(snmpProfileId) : null,
        snmpPollerIds: snmpPollerIds.length ? snmpPollerIds.map(String) : null,
        successCount: 0,
        failureCount: 0,
        ts,
      });

      // Seed device_state with last known poll health so xMon starts with real status
      const health = getDevicePollHealth(db, d.id);
      // Derive status from latest known outcome, falling back to currentStatus
      const lastSuccess = health.lastSuccessAt ? new Date(health.lastSuccessAt) : null;
      const lastFailure = health.lastFailureAt ? new Date(health.lastFailureAt) : null;
      let status: "up" | "down" | "unknown" = "unknown";
      if (lastSuccess && (!lastFailure || lastSuccess > lastFailure)) status = "up";
      else if (lastFailure && (!lastSuccess || lastFailure > lastSuccess)) status = "down";
      else if (health.currentStatus === "completed") status = "up";
      else if (health.currentStatus === "failed") status = "down";
      enqueueDeviceState({
        deviceId: String(d.id),
        status,
        successCountDelta: 0,
        failureCountDelta: 0,
        ts,
        lastPollAt: health.lastPollAt ?? undefined,
        lastSuccessAt: health.lastSuccessAt ?? undefined,
        lastFailureAt: health.lastFailureAt ?? undefined,
        lastError: health.lastError ?? null,
      });
    });
    profiles.forEach((p) => {
      const cfg = p.config ?? {};
      const version: "v2c" | "v3" =
        typeof cfg.version === "string" && cfg.version.toLowerCase() === "v3" ? "v3" : "v2c";
      enqueueSnmpProfileSnapshot({
        profileId: String(p.id),
        name: p.name,
        version,
        community: cfg.community,
        username: cfg.username,
      });
      const targetsFromCfg = Array.isArray(cfg.targets)
        ? cfg.targets
            .map((t: any) => ({
              oid: typeof t?.oid === "string" ? t.oid : null,
              label: typeof t?.label === "string" ? t.label : undefined,
            }))
            .filter((t: any) => t.oid)
        : [];
      const targetsFromDb =
        targetsByProfile.get(p.id)?.map((t) => ({
          oid: "1.3.6.1.2.1.1.3.0",
          label: `profile-${p.id}-target-${t.id}`,
        })) ?? [];
      const targets = targetsFromCfg.length ? targetsFromCfg : targetsFromDb;
      if (targets.length) {
        enqueueSnmpPollerSnapshot({
          pollerId: `poller-${p.id}`,
          name: p.name,
          description: typeof cfg.description === "string" ? cfg.description : undefined,
          targets: targets as Array<{ oid: string; label?: string }>,
          intervalSecs: typeof p.intervalSec === "number" ? p.intervalSec : cfg.intervalSecs ?? 60,
        });
      }
    });
  } catch {
    /* ignore */
  }

  function resolveCloudCfg() {
    const saved = getAllAppConfig(db);
    return {
      ...cfg,
      xmonApiBase: saved["XMON_API_BASE"] || cfg.xmonApiBase,
      xmonCollectorId: saved["XMON_COLLECTOR_ID"] || cfg.xmonCollectorId,
      xmonApiKey: saved["XMON_API_KEY"] || cfg.xmonApiKey,
    };
  }

  const stopRpcSession = startCollectorRpcSession(db, resolveCloudCfg);

  async function heartbeatLoop() {
    while (!stopped) {
      let retryAfterSec: number | undefined;
      try {
        const result = await sendPing(resolveCloudCfg());
        retryAfterSec = result.retryAfterSec;
        const { ok, state } = result;
        updateLicenseFromCloud(db, state);
        if (lastAuthState.authorized !== state.authorized || lastAuthState.collectionAllowed !== state.collectionAllowed) {
          enqueueTelemetry({
            messageId: `auth-${Date.now()}`,
            kind: "event",
            ts: new Date().toISOString(),
            payload: {
              type: "auth_state_changed",
              authorized: state.authorized,
              collectionAllowed: state.collectionAllowed,
              reason: state.reason,
            },
          });
          lastAuthState = { authorized: state.authorized, collectionAllowed: state.collectionAllowed };
        }
        setCloudState({
          enabled: true,
          status: ok && state.collectionAllowed ? "connected" : "blocked",
          lastCheckAt: state.lastCheckedAt ?? new Date().toISOString(),
        });
        backoff.attempts = ok ? 0 : backoff.attempts + 1;
      } catch {
        backoff.attempts += 1;
        setCloudState({ enabled: true, status: "error", lastCheckAt: new Date().toISOString() });
      }
      const delay = typeof retryAfterSec === "number" && retryAfterSec > 0 ? retryAfterSec * 1000 : backoffDelay(backoff.attempts, heartbeatBase, 300_000);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  async function configLoop() {
    while (!stopped) {
      let retryAfterSec: number | undefined;
      try {
        const res = await fetchCollectorConfig(resolveCloudCfg());
        retryAfterSec = res.retryAfterSec;
        if (db) {
          // surface drift visibility: mark last fetch even if not applied
          const nowIso = new Date().toISOString();
          updateCloudSyncState(db, {
            enabled: true,
            status: res.config ? "fetched_not_applied" : "fetched_empty",
            lastSyncAt: nowIso,
            cloudEndpoint: resolveCloudCfg().xmonApiBase,
          });
          if (res.config) {
            upsertAppConfigEntries(db, {
              XMON_LAST_CONFIG: JSON.stringify(res.config),
              XMON_LAST_CONFIG_AT: nowIso,
            });
          }
        }
      } catch {
        // ignore
      }
      const delay = typeof retryAfterSec === "number" && retryAfterSec > 0 ? retryAfterSec * 1000 : configRefreshMs;
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  heartbeatLoop();
  configLoop();

  return () => {
    stopped = true;
    stopRpcSession();
  };
}
