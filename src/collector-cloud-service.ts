import { setCloudState } from "./runtime-state.js";
import type { AppConfig } from "./config.js";
import type { DB } from "./db.js";
import { setLicenseState } from "./db.js";
import { sendPing, fetchCollectorConfig, type CloudAuthState } from "./xmon-client.js";
import { enqueueTelemetry, startTelemetryQueue } from "./telemetry-queue.js";
import { listDevices } from "./db.js";
import { enqueueDeviceSnapshot } from "./telemetry-queue.js";

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
  startTelemetryQueue(cfg);
  let lastAuthState: { authorized?: boolean; collectionAllowed?: boolean } = {};

  // Send initial device snapshots once on start
  try {
    const devices = listDevices(db);
    devices.forEach((d) =>
      enqueueDeviceSnapshot({
        deviceId: String(d.id),
        name: d.hostname,
        deviceType: d.type ?? undefined,
        status: d.enabled ? "unknown" : "down",
        ts: new Date().toISOString(),
      })
    );
  } catch {
    /* ignore */
  }

  async function heartbeatLoop() {
    while (!stopped) {
      let retryAfterSec: number | undefined;
      try {
        const result = await sendPing(cfg);
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
      const delay =
        typeof retryAfterSec === "number" && retryAfterSec > 0
          ? retryAfterSec * 1000
          : backoffDelay(backoff.attempts, heartbeatBase, 300_000);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  async function configLoop() {
    while (!stopped) {
      let retryAfterSec: number | undefined;
      try {
        const res = await fetchCollectorConfig(cfg);
        retryAfterSec = res.retryAfterSec;
      } catch {
        // ignore
      }
      const delay =
        typeof retryAfterSec === "number" && retryAfterSec > 0 ? retryAfterSec * 1000 : configRefreshMs;
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  heartbeatLoop();
  configLoop();

  return () => {
    stopped = true;
  };
}
