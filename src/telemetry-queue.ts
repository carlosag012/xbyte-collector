import { sendTelemetry } from "./xmon-client.js";
import type { AppConfig } from "./config.js";

type TelemetryItem = {
  messageId: string;
  kind: "heartbeat" | "metric" | "event";
  ts: string;
  payload: unknown;
};

const queue: TelemetryItem[] = [];
let flushing = false;
let retryUntil: number | null = null;

const MAX_BATCH = 25;
const FLUSH_MS = 4000;
const MAX_QUEUE = 200;

export function enqueueTelemetry(item: TelemetryItem) {
  if (queue.length >= MAX_QUEUE) queue.shift();
  queue.push(item);
}

export function enqueueDeviceSnapshot(item: {
  deviceId: string;
  name?: string;
  deviceType?: string;
  status?: "up" | "down" | "unknown";
  snmpProfileId?: string | null;
  snmpPollerIds?: string[] | null;
  successCount?: number;
  failureCount?: number;
  ts?: string | Date;
}) {
  enqueueTelemetry({
    messageId: `device-${item.deviceId}-${Date.now()}`,
    kind: "event",
    ts: item.ts ? new Date(item.ts).toISOString() : new Date().toISOString(),
    payload: {
      type: "device_snapshot",
      deviceId: item.deviceId,
      name: item.name,
      deviceType: item.deviceType,
      status: item.status,
      snmpProfileId: item.snmpProfileId ?? null,
      snmpPollerIds: item.snmpPollerIds,
      successCount: item.successCount,
      failureCount: item.failureCount,
    },
  });
}

export function enqueueDeviceState(item: {
  deviceId: string;
  status: "up" | "down" | "unknown";
  successCountDelta: number;
  failureCountDelta: number;
  ts?: string | Date;
  lastSuccessAt?: string | Date;
  lastFailureAt?: string | Date;
  lastPollAt?: string | Date;
  lastError?: string | null;
  latencyMs?: number | null;
}) {
  enqueueTelemetry({
    messageId: `device-state-${item.deviceId}-${Date.now()}`,
    kind: "event",
    ts: item.ts ? new Date(item.ts).toISOString() : new Date().toISOString(),
    payload: {
      type: "device_state",
      deviceId: item.deviceId,
      status: item.status,
      successCountDelta: item.successCountDelta,
      failureCountDelta: item.failureCountDelta,
      lastSuccessAt: item.lastSuccessAt ? new Date(item.lastSuccessAt).toISOString() : undefined,
      lastFailureAt: item.lastFailureAt ? new Date(item.lastFailureAt).toISOString() : undefined,
      lastPollAt: item.lastPollAt ? new Date(item.lastPollAt).toISOString() : item.ts ? new Date(item.ts).toISOString() : undefined,
      lastError: item.lastError ?? undefined,
      latencyMs: item.latencyMs ?? undefined,
      lastPingAt: item.ts ? new Date(item.ts).toISOString() : undefined,
    },
  });
}

export function enqueueSnmpProfileSnapshot(item: {
  profileId: string;
  name: string;
  version: "v2c" | "v3";
  community?: string;
  username?: string;
  ts?: string | Date;
}) {
  enqueueTelemetry({
    messageId: `snmp-profile-${item.profileId}-${Date.now()}`,
    kind: "event",
    ts: item.ts ? new Date(item.ts).toISOString() : new Date().toISOString(),
    payload: {
      type: "snmp_profile_snapshot",
      profileId: item.profileId,
      name: item.name,
      version: item.version,
      community: item.community,
      username: item.username,
    },
  });
}

export function enqueueSnmpPollerSnapshot(item: {
  pollerId: string;
  name: string;
  description?: string;
  targets: Array<{ oid: string; label?: string }>;
  intervalSecs: number;
  ts?: string | Date;
}) {
  enqueueTelemetry({
    messageId: `snmp-poller-${item.pollerId}-${Date.now()}`,
    kind: "event",
    ts: item.ts ? new Date(item.ts).toISOString() : new Date().toISOString(),
    payload: {
      type: "snmp_poller_snapshot",
      pollerId: item.pollerId,
      name: item.name,
      description: item.description,
      targets: item.targets,
      intervalSecs: item.intervalSecs,
    },
  });
}

export function enqueueLldpNeighbors(item: {
  deviceId: string;
  neighbors: Array<{
    localPort?: string | null;
    remoteSysName?: string | null;
    remotePortId?: string | null;
    remotePortDesc?: string | null;
    remoteChassisId?: string | null;
    remoteMgmtIp?: string | null;
  }>;
  collectedAt?: string | Date;
}) {
  enqueueTelemetry({
    messageId: `lldp-${item.deviceId}-${Date.now()}`,
    kind: "event",
    ts: item.collectedAt ? new Date(item.collectedAt).toISOString() : new Date().toISOString(),
    payload: {
      type: "lldp_neighbors",
      deviceId: item.deviceId,
      collectedAt: item.collectedAt ? new Date(item.collectedAt).toISOString() : undefined,
      neighbors: item.neighbors,
    },
  });
}

export function enqueueInterfaceSnapshot(item: {
  deviceId: string;
  interfaces: Array<{
    ifIndex?: number | null;
    ifName?: string | null;
    ifDescr?: string | null;
    ifAlias?: string | null;
    ifAdminStatus?: string | null;
    ifOperStatus?: string | null;
    ifSpeed?: number | null;
    ifHighSpeed?: number | null;
    mtu?: number | null;
    mac?: string | null;
    collectedAt?: string | null;
  }>;
  collectedAt?: string | Date;
}) {
  enqueueTelemetry({
    messageId: `iface-${item.deviceId}-${Date.now()}`,
    kind: "event",
    ts: item.collectedAt ? new Date(item.collectedAt).toISOString() : new Date().toISOString(),
    payload: {
      type: "interface_snapshot",
      deviceId: item.deviceId,
      collectedAt: item.collectedAt ? new Date(item.collectedAt).toISOString() : undefined,
      interfaces: item.interfaces,
    },
  });
}

export function enqueueSnmpSystemSnapshot(item: {
  deviceId: string;
  sysName?: string | null;
  sysDescr?: string | null;
  sysLocation?: string | null;
  sysContact?: string | null;
  sysUptime?: number | null;
  collectedAt?: string | Date;
}) {
  enqueueTelemetry({
    messageId: `sys-${item.deviceId}-${Date.now()}`,
    kind: "event",
    ts: item.collectedAt ? new Date(item.collectedAt).toISOString() : new Date().toISOString(),
    payload: {
      type: "snmp_system_snapshot",
      deviceId: item.deviceId,
      sysName: item.sysName,
      sysDescr: item.sysDescr,
      sysLocation: item.sysLocation,
      sysContact: item.sysContact,
      sysUptime: item.sysUptime,
      collectedAt: item.collectedAt ? new Date(item.collectedAt).toISOString() : undefined,
    },
  });
}

export function startTelemetryQueue(resolveCfg: () => AppConfig) {
  setInterval(async () => {
    if (flushing) return;
    if (retryUntil && Date.now() < retryUntil) return;
    if (!queue.length) return;
    flushing = true;
    try {
      const batch = queue.splice(0, MAX_BATCH);
      const cfg = resolveCfg();
      if (!cfg.xmonApiBase || !cfg.xmonCollectorId || !cfg.xmonApiKey) {
        console.error(
          JSON.stringify({
            level: "warn",
            msg: "telemetry_send_skipped_missing_config",
            missingApiBase: !cfg.xmonApiBase,
            missingCollectorId: !cfg.xmonCollectorId,
            missingApiKey: !cfg.xmonApiKey,
            batchSize: batch.length,
          }),
        );
        // requeue batch to try again later
        queue.unshift(...batch);
        flushing = false;
        return;
      }

      const res = await sendTelemetry(cfg, batch);
      if (!res.ok && res.retryAfterSec) {
        retryUntil = Date.now() + res.retryAfterSec * 1000;
        console.error(JSON.stringify({ level: "warn", msg: "telemetry_rate_limited", retryAfterSec: res.retryAfterSec, batchSize: batch.length }));
      } else {
        retryUntil = null;
      }
      if (!res.ok && !res.retryAfterSec) {
        // push back batch to retry once later; keep bounded
        queue.unshift(...batch);
        if (queue.length > MAX_QUEUE) queue.splice(0, queue.length - MAX_QUEUE);
        console.error(JSON.stringify({ level: "warn", msg: "telemetry_send_failed", batchSize: batch.length, queued: queue.length }));
      } else if (res.ok) {
        const firstPayload: any = batch[0]?.payload;
        console.log(
          JSON.stringify({
            level: "info",
            msg: "telemetry_send_ok",
            batchSize: batch.length,
            firstType: firstPayload?.type ?? null,
            collectorIdPresent: !!cfg.xmonCollectorId,
          }),
        );
      }
    } catch (err: any) {
      console.error(JSON.stringify({ level: "error", msg: "telemetry_flush_exception", err: err?.message ?? String(err) }));
    } finally {
      flushing = false;
    }
  }, FLUSH_MS);
}
