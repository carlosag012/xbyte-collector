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

export function startTelemetryQueue(cfg: AppConfig) {
  setInterval(async () => {
    if (flushing) return;
    if (retryUntil && Date.now() < retryUntil) return;
    if (!queue.length) return;
    flushing = true;
    try {
      const batch = queue.splice(0, MAX_BATCH);
      const res = await sendTelemetry(cfg, batch);
      if (!res.ok && res.retryAfterSec) {
        retryUntil = Date.now() + res.retryAfterSec * 1000;
      } else {
        retryUntil = null;
      }
      if (!res.ok && !res.retryAfterSec) {
        // push back batch to retry once later; keep bounded
        queue.unshift(...batch);
        if (queue.length > MAX_QUEUE) queue.splice(0, queue.length - MAX_QUEUE);
      }
    } finally {
      flushing = false;
    }
  }, FLUSH_MS);
}
