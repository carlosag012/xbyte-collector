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
      snmpPollerIds: item.snmpPollerIds ?? null,
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
