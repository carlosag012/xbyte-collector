import type { AppConfig } from "./config.js";

type PingResponse = {
  ok: boolean;
  authorized?: boolean;
  collectionAllowed?: boolean;
  licenseStatus?: string | null;
  effectiveUntil?: string | null;
  reason?: string | null;
};

type ConfigResponse = {
  ok: boolean;
  config?: any;
};

export type CollectorUplinkFiberConfigRow = {
  deviceId: string;
  stableInterfaceKey: string;
  localIfIndex?: number | null;
  localPortNormalized?: string | null;
  localPortDisplay?: string | null;
  cableCount?: string | null;
  bufferColor?: string | null;
  txStrandColor?: string | null;
  rxStrandColor?: string | null;
  jumperMode?: string | null;
  connectorType?: string | null;
  patchPanelPorts?: string | null;
  sfpDetected?: string | null;
  sfpPartNumber?: string | null;
  rxLight?: string | null;
  txLight?: string | null;
  updatedAt?: string | null;
};

export type CollectorUplinkFiberConfigSnapshot = {
  collectorId: string;
  deviceIds: string[];
  rows: CollectorUplinkFiberConfigRow[];
};

export type CloudAuthState = {
  authorized: boolean;
  collectionAllowed: boolean;
  licenseStatus: string | null;
  effectiveUntil: string | null;
  reason: string | null;
  lastCheckedAt: string | null;
};

export type ApplianceActivateResponse = {
  ok: boolean;
  orgId?: string;
  collectorId?: string;
  apiKey?: string | null;
  apiKeyIssued?: boolean;
  apiBase?: string | null;
  authorized?: boolean;
  collectionAllowed?: boolean;
  licenseStatus?: string | null;
  effectiveUntil?: string | null;
  reason?: string | null;
  collectorLimit?: number | null;
  activeCollectorCount?: number;
  error?: string;
};

async function doFetch(url: string, opts: RequestInit) {
  const res = await fetch(url, {
    ...opts,
    headers: {
      "content-type": "application/json",
      ...(opts.headers || {}),
    },
  });
  return res;
}

export async function sendPing(cfg: AppConfig): Promise<{ state: CloudAuthState; ok: boolean; retryAfterSec?: number }> {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) {
    return {
      ok: false,
      state: {
        authorized: false,
        collectionAllowed: false,
        licenseStatus: "unconfigured",
        effectiveUntil: null,
        reason: "collector_id_or_api_key_missing",
        lastCheckedAt: new Date().toISOString(),
      },
    };
  }

  try {
    const res = await doFetch(`${cfg.xmonApiBase}/collectors/${encodeURIComponent(cfg.xmonCollectorId)}/ping`, {
      method: "POST",
      body: JSON.stringify({ ts: new Date().toISOString() }),
      headers: { "x-xmon-api-key": cfg.xmonApiKey },
    });

    if (res.status === 429) {
      const retryAfter = Number(res.headers.get("retry-after")) || undefined;
      return {
        ok: false,
        retryAfterSec: retryAfter,
        state: {
          authorized: false,
          collectionAllowed: false,
          licenseStatus: "rate_limited",
          effectiveUntil: null,
          reason: "rate_limited",
          lastCheckedAt: new Date().toISOString(),
        },
      };
    }

    if (!res.ok) {
      return {
        ok: false,
        state: {
          authorized: false,
          collectionAllowed: false,
          licenseStatus: "unauthorized",
          effectiveUntil: null,
          reason: `http_${res.status}`,
          lastCheckedAt: new Date().toISOString(),
        },
      };
    }

    const body = (await res.json().catch(() => ({}))) as PingResponse;
    const authorized = body.authorized !== false;
    const collectionAllowed = body.collectionAllowed !== false && authorized;

    return {
      ok: true,
      state: {
        authorized,
        collectionAllowed,
        licenseStatus: body.licenseStatus ?? null,
        effectiveUntil: body.effectiveUntil ?? null,
        reason: body.reason ?? null,
        lastCheckedAt: new Date().toISOString(),
      },
    };
  } catch (error) {
    const code = (error as any)?.code || (error as any)?.cause?.code;
    const message = error instanceof Error ? error.message : undefined;
    const reason = (code || message || "network_error") as string;
    return {
      ok: false,
      state: {
        authorized: false,
        collectionAllowed: false,
        licenseStatus: "unreachable",
        effectiveUntil: null,
        reason,
        lastCheckedAt: new Date().toISOString(),
      },
    };
  }
}

export async function activateAppliance(apiBase: string, payload: { licenseKey: string; hostname?: string; fingerprint?: string; applianceName?: string }) {
  const res = await doFetch(`${apiBase.replace(/\/+$/, "")}/appliance/activate`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
  const body = (await res.json().catch(() => ({}))) as ApplianceActivateResponse;
  return { status: res.status, body };
}

export async function fetchCollectorConfig(cfg: AppConfig): Promise<{ config: any | null; retryAfterSec?: number }> {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) return { config: null };
  try {
    const res = await doFetch(`${cfg.xmonApiBase}/collectors/${encodeURIComponent(cfg.xmonCollectorId)}/config`, {
      method: "GET",
      headers: { "x-xmon-api-key": cfg.xmonApiKey },
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get("retry-after")) || undefined;
      return { config: null, retryAfterSec: retryAfter };
    }
    if (!res.ok) return { config: null };
    const body = (await res.json().catch(() => ({}))) as ConfigResponse;
    const candidate =
      body && typeof body === "object" && "config" in body ? body?.config ?? null : body;
    return { config: candidate && typeof candidate === "object" ? candidate : null };
  } catch {
    return { config: null };
  }
}

export async function fetchCollectorUplinkFiberConfig(
  cfg: AppConfig,
): Promise<{ snapshot: CollectorUplinkFiberConfigSnapshot | null; retryAfterSec?: number }> {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) return { snapshot: null };
  try {
    const res = await doFetch(`${cfg.xmonApiBase}/collectors/${encodeURIComponent(cfg.xmonCollectorId)}/uplink-fiber-config`, {
      method: "GET",
      headers: { "x-xmon-api-key": cfg.xmonApiKey },
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get("retry-after")) || undefined;
      return { snapshot: null, retryAfterSec: retryAfter };
    }
    if (!res.ok) return { snapshot: null };
    const body = (await res.json().catch(() => null)) as any;
    if (!body || typeof body !== "object") return { snapshot: null };

    const collectorId = String(body.collectorId ?? "").trim();
    if (!collectorId) return { snapshot: null };

    const deviceIds = Array.isArray(body.deviceIds)
      ? body.deviceIds.map((entry: unknown) => String(entry ?? "").trim()).filter((entry: string) => entry.length > 0)
      : [];
    const rows = Array.isArray(body.rows) ? body.rows : [];
    const parsedRows: CollectorUplinkFiberConfigRow[] = [];
    for (const row of rows) {
      if (!row || typeof row !== "object") continue;
      const raw = row as Record<string, unknown>;
      const deviceId = String(raw.deviceId ?? "").trim();
      const stableInterfaceKey = String(raw.stableInterfaceKey ?? "").trim();
      if (!deviceId || !stableInterfaceKey) continue;
      const localIfIndexValue = raw.localIfIndex;
      const localIfIndexNumber =
        localIfIndexValue === null || localIfIndexValue === undefined ? null : Number(localIfIndexValue);
      parsedRows.push({
        deviceId,
        stableInterfaceKey,
        localIfIndex: Number.isInteger(localIfIndexNumber) ? localIfIndexNumber : null,
        localPortNormalized: raw.localPortNormalized == null ? null : String(raw.localPortNormalized).trim() || null,
        localPortDisplay: raw.localPortDisplay == null ? null : String(raw.localPortDisplay).trim() || null,
        cableCount: raw.cableCount == null ? null : String(raw.cableCount).trim() || null,
        bufferColor: raw.bufferColor == null ? null : String(raw.bufferColor).trim() || null,
        txStrandColor: raw.txStrandColor == null ? null : String(raw.txStrandColor).trim() || null,
        rxStrandColor: raw.rxStrandColor == null ? null : String(raw.rxStrandColor).trim() || null,
        jumperMode: raw.jumperMode == null ? null : String(raw.jumperMode).trim() || null,
        connectorType: raw.connectorType == null ? null : String(raw.connectorType).trim() || null,
        patchPanelPorts: raw.patchPanelPorts == null ? null : String(raw.patchPanelPorts).trim() || null,
        sfpDetected: raw.sfpDetected == null ? null : String(raw.sfpDetected).trim() || null,
        sfpPartNumber: raw.sfpPartNumber == null ? null : String(raw.sfpPartNumber).trim() || null,
        rxLight: raw.rxLight == null ? null : String(raw.rxLight).trim() || null,
        txLight: raw.txLight == null ? null : String(raw.txLight).trim() || null,
        updatedAt: raw.updatedAt == null ? null : String(raw.updatedAt).trim() || null,
      });
    }

    return {
      snapshot: {
        collectorId,
        deviceIds,
        rows: parsedRows,
      },
    };
  } catch {
    return { snapshot: null };
  }
}

export async function sendTelemetry(cfg: AppConfig, payload: unknown): Promise<{ ok: boolean; retryAfterSec?: number }> {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) return { ok: false };
  try {
    const res = await doFetch(`${cfg.xmonApiBase}/collectors/${encodeURIComponent(cfg.xmonCollectorId)}/telemetry`, {
      method: "POST",
      headers: { "x-xmon-api-key": cfg.xmonApiKey },
      body: JSON.stringify(payload ?? {}),
    });
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get("retry-after")) || undefined;
      return { ok: false, retryAfterSec: retryAfter };
    }
    return { ok: res.ok };
  } catch {
    return { ok: false };
  }
}

export async function publishAvailabilityUpdate(
  cfg: AppConfig,
  input: {
    deviceId: string;
    updatedAt: string;
    revision?: string | null;
  },
): Promise<{ ok: boolean; accepted?: boolean; reason?: string }> {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) return { ok: false, reason: "collector_id_or_api_key_missing" };
  try {
    const res = await doFetch(`${cfg.xmonApiBase}/collectors/${encodeURIComponent(cfg.xmonCollectorId)}/session/publish`, {
      method: "POST",
      headers: { "x-xmon-api-key": cfg.xmonApiKey },
      body: JSON.stringify({
        event: {
          type: "availability_updated",
          deviceId: input.deviceId,
          updatedAt: input.updatedAt,
          ...(input.revision ? { revision: input.revision } : {}),
        },
      }),
    });

    const body = (await res.json().catch(() => ({}))) as { accepted?: boolean; reason?: string };
    if (!res.ok) return { ok: false, accepted: body?.accepted, reason: body?.reason ?? `http_${res.status}` };
    return { ok: true, accepted: body?.accepted, reason: body?.reason };
  } catch (err: any) {
    const code = (err as any)?.code || (err as any)?.cause?.code;
    return { ok: false, reason: (code || "network_error") as string };
  }
}

export async function publishDeviceIdentityUpdate(
  cfg: AppConfig,
  input: {
    deviceId: string;
    updatedAt: string;
    assetTag?: string | null;
    serialNumber?: string | null;
  },
): Promise<{ ok: boolean; accepted?: boolean; reason?: string }> {
  if (!cfg.xmonCollectorId || !cfg.xmonApiKey) return { ok: false, reason: "collector_id_or_api_key_missing" };
  try {
    const res = await doFetch(`${cfg.xmonApiBase}/collectors/${encodeURIComponent(cfg.xmonCollectorId)}/session/publish`, {
      method: "POST",
      headers: { "x-xmon-api-key": cfg.xmonApiKey },
      body: JSON.stringify({
        event: {
          type: "device_identity_updated",
          deviceId: input.deviceId,
          updatedAt: input.updatedAt,
          assetTag: input.assetTag ?? null,
          serialNumber: input.serialNumber ?? null,
        },
      }),
    });

    const body = (await res.json().catch(() => ({}))) as { accepted?: boolean; reason?: string };
    if (!res.ok) return { ok: false, accepted: body?.accepted, reason: body?.reason ?? `http_${res.status}` };
    return { ok: true, accepted: body?.accepted, reason: body?.reason };
  } catch (err: any) {
    const code = (err as any)?.code || (err as any)?.cause?.code;
    return { ok: false, reason: (code || "network_error") as string };
  }
}
