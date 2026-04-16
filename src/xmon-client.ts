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
    return { config: body?.config ?? null };
  } catch {
    return { config: null };
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
