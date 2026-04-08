import { createServer } from "node:http";
import { readFileSync, existsSync, statSync } from "node:fs";
import { resolve, join, normalize } from "node:path";
import os from "node:os";
import { loadConfig } from "./config.js";
import { runtimeState, setBootstrapState, setCloudState, setRegisteredWorkers } from "./runtime-state.js";
import { parseNonNegativeInteger } from "./http-utils.js";
import { versionInfo } from "./version.js";
// readJsonBody is ready for future POST endpoints that accept JSON payloads.
import { readJsonBody } from "./body-utils.js";
import { configState, updateConfig } from "./config-state.js";
import {
  initDatabase,
  getAllAppConfig,
  upsertAppConfigEntries,
  getBootstrapState,
  setBootstrapStateRow,
  createInitialAdminIfMissing,
  createDevice,
  updateDevice,
  listDevices,
  createPollProfile,
  updatePollProfile,
  listPollProfiles,
  listWorkerRegistrations,
  upsertWorkerRegistration,
  heartbeatWorkerRegistration,
  getWorkerRegistrationByName,
  updateWorkerRegistrationEnabled,
  getWorkerMetricsSnapshot,
  getWorkerMetricsSummaryByType,
  getWorkerExecutionSummary,
  listPollTargets,
  listEnabledPollTargets,
  createPollTarget,
  updatePollTarget,
  getPollTargetById,
  listPollJobs,
  claimNextPendingPollJob,
  getPollJobById,
  retryPollJob,
  finishPollJob,
  releasePollJob,
  heartbeatPollJob,
  abandonPollJob,
  unclaimPollJobById,
  listStaleRunningPollJobs,
  requeueStaleRunningPollJobs,
  requeueStaleRunningPollJobsForWorker,
  abandonStaleRunningPollJobsForWorker,
  retryFailedPollJobsForWorker,
  listFailedPollJobsForWorker,
  listCompletedPollJobsForWorker,
  getPollJobSummaryForWorker,
  getPollJobSummary,
  getStalePollJobSummary,
  getDevicePollHealth,
  getPendingPollJobAvailabilityForWorkerCapabilities,
  claimNextPendingPollJobForWorker,
  claimNextPendingPollJobForWorkerCapabilities,
  claimPendingPollJobsBatch,
  listRunningPollJobsForWorker,
  listStaleRunningPollJobsForWorker,
  claimPollJobById,
  createPollJob,
  getDeviceById,
  getPollProfileById,
  getPollJobDetail,
  enqueuePollJobForTarget,
  enqueuePollJobsForTargets,
  getRunningPollJobDetailForLeaseOwner,
  getRunningPollJobForLeaseOwner,
  getSystemSnapshotsForDevice,
  listInterfaceSnapshotsForDevice,
  listLldpNeighborsForDevice,
  listLldpNeighbors,
  listNeighborsWithReview,
  getLldpNeighborById,
  setNeighborReview,
  logNeighborReviewEvent,
  listNeighborReviewEvents,
  logAdminAuditEvent,
  listAdminAuditEvents,
  findDeviceByIpOrHostname,
  findPollProfileByNameAndKind,
  findPollTargetByDeviceProfile,
  getLicenseState,
  setLicenseState,
  licenseAllowsCollection,
  evaluateLicenseState,
  listDiscoveredCandidatesForSourceDevice,
  getCompany,
  upsertCompany,
  getDeployment,
  upsertDeployment,
  getCloudSyncState,
  updateCloudSyncState,
  normalizeCompanySlug,
  isCompanySlugInUse,
  generateDeploymentId,
  listAgentEnrollments,
  createAgentEnrollment,
  generateEnrollmentId,
  generateEnrollmentToken,
  revokeAgentEnrollment,
  getAgentEnrollmentByEnrollmentId,
  getAgentEnrollmentByToken,
  touchAgentEnrollmentLastUsedAt,
  updateAgentEnrollmentStatus,
  updateUserPassword,
  type DB,
} from "./db.js";
import { hashPassword, verifyPassword } from "./passwords.js";
import { authenticateLocalUser } from "./auth-service.js";
import { issueSession, clearSession, readValidSession } from "./session-service.js";
import { getUserById } from "./db.js";
import { Logger } from "./logger.js";
import { startCollectorCloudBridge } from "./collector-cloud-service.js";
import { sendPing, activateAppliance } from "./xmon-client.js";
import { loadApplianceIdentity, buildApplianceSummary } from "./appliance-state.js";
import { startApplianceSync } from "./appliance-sync.js";

const config = loadConfig();
let cloudBridgeStarted = false;
let dbInitFailed = false;
let pollSchedulerHandle: NodeJS.Timeout | null = null;
let stopApplianceSync: (() => void) | null = null;

function resolveXmonConfig(db: DB, overrides?: { apiBase?: string; collectorId?: string; apiKey?: string }) {
  const saved = getAllAppConfig(db);
  const apiBase = overrides?.apiBase?.trim() || saved["XMON_API_BASE"] || config.xmonApiBase;
  const collectorId = overrides?.collectorId?.trim() || saved["XMON_COLLECTOR_ID"] || config.xmonCollectorId;
  const apiKey = overrides?.apiKey?.trim() || saved["XMON_API_KEY"] || config.xmonApiKey;
  return { apiBase, collectorId, apiKey };
}
let db: DB | null = null;
const distDir = resolve("web", "dist");
const distIndexPath = join(distDir, "index.html");
const logDir = join(process.cwd(), "var", "logs");
const logFile = join(logDir, "xbyte-collector.log");
const logger = new Logger(config.logLevel ?? "info", logDir, "xbyte-collector.log");

function startCloudBridgeIfReady(db: DB) {
  if (cloudBridgeStarted) return;
  const resolvedXmon = resolveXmonConfig(db);
  const bridgeConfig = {
    ...config,
    xmonApiBase: resolvedXmon.apiBase ?? config.xmonApiBase,
    xmonCollectorId: resolvedXmon.collectorId ?? config.xmonCollectorId,
    xmonApiKey: resolvedXmon.apiKey ?? config.xmonApiKey,
  };
  if (!bridgeConfig.xmonCollectorId || !bridgeConfig.xmonApiKey) return;
  startCollectorCloudBridge(bridgeConfig, db);
  cloudBridgeStarted = true;
}

function startPollScheduler(db: DB) {
  if (pollSchedulerHandle) return;
  const intervalMs = 30000; // minimal scheduler, 30s
  const FAILURE_COOLDOWN_MS = 5 * 60 * 1000; // 5 minutes to avoid hammering bad targets
  const RETAIN_POLL_JOBS = 10_000; // keep latest 10k jobs
  const PREFERRED_SNMP_SUCCESS_MS = 15 * 60 * 1000; // prefer a recently successful SNMP target for a device
  const LOSS_WINDOW_MS = 30 * 60 * 1000; // evaluate losing targets in this window
  const LOSS_FAILURE_THRESHOLD = 3; // failures needed to auto-suppress
  const GRACE_AFTER_MANUAL_ENABLE_MS = 10 * 60 * 1000; // grace after manual re-enable before auto-suppressing again
  const STALE_RUNNING_SEC = 10 * 60; // recover running jobs older than 10 minutes
  const PING_FAIL_SNMP_BACKOFF_MS = 24 * 60 * 60 * 1000; // when ping failing, only try SNMP once per day
  const hasActiveStmt = db.prepare(
    `SELECT COUNT(*) as cnt FROM poll_jobs WHERE target_id = ? AND status IN ('pending','running')`
  );
  const latestJobStmt = db.prepare(
    `SELECT status, finished_at as finishedAt, updated_at as updatedAt
     FROM poll_jobs
     WHERE target_id = ?
     ORDER BY updated_at DESC
     LIMIT 1`
  );
  const retentionCutoffStmt = db.prepare(
    `SELECT id FROM poll_jobs ORDER BY id DESC LIMIT 1 OFFSET ?`
  );
  const deleteOldStmt = db.prepare(
    `DELETE FROM poll_jobs WHERE id < ?`
  );
  const lastSuccessForTargetStmt = db.prepare(
    `SELECT finished_at as finishedAt, updated_at as updatedAt
     FROM poll_jobs
     WHERE target_id = ? AND status = 'completed'
     ORDER BY updated_at DESC
     LIMIT 1`
  );
  const successCountWindowStmt = db.prepare(
    `SELECT COUNT(*) as cnt FROM poll_jobs
     WHERE target_id = ? AND status = 'completed' AND updated_at > ?`
  );
  const failureCountWindowStmt = db.prepare(
    `SELECT COUNT(*) as cnt FROM poll_jobs
     WHERE target_id = ? AND status = 'failed' AND updated_at > ?`
  );
  pollSchedulerHandle = setInterval(() => {
    try {
      if (!licenseAllowsCollection(db).allowed) return;
      const enabledTargets = listEnabledPollTargets(db);
      const profiles = listPollProfiles(db);
      const profileKind = new Map<number, string>();
      profiles.forEach((p) => profileKind.set(p.id, p.kind));
      // Recover stale running jobs globally (lightweight)
      requeueStaleRunningPollJobs(db, STALE_RUNNING_SEC);
      // Latest ping result per device (authoritative availability)
      const latestPingRows = db
        .prepare(
          `SELECT pt.device_id as deviceId, pj.status, pj.updated_at as updatedAt
           FROM poll_jobs pj
           JOIN poll_targets pt ON pt.id = pj.target_id
           JOIN poll_profiles pp ON pp.id = pt.profile_id
           WHERE pp.kind = 'ping' AND pj.status IN ('completed','failed')
           ORDER BY pj.updated_at DESC`
        )
        .all() as Array<{ deviceId: number; status: string; updatedAt: string }>;
      const latestPingByDevice = new Map<number, { status: string; updatedMs: number }>();
      for (const row of latestPingRows) {
        if (latestPingByDevice.has(row.deviceId)) continue;
        const ms = new Date(row.updatedAt).getTime();
        latestPingByDevice.set(row.deviceId, { status: row.status, updatedMs: ms });
      }
      // Pick a preferred SNMP target per device based on recent success
      const preferredSnmpTargetByDevice = new Map<number, { targetId: number; tsMs: number }>();
      const nowMs = Date.now();
      for (const t of enabledTargets) {
        if (profileKind.get(t.profileId)?.toLowerCase() !== "snmp") continue;
        const lastSucc = lastSuccessForTargetStmt.get(t.id) as { finishedAt?: string | null; updatedAt?: string } | undefined;
        const tsStr = lastSucc?.finishedAt || lastSucc?.updatedAt;
        if (!tsStr) continue;
        const tsMs = new Date(tsStr).getTime();
        if (Number.isNaN(tsMs)) continue;
        if (nowMs - tsMs > PREFERRED_SNMP_SUCCESS_MS) continue;
        const existing = preferredSnmpTargetByDevice.get(t.deviceId);
        if (!existing || tsMs > existing.tsMs) {
          preferredSnmpTargetByDevice.set(t.deviceId, { targetId: t.id, tsMs });
        }
      }
      if (!enabledTargets.length) return;
      const nowMsLoop = nowMs;
      const lossWindowIso = new Date(nowMsLoop - LOSS_WINDOW_MS).toISOString();
      const deviceSnmpTargets = new Map<number, number[]>();
      for (const t of enabledTargets) {
        if (profileKind.get(t.profileId)?.toLowerCase() === "snmp") {
          if (!deviceSnmpTargets.has(t.deviceId)) deviceSnmpTargets.set(t.deviceId, []);
          deviceSnmpTargets.get(t.deviceId)!.push(t.id);
        }
      }
      for (const t of enabledTargets) {
        const kind = profileKind.get(t.profileId)?.toLowerCase();
        const preferred = preferredSnmpTargetByDevice.get(t.deviceId);
        let latestJob: { status?: string; finishedAt?: string | null; updatedAt?: string } | undefined;
        const getLatestJob = () => {
          if (latestJob !== undefined) return latestJob;
          latestJob = latestJobStmt.get(t.id) as { status?: string; finishedAt?: string | null; updatedAt?: string } | undefined;
          return latestJob;
        };
        if (kind === "snmp" && preferred && preferred.targetId !== t.id) {
          // Auto-suppress persistently losing alternates when another target is succeeding
          const updatedMs = new Date(t.updatedAt).getTime();
          if (!Number.isNaN(updatedMs) && nowMsLoop - updatedMs < GRACE_AFTER_MANUAL_ENABLE_MS) {
            // Skip suppression during grace after manual enable
            continue;
          }
          const siblings = deviceSnmpTargets.get(t.deviceId) ?? [];
          if (siblings.length > 1) {
            const succRow = successCountWindowStmt.get(t.id, lossWindowIso) as { cnt: number } | undefined;
            const failRow = failureCountWindowStmt.get(t.id, lossWindowIso) as { cnt: number } | undefined;
            const succCnt = succRow?.cnt ?? 0;
            const failCnt = failRow?.cnt ?? 0;
            if (succCnt === 0 && failCnt >= LOSS_FAILURE_THRESHOLD) {
              const before = getPollTargetById(db, t.id);
              updatePollTarget(db, { id: t.id, enabled: false });
              const after = getPollTargetById(db, t.id);
              logAdminAuditEvent(db, {
                entityType: "target",
                entityId: t.id,
                action: "auto_suppress",
                note: `failed ${failCnt} in ${LOSS_WINDOW_MS / 60000}m while another target succeeds`,
                before,
                after,
              });
              logger.warn("snmp target auto-suppressed", {
                targetId: t.id,
                deviceId: t.deviceId,
                failures: failCnt,
                windowMs: LOSS_WINDOW_MS,
              });
              continue;
            }
          }
          // Skip alternate SNMP targets while a recent successful one exists
          continue;
        }
        const row = hasActiveStmt.get(t.id) as { cnt: number };
        if (row?.cnt && row.cnt > 0) continue;
        const latest = getLatestJob();
        // Ping-fail backoff for SNMP
        if (kind === "snmp") {
          const pingInfo = latestPingByDevice.get(t.deviceId);
          if (pingInfo && pingInfo.status === "failed") {
            const lastSnmpUpdated = latest?.updatedAt ? new Date(latest.updatedAt).getTime() : null;
            if (lastSnmpUpdated && nowMsLoop - lastSnmpUpdated < PING_FAIL_SNMP_BACKOFF_MS) {
              continue;
            }
          }
        }
        if (latest?.status === "failed") {
          const ts = latest.finishedAt || latest.updatedAt;
          if (ts) {
            const ageMs = nowMsLoop - new Date(ts).getTime();
            if (ageMs < FAILURE_COOLDOWN_MS) continue;
          }
        }
        enqueuePollJobForTarget(db, t.id);
      }
      // prune oldest jobs to keep table bounded
      const cutoff = retentionCutoffStmt.get(RETAIN_POLL_JOBS - 1) as { id?: number } | undefined;
      if (cutoff?.id) {
        deleteOldStmt.run(cutoff.id);
      }
    } catch (err: any) {
      // best-effort; do not crash server
    }
  }, intervalMs);
}

try {
  db = initDatabase(config);
  syncConfigFromDb(db);
  syncBootstrapFromDb(db);
  setRegisteredWorkers(listWorkerRegistrations(db).length);
  logger.info("server_start", { pid: process.pid });
  loadApplianceIdentity(db, configState.orgId);
  startCloudBridgeIfReady(db);
  stopApplianceSync = startApplianceSync(config, db);
  startPollScheduler(db);
} catch (err: any) {
  console.error(
    JSON.stringify({
      level: "fatal",
      msg: "sqlite init failed",
      error: err?.message ?? String(err),
      path: config.sqlitePath,
      time: new Date().toISOString(),
    })
  );
  logger.error("sqlite init failed", { error: err?.message ?? String(err), path: config.sqlitePath });
  dbInitFailed = true;
}

const server = createServer((req, res) => {
  const url = req.url ?? "/";
  const method = req.method ?? "GET";

  res.setHeader("Content-Type", "application/json; charset=utf-8");

  if (method === "GET" && url === "/api/health") {
    const body = {
      ok: true,
      service: "xbyte-collector",
      phase: "phase-1-foundation",
      time: new Date().toISOString(),
      uptimeSec: Math.round(process.uptime()),
      runtime: runtimeState,
    };
    res.writeHead(200);
    res.end(JSON.stringify(body));
    return;
  }

  if (method === "GET" && url === "/api/version") {
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        ...versionInfo,
      })
    );
    return;
  }

  if (method === "GET" && url === "/api/status") {
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        ...versionInfo,
        time: new Date().toISOString(),
        bootstrap: runtimeState.bootstrap,
        cloud: runtimeState.cloud,
        workers: runtimeState.workers,
        config: configState,
        appliance: db ? buildApplianceSummary(db) : null,
      })
    );
    return;
  }

  if (method === "GET" && url === "/api/bootstrap/status") {
    try {
      if (db) syncBootstrapFromDb(db);
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          bootstrap: runtimeState.bootstrap,
        })
      );
    } catch {
      res.writeHead(500);
      res.end(
        JSON.stringify({
          ok: false,
          error: "db_error",
        })
      );
    }
    return;
  }

  if (method === "GET" && url === "/api/appliance/summary") {
    try {
      if (!db) throw new Error("db missing");
      const summary = buildApplianceSummary(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, summary }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "summary_unavailable" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/bootstrap/mark-configured") {
    try {
      if (!db) throw new Error("db missing");
      setBootstrapStateRow(db, { configured: true, status: "configured" });
      syncBootstrapFromDb(db);
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          bootstrap: runtimeState.bootstrap,
        })
      );
    } catch {
      res.writeHead(500);
      res.end(
        JSON.stringify({
          ok: false,
          error: "db_error",
        })
      );
    }
    return;
  }

  if (method === "GET" && url === "/api/cloud/status") {
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        cloud: runtimeState.cloud,
      })
    );
    return;
  }

  if (method === "POST" && url === "/api/cloud/mark-connected") {
    setCloudState({ enabled: true, status: "connected", lastCheckAt: new Date().toISOString() });
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        cloud: runtimeState.cloud,
      })
    );
    return;
  }

  if (method === "POST" && url === "/api/cloud/mark-disconnected") {
    setCloudState({ enabled: true, status: "error", lastCheckAt: new Date().toISOString() });
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        cloud: runtimeState.cloud,
      })
    );
    return;
  }

  if (method === "GET" && url === "/api/workers/status") {
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        workers: runtimeState.workers,
      })
    );
    return;
  }

  if (method === "POST" && url.startsWith("/api/workers/set-registered/")) {
    const parts = url.split("/");
    const countStr = parts[parts.length - 1];
    const count = parseNonNegativeInteger(countStr);
    if (count === null) {
      res.writeHead(400);
      res.end(
        JSON.stringify({
          ok: false,
          error: "invalid_worker_count",
        })
      );
      return;
    }
    setRegisteredWorkers(count);
    res.writeHead(200);
    res.end(
      JSON.stringify({
        ok: true,
        workers: runtimeState.workers,
      })
    );
    return;
  }

  if (method === "GET" && url === "/api/config") {
    try {
      if (db) syncConfigFromDb(db);
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          config: configState,
        })
      );
    } catch {
      res.writeHead(500);
      res.end(
        JSON.stringify({
          ok: false,
          error: "db_error",
        })
      );
    }
    return;
  }

  if (method === "POST" && url === "/api/config") {
    readJsonBody<any>(req)
      .then((body) => {
        if (
          (body.applianceName !== undefined && typeof body.applianceName !== "string") ||
          (body.companyName !== undefined && typeof body.companyName !== "string") ||
          (body.orgId !== undefined && typeof body.orgId !== "string") ||
          (body.cloudEnabled !== undefined && typeof body.cloudEnabled !== "boolean")
        ) {
          res.writeHead(400);
          res.end(
            JSON.stringify({
              ok: false,
              error: "invalid_config_payload",
            })
          );
          return;
        }

        try {
          if (!db) throw new Error("db missing");

          const nextConfig = {
            applianceName: body.applianceName ?? configState.applianceName,
            companyName: body.companyName ?? configState.companyName,
            orgId: body.orgId ?? configState.orgId,
            cloudEnabled: body.cloudEnabled ?? configState.cloudEnabled,
          };

          upsertAppConfigEntries(db, {
            applianceName: nextConfig.applianceName,
            companyName: nextConfig.companyName,
            orgId: nextConfig.orgId,
            cloudEnabled: nextConfig.cloudEnabled ? "true" : "false",
          });
          updateConfig(nextConfig);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              config: configState,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(
            JSON.stringify({
              ok: false,
              error: "db_error",
            })
          );
        }
      })
      .catch((err) => {
        const errorCode = err?.message === "request_body_too_large" ? "request_body_too_large" : "invalid_json";
        res.writeHead(400);
        res.end(
          JSON.stringify({
            ok: false,
            error: errorCode,
          })
        );
      });
    return;
  }

  if (method === "GET" && url === "/api/deployment") {
    try {
      if (!db) throw new Error("db missing");
      const deployment = getDeployment(db);
      const company = getCompany(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, deployment, company }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/deployment") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || (body.deploymentId !== undefined && typeof body.deploymentId !== "string") || typeof body.deploymentName !== "string" || !body.deploymentName) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_deployment_payload" }));
          return;
        }
        if (body.companyName && typeof body.companyName !== "string") {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_deployment_payload" }));
          return;
        }
        if (body.companySlug && typeof body.companySlug !== "string") {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_deployment_payload" }));
          return;
        }
        const normalizedSlug = body.companySlug ? normalizeCompanySlug(body.companySlug) : null;
        if (body.companyName && !normalizedSlug) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_company_slug" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const existingCompany = getCompany(db);
          const company = body.companyName
            ? upsertCompany(db, {
                companyName: body.companyName,
                companySlug: normalizedSlug!,
                orgId: typeof body.orgId === "string" ? body.orgId : undefined,
              })
            : existingCompany;
          const deploymentId =
            typeof body.deploymentId === "string" && body.deploymentId ? body.deploymentId : generateDeploymentId();
          const deployment = upsertDeployment(db, {
            deploymentId,
            deploymentName: body.deploymentName,
            mode: typeof body.mode === "string" && body.mode ? body.mode : "onprem",
            registeredToCloud: body.registeredToCloud !== undefined ? Boolean(body.registeredToCloud) : false,
            buildChannel: typeof body.buildChannel === "string" ? body.buildChannel : null,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, deployment, company }));
        } catch (err: any) {
          if (err?.code === "company_slug_in_use") {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "company_slug_in_use" }));
            return;
          }
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/cloud-sync") {
    try {
      if (!db) throw new Error("db missing");
      const cloudSync = getCloudSyncState(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, cloudSync }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/cloud-sync") {
    readJsonBody<any>(req)
      .then((body) => {
        if (
          body &&
          ((body.enabled !== undefined && typeof body.enabled !== "boolean") ||
            (body.status !== undefined && typeof body.status !== "string") ||
            (body.cloudEndpoint !== undefined && typeof body.cloudEndpoint !== "string") ||
            (body.tenantKey !== undefined && typeof body.tenantKey !== "string") ||
            (body.lastSyncAt !== undefined && body.lastSyncAt !== null && typeof body.lastSyncAt !== "string"))
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_cloud_sync_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const cloudSync = updateCloudSyncState(db, {
            enabled: body?.enabled,
            status: body?.status,
            cloudEndpoint: body?.cloudEndpoint,
            tenantKey: body?.tenantKey,
            lastSyncAt: body?.lastSyncAt,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, cloudSync }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/devices") {
    try {
      if (!db) throw new Error("db missing");
      const devices = listDevices(db).map((d) => ({ ...d, pollHealth: getDevicePollHealth(db, d.id) }));
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, devices }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/devices") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.hostname !== "string" || !body.hostname || typeof body.ipAddress !== "string" || !body.ipAddress) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_device_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const device = createDevice(db, {
            hostname: body.hostname,
            ipAddress: body.ipAddress,
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : true,
            site: typeof body.site === "string" ? body.site : undefined,
            org: typeof body.org === "string" ? body.org : undefined,
          });
          logAdminAuditEvent(db, {
            entityType: "device",
            entityId: device.id,
            action: "create",
            actor: currentUsernameFromRequest(db, req),
            after: device,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, device }));
        } catch (err: any) {
          if (err?.message === "device_ip_exists") {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "device_ip_exists" }));
            return;
          }
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/devices/update") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.id)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_device_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = updateDevice(db, {
            id: Number(body.id),
            hostname: typeof body.hostname === "string" ? body.hostname : undefined,
            ipAddress: typeof body.ipAddress === "string" ? body.ipAddress : undefined,
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : undefined,
            site: body.site === undefined ? undefined : body.site,
            org: body.org === undefined ? undefined : body.org,
          });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "device_not_found" }));
            return;
          }
          logAdminAuditEvent(db, {
            entityType: "device",
            entityId: updated.id,
            action: "update",
            actor: currentUsernameFromRequest(db, req),
            after: updated,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, device: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/poll-profiles") {
    try {
      if (!db) throw new Error("db missing");
      const profiles = listPollProfiles(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, profiles }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-profiles") {
    readJsonBody<any>(req)
      .then((body) => {
        const validKind = body?.kind === "ping" || body?.kind === "snmp";
        if (
          !body ||
          !validKind ||
          typeof body.name !== "string" ||
          !body.name ||
          !Number.isFinite(body.intervalSec) ||
          body.intervalSec <= 0 ||
          !Number.isInteger(Number(body.intervalSec)) ||
          !Number.isFinite(body.timeoutMs) ||
          body.timeoutMs <= 0 ||
          !Number.isFinite(body.retries) ||
          Number(body.retries) < 0 ||
          !Number.isInteger(Number(body.retries))
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_profile_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const profile = createPollProfile(db, {
            kind: body.kind,
            name: body.name,
            intervalSec: Number(body.intervalSec),
            timeoutMs: Number(body.timeoutMs),
            retries: Number(body.retries),
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : true,
            config: typeof body.config === "object" && body.config !== null ? body.config : {},
          });
          logAdminAuditEvent(db, {
            entityType: "profile",
            entityId: profile.id,
            action: "create",
            actor: currentUsernameFromRequest(db, req),
            after: profile,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, profile }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-profiles/update") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.id)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_profile_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = updatePollProfile(db, {
            id: Number(body.id),
            name: typeof body.name === "string" ? body.name : undefined,
            intervalSec: Number.isFinite(body.intervalSec) ? Number(body.intervalSec) : undefined,
            timeoutMs: Number.isFinite(body.timeoutMs) ? Number(body.timeoutMs) : undefined,
            retries: Number.isFinite(body.retries) ? Number(body.retries) : undefined,
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : undefined,
            config: typeof body.config === "object" && body.config !== null ? body.config : undefined,
          });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "profile_not_found" }));
            return;
          }
          logAdminAuditEvent(db, {
            entityType: "profile",
            entityId: updated.id,
            action: "update",
            actor: currentUsernameFromRequest(db, req),
            after: updated,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, profile: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/workers") {
    try {
      if (!db) throw new Error("db missing");
      const workers = listWorkerRegistrations(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, workers }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/workers/register") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.workerType !== "string" || !body.workerType || typeof body.workerName !== "string" || !body.workerName) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_worker_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = upsertWorkerRegistration(db, {
            workerType: body.workerType,
            workerName: body.workerName,
            capabilities: typeof body.capabilities === "object" && body.capabilities !== null ? body.capabilities : {},
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : true,
          });
          setRegisteredWorkers(listWorkerRegistrations(db).length);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, worker }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/workers/set-enabled") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.workerName !== "string" || !body.workerName.trim() || typeof body.enabled !== "boolean") {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_worker_set_enabled_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = updateWorkerRegistrationEnabled(db, body.workerName.trim(), body.enabled);
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, worker: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/workers/metrics-summary-by-type") {
    try {
      if (!db) throw new Error("db missing");
      const summary = getWorkerMetricsSummaryByType(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, summary }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/workers/metrics")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/workers/metrics", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      const workerType = parsed.searchParams.get("workerType");
      if ((workerNameRaw !== null && !workerNameRaw.trim()) || (workerType && workerType !== "ping" && workerType !== "snmp")) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_worker_metrics_query" }));
        return;
      }
      const snapshot = getWorkerMetricsSnapshot(db, {
        workerName: workerNameRaw ?? undefined,
        workerType: workerType as "ping" | "snmp" | undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, snapshot }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/workers/execution-summary")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/workers/execution-summary", "http://localhost");
      const workerType = parsed.searchParams.get("workerType");
      const sinceSecondsStr = parsed.searchParams.get("sinceSeconds");
      const workerTypeValid = !workerType || workerType === "ping" || workerType === "snmp";
      const sinceValid =
        sinceSecondsStr === null ||
        (Number.isFinite(Number(sinceSecondsStr)) && Number(sinceSecondsStr) > 0 && Number.isInteger(Number(sinceSecondsStr)));
      if (!workerTypeValid || !sinceValid) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_worker_execution_summary_query" }));
        return;
      }
      const summary = getWorkerExecutionSummary(db, {
        workerType: workerType as "ping" | "snmp" | undefined,
        sinceSeconds: sinceSecondsStr ? Number(sinceSecondsStr) : undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, summary }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/tdt/system-snapshots")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/tdt/system-snapshots", "http://localhost");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const deviceId = Number(deviceIdStr);
      if (!deviceIdStr || !Number.isFinite(deviceId) || deviceId <= 0 || !Number.isInteger(deviceId)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_tdt_system_snapshots_query" }));
        return;
      }
      const snapshots = getSystemSnapshotsForDevice(db, deviceId);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, snapshots }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/tdt/interfaces")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/tdt/interfaces", "http://localhost");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const deviceId = Number(deviceIdStr);
      if (!deviceIdStr || !Number.isFinite(deviceId) || deviceId <= 0 || !Number.isInteger(deviceId)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_tdt_interfaces_query" }));
        return;
      }
      const interfaces = listInterfaceSnapshotsForDevice(db, deviceId);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, interfaces }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/tdt/lldp-neighbors")) {
    if (url.startsWith("/api/tdt/lldp-neighbors-all")) {
      try {
        if (!db) throw new Error("db missing");
        const parsed = new URL(req.url ?? "/api/tdt/lldp-neighbors-all", "http://localhost");
        const deviceIdStr = parsed.searchParams.get("deviceId");
        const deviceId = deviceIdStr ? Number(deviceIdStr) : undefined;
        if (deviceIdStr && (!Number.isFinite(deviceId) || deviceId! <= 0 || !Number.isInteger(deviceId!))) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_tdt_lldp_query" }));
          return;
        }
        const neighbors = listLldpNeighbors(db, { deviceId });
        res.writeHead(200);
        res.end(JSON.stringify({ ok: true, neighbors }));
      } catch {
        res.writeHead(500);
        res.end(JSON.stringify({ ok: false, error: "db_error" }));
      }
      return;
    }

    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/tdt/lldp-neighbors", "http://localhost");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const deviceId = Number(deviceIdStr);
      if (!deviceIdStr || !Number.isFinite(deviceId) || deviceId <= 0 || !Number.isInteger(deviceId)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_tdt_lldp_query" }));
        return;
      }
      const neighbors = listLldpNeighborsForDevice(db, deviceId);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, neighbors }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/tdt/discovered-candidates")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/tdt/discovered-candidates", "http://localhost");
      const sourceIdStr = parsed.searchParams.get("sourceDeviceId");
      const sourceDeviceId = Number(sourceIdStr);
      if (!sourceIdStr || !Number.isFinite(sourceDeviceId) || sourceDeviceId <= 0 || !Number.isInteger(sourceDeviceId)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_tdt_discovered_candidates_query" }));
        return;
      }
      const candidates = listDiscoveredCandidatesForSourceDevice(db, sourceDeviceId);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, candidates }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/tdt/lldp-neighbors-all")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/tdt/lldp-neighbors-all", "http://localhost");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const deviceId = deviceIdStr ? Number(deviceIdStr) : undefined;
      if (deviceIdStr && (!Number.isFinite(deviceId) || deviceId! <= 0 || !Number.isInteger(deviceId!))) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_tdt_lldp_query" }));
        return;
      }
      const neighbors = listLldpNeighbors(db, { deviceId });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, neighbors }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/neighbors")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/neighbors", "http://localhost");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const status = parsed.searchParams.get("status");
      const deviceId = deviceIdStr ? Number(deviceIdStr) : undefined;
      if (deviceIdStr && (!Number.isFinite(deviceId) || deviceId! <= 0 || !Number.isInteger(deviceId!))) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_neighbor_query" }));
        return;
      }
      const neighbors = listNeighborsWithReview(db, { deviceId, status: status || undefined });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, neighbors }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/neighbors/history")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/neighbors/history", "http://localhost");
      const neighborIdStr = parsed.searchParams.get("neighborId");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const neighborId = neighborIdStr ? Number(neighborIdStr) : undefined;
      const deviceId = deviceIdStr ? Number(deviceIdStr) : undefined;
      const events = listNeighborReviewEvents(db, {
        neighborId: neighborId && Number.isFinite(neighborId) ? neighborId : undefined,
        deviceId: deviceId && Number.isFinite(deviceId) ? deviceId : undefined,
        limit: 200,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, events }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/neighbors/promote") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.neighborId) || !body.hostname || !body.ipAddress) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_neighbor_promote_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const neighbor = getLldpNeighborById(db, Number(body.neighborId));
          if (!neighbor) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "neighbor_not_found" }));
            return;
          }
          const device = createDevice(db, {
            hostname: body.hostname,
            ipAddress: body.ipAddress,
            enabled: true,
          });
          logAdminAuditEvent(db, {
            entityType: "device",
            entityId: device.id,
            action: "create",
            actor: currentUsernameFromRequest(db, req),
            after: device,
          });
          setNeighborReview(db, {
            neighborId: neighbor.id,
            status: "promoted",
            promotedDeviceId: device.id,
            linkedDeviceId: null,
            note: body.note ?? null,
          });
          logNeighborReviewEvent(db, {
            neighborId: neighbor.id,
            action: "promote",
            actor: currentUsernameFromRequest(db, req),
            deviceId: device.id,
            note: body.note ?? null,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, device }));
        } catch (err: any) {
          if (err?.message === "device_ip_exists") {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "device_ip_exists" }));
            return;
          }
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/neighbors/link") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.neighborId) || !Number.isFinite(body.deviceId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_neighbor_link_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const neighbor = getLldpNeighborById(db, Number(body.neighborId));
          if (!neighbor) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "neighbor_not_found" }));
            return;
          }
          const device = getDeviceById(db, Number(body.deviceId));
          if (!device) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "device_not_found" }));
            return;
          }
          setNeighborReview(db, {
            neighborId: neighbor.id,
            status: "linked",
            linkedDeviceId: device.id,
            promotedDeviceId: null,
            note: body.note ?? null,
          });
          logAdminAuditEvent(db, {
            entityType: "device",
            entityId: device.id,
            action: "link_neighbor",
            actor: currentUsernameFromRequest(db, req),
            note: body.note ?? null,
          });
          logNeighborReviewEvent(db, {
            neighborId: neighbor.id,
            action: "link",
            actor: currentUsernameFromRequest(db, req),
            deviceId: device.id,
            note: body.note ?? null,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, neighborId: neighbor.id, device }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/neighbors/ignore") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.neighborId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_neighbor_ignore_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const neighbor = getLldpNeighborById(db, Number(body.neighborId));
          if (!neighbor) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "neighbor_not_found" }));
            return;
          }
          setNeighborReview(db, {
            neighborId: neighbor.id,
            status: "ignored",
            linkedDeviceId: null,
            promotedDeviceId: null,
            note: body.note ?? null,
          });
          logNeighborReviewEvent(db, {
            neighborId: neighbor.id,
            action: "ignore",
            actor: currentUsernameFromRequest(db, req),
            note: body.note ?? null,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/neighbors/unignore") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.neighborId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_neighbor_unignore_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const neighbor = getLldpNeighborById(db, Number(body.neighborId));
          if (!neighbor) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "neighbor_not_found" }));
            return;
          }
          setNeighborReview(db, {
            neighborId: neighbor.id,
            status: "new",
            linkedDeviceId: null,
            promotedDeviceId: null,
            note: null,
          });
          logNeighborReviewEvent(db, {
            neighborId: neighbor.id,
            action: "unignore",
            actor: currentUsernameFromRequest(db, req),
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/system/services") {
    try {
      if (!db) throw new Error("db missing");
      const workers = listWorkerRegistrations(db);
      const summary = getWorkerMetricsSummaryByType(db);
      const exec = getWorkerExecutionSummary(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, workers, summary: summary.types, execution: exec.types }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/system/service/restart") {
    // Restart not supported via UI; expose explicit unsupported response.
    res.writeHead(503);
    res.end(JSON.stringify({ ok: false, supported: false, error: "restart_not_supported_via_ui" }));
    return;
  }

  if (method === "GET" && url === "/api/system/runtime") {
    try {
      const runtime = {
        hostname: os.hostname(),
        platform: os.platform(),
        arch: os.arch(),
        nodeVersion: process.version,
        uptimeSeconds: Math.round(process.uptime()),
        cwd: process.cwd(),
        sqlitePath: config.sqlitePath,
        version: versionInfo,
      };
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, runtime }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "runtime_unavailable" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/system/storage") {
    try {
      const dbPath = resolve(config.sqlitePath);
      const exists = existsSync(dbPath);
      const stats = exists ? statSync(dbPath) : null;
      const logsPath = join(process.cwd(), "var", "logs", "xbyte-collector.log");
      const storage = {
        dbPath,
        dbExists: exists,
        dbSizeBytes: stats?.size ?? null,
        logPath: logsPath,
      };
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, storage }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "storage_unavailable" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/system/recent-activity") {
    try {
      if (!db) throw new Error("db missing");
      const audits = listAdminAuditEvents(db, { limit: 50 });
      const neighborEvents = listNeighborReviewEvents(db, { limit: 50 });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, audits, neighborEvents }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/licensing/status") {
    try {
      if (!db) throw new Error("db missing");
      const state = getLicenseState(db);
      const effective = evaluateLicenseState(db);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, licensing: state, effective }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/xmon/config") {
    try {
      if (!db) throw new Error("db missing");
      const saved = getAllAppConfig(db);
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          apiBase: saved["XMON_API_BASE"] ?? config.xmonApiBase,
          collectorId: saved["XMON_COLLECTOR_ID"] ?? config.xmonCollectorId,
          apiKey: saved["XMON_API_KEY"] ? "••••••" : null, // do not return real key
        })
      );
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/xmon/config") {
    readJsonBody<any>(req)
      .then((body) => {
        try {
          if (!db) throw new Error("db missing");
          const updates: Record<string, string> = {};
          if (typeof body?.apiBase === "string" && body.apiBase.trim()) updates["XMON_API_BASE"] = body.apiBase.trim();
          if (typeof body?.collectorId === "string" && body.collectorId.trim()) updates["XMON_COLLECTOR_ID"] = body.collectorId.trim();
          if (typeof body?.apiKey === "string" && body.apiKey.trim()) updates["XMON_API_KEY"] = body.apiKey.trim();
          if (Object.keys(updates).length === 0) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "no_updates" }));
            return;
          }
          upsertAppConfigEntries(db, updates);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/xmon/verify") {
    readJsonBody<any>(req)
      .then(async (body) => {
        try {
          if (!db) throw new Error("db missing");
          const overrides = {
            apiBase: typeof body?.apiBase === "string" ? body.apiBase : undefined,
            collectorId: typeof body?.collectorId === "string" ? body.collectorId : undefined,
            apiKey: typeof body?.apiKey === "string" ? body.apiKey : undefined,
          };
          const resolved = resolveXmonConfig(db, overrides);
          if (!resolved.apiBase || !resolved.collectorId || !resolved.apiKey) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "missing_config" }));
            return;
          }
          // persist overrides if provided
          const save: Record<string, string> = {};
          if (overrides.apiBase?.trim()) save["XMON_API_BASE"] = overrides.apiBase.trim();
          if (overrides.collectorId?.trim()) save["XMON_COLLECTOR_ID"] = overrides.collectorId.trim();
          if (overrides.apiKey?.trim()) save["XMON_API_KEY"] = overrides.apiKey.trim();
          if (Object.keys(save).length) upsertAppConfigEntries(db, save);

          const result = await sendPing({
            ...config,
            xmonApiBase: resolved.apiBase,
            xmonCollectorId: resolved.collectorId,
            xmonApiKey: resolved.apiKey,
          });
          const { state } = result;
          setLicenseState(db, {
            status: state.collectionAllowed ? "active" : "revoked",
            subscriptionStatus: state.collectionAllowed ? "active" : "inactive",
            validatedAt: state.lastCheckedAt ?? new Date().toISOString(),
            expiresAt: state.effectiveUntil ?? null,
            lastError: state.reason ?? null,
            lastErrorCode: state.reason ?? null,
          });
          setCloudState({
            enabled: true,
            status: state.collectionAllowed ? "connected" : "blocked",
            lastCheckAt: state.lastCheckedAt ?? new Date().toISOString(),
          });
          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              authorized: state.authorized,
              collectionAllowed: state.collectionAllowed,
              licenseStatus: state.licenseStatus ?? null,
              effectiveUntil: state.effectiveUntil ?? null,
              reason: state.reason ?? null,
              collectorRegistered: true,
              collectorLimit: null,
              activeCollectorCount: 0,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "verify_failed" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/licensing/activate") {
    readJsonBody<any>(req)
      .then(async (body) => {
        if (!body || typeof body.licenseKey !== "string" || !body.licenseKey.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_license_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const licenseKey = body.licenseKey.trim();
          const state = setLicenseState(db, {
            status: "active",
            subscriptionStatus: "active",
            licenseKey,
            customer: typeof body.customer === "string" ? body.customer : null,
            validatedAt: new Date().toISOString(),
            activatedAt: new Date().toISOString(),
            expiresAt: body.expiresAt && typeof body.expiresAt === "string" ? body.expiresAt : null,
            graceUntil: body.graceUntil && typeof body.graceUntil === "string" ? body.graceUntil : null,
            lastError: null,
          });

          const hostname = os.hostname();
          const fingerprint = (state.customer ?? "") + ":" + hostname;
          const apiBaseForBootstrap = getAllAppConfig(db)["XMON_API_BASE"] || config.xmonApiBase;
          if (!apiBaseForBootstrap) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "api_base_missing" }));
            return;
          }

          const activateRes = await activateAppliance(apiBaseForBootstrap, {
            licenseKey,
            hostname,
            fingerprint,
            applianceName: body.applianceName ?? hostname,
          });

          if (activateRes.status >= 400 || !activateRes.body.ok) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: activateRes.body.error ?? "activation_failed", reason: activateRes.body.reason ?? null }));
            return;
          }

          const payload = activateRes.body;
          const save: Record<string, string> = {};
          if (payload.apiBase) save["XMON_API_BASE"] = payload.apiBase;
          if (payload.collectorId) save["XMON_COLLECTOR_ID"] = payload.collectorId;
          if (payload.apiKey) save["XMON_API_KEY"] = payload.apiKey;
          if (Object.keys(save).length) upsertAppConfigEntries(db, save);

          setLicenseState(db, {
            status: payload.collectionAllowed ? "active" : "revoked",
            subscriptionStatus: payload.collectionAllowed ? "active" : "inactive",
            validatedAt: new Date().toISOString(),
            expiresAt: payload.effectiveUntil ?? null,
            lastError: payload.reason ?? null,
            lastErrorCode: payload.reason ?? null,
          });

          setCloudState({
            enabled: true,
            status: payload.collectionAllowed ? "connected" : "blocked",
            lastCheckAt: new Date().toISOString(),
          });

          startCloudBridgeIfReady(db);

          logAdminAuditEvent(db, {
            entityType: "licensing",
            entityId: 1,
            action: "activate",
            actor: currentUsernameFromRequest(db, req),
            after: state,
          });

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              activated: true,
              authorized: payload.authorized ?? false,
              collectionAllowed: payload.collectionAllowed ?? false,
              collectorId: payload.collectorId ?? null,
              apiBase: payload.apiBase ?? null,
              apiKeyIssued: payload.apiKeyIssued ?? false,
              reason: payload.reason ?? null,
              licenseStatus: payload.licenseStatus ?? null,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/licensing/validate") {
    readJsonBody<any>(req)
      .then((body) => {
        try {
          if (!db) throw new Error("db missing");
          const current = getLicenseState(db);
          if (!current.licenseKey) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "license_not_set" }));
            return;
          }
          // Mock/local validator: simple rules based on key string for now
          const key = current.licenseKey.toUpperCase();
          let status = "active";
          let subscriptionStatus = "active";
          let expiresAt: string | null = current.expiresAt ?? null;
          let graceUntil: string | null = current.graceUntil ?? null;
          let lastError: string | null = null;
          let lastErrorCode: string | null = null;
          if (key.includes("EXPIRE")) {
            status = "expired";
            expiresAt = new Date(Date.now() - 24 * 3600 * 1000).toISOString();
            graceUntil = new Date(Date.now() + 7 * 24 * 3600 * 1000).toISOString();
          } else if (key.includes("REVOKE")) {
            status = "revoked";
            lastError = "revoked";
            lastErrorCode = "license_revoked";
          } else if (key.includes("INVALID")) {
            status = "invalid";
            lastError = "invalid_license";
            lastErrorCode = "license_invalid";
          } else if (key.includes("SUBOFF")) {
            subscriptionStatus = "inactive";
            lastError = "inactive_subscription";
            lastErrorCode = "inactive_subscription";
          }
          const state = setLicenseState(db, {
            status,
            subscriptionStatus,
            validatedAt: new Date().toISOString(),
            expiresAt,
            graceUntil,
            lastValidationSource: "local",
            lastError,
            lastErrorCode,
          });
          logAdminAuditEvent(db, {
            entityType: "licensing",
            entityId: 1,
            action: lastError ? "validate_failure" : "validate_success",
            actor: currentUsernameFromRequest(db, req),
            after: state,
          });
          const effective = evaluateLicenseState(db);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, licensing: state, effective }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url.startsWith("/api/audit")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/audit", "http://localhost");
      const entityType = parsed.searchParams.get("entityType") || undefined;
      const entityIdStr = parsed.searchParams.get("entityId");
      const action = parsed.searchParams.get("action") || undefined;
      const limitStr = parsed.searchParams.get("limit");
      const entityId = entityIdStr ? Number(entityIdStr) : undefined;
      const limit = limitStr ? Number(limitStr) : undefined;
      const events = listAdminAuditEvents(db, {
        entityType,
        entityId: entityId && Number.isFinite(entityId) ? entityId : undefined,
        action,
        limit: limit && Number.isFinite(limit) ? limit : undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, events }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/workers/heartbeat") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.workerName !== "string" || !body.workerName.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_worker_heartbeat_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = heartbeatWorkerRegistration(db, body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          setRegisteredWorkers(listWorkerRegistrations(db).length);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, worker }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/system/about") {
    try {
      const about = {
        version: versionInfo,
        serverTime: new Date().toISOString(),
        db: db ? "ok" : "missing",
        bootstrap: db ? getBootstrapState(db) : null,
      };
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, about }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/system/health") {
    try {
      if (!db) throw new Error("db missing");
      const bootstrap = getBootstrapState(db);
      const cloud = getCloudSyncState(db);
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          health: {
            db: "ok",
            bootstrap,
            cloud,
            workers: runtimeState.workers,
          },
        })
      );
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/system/logs") {
    try {
      const logPath = existsSync(logFile) ? logFile : join(process.cwd(), "var", "log", "xbyte-collector.log");
      const path = logPath;
      let entries: Array<{ ts: string; line: string }> = [];
      if (existsSync(path)) {
        const content = readFileSync(path, "utf8");
        const lines = content.split(/\r?\n/).filter(Boolean);
        const tail = lines.slice(-200);
        entries = tail.map((line) => {
          const match = line.match(/^(\d{4}-\d{2}-\d{2}[^ ]*)\s+(.*)$/);
          return { ts: match ? match[1] : "", line: match ? match[2] : line };
        });
      }
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, logs: entries }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "log_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/backups/export/config") {
    try {
      if (!db) throw new Error("db missing");
      const rows = getAllAppConfig(db);
      logAdminAuditEvent(db, {
        entityType: "backup",
        action: "export_config",
        actor: "local-user",
        after: { count: Object.keys(rows ?? {}).length },
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, config: rows }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/backups/export/inventory") {
    try {
      if (!db) throw new Error("db missing");
      const inventory = {
        devices: listDevices(db),
        profiles: listPollProfiles(db),
        targets: listPollTargets(db),
        neighbors: listNeighborsWithReview(db),
      };
      logAdminAuditEvent(db, {
        entityType: "backup",
        action: "export_inventory",
        actor: "local-user",
        after: {
          devices: inventory.devices?.length ?? 0,
          profiles: inventory.profiles?.length ?? 0,
          targets: inventory.targets?.length ?? 0,
          neighbors: inventory.neighbors?.length ?? 0,
        },
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, inventory }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/backups/import/config") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.config !== "object" || body.config === null) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_config_import_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const entries: Record<string, string> = {};
          for (const [k, v] of Object.entries(body.config)) {
            entries[k] = typeof v === "string" ? v : JSON.stringify(v);
          }
          upsertAppConfigEntries(db, entries);
          syncConfigFromDb(db);
          logAdminAuditEvent(db, {
            entityType: "backup",
            action: "import_config",
            actor: "local-user",
            after: { updated: Object.keys(entries).length },
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, updated: Object.keys(entries).length }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/backups/import/inventory") {
    readJsonBody<any>(req)
      .then((body) => {
        const inv = body?.inventory;
        const dryRun = Boolean(body?.dryRun);
        if (!inv || typeof inv !== "object") {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_inventory_import_payload" }));
          return;
        }
        const devices = Array.isArray(inv.devices) ? inv.devices : [];
        const profiles = Array.isArray(inv.profiles) ? inv.profiles : [];
        const targets = Array.isArray(inv.targets) ? inv.targets : [];
        const summary = {
          dryRun,
          devices: { created: 0, updated: 0, skipped: 0 },
          profiles: { created: 0, updated: 0, skipped: 0 },
          targets: { created: 0, skipped: 0 },
          errors: [] as string[],
          errorsDetailed: [] as any[],
        };
        try {
          if (!db) throw new Error("db missing");
          const deviceMap = new Map<string, number>(); // key ip or hostname -> id
          for (const d of devices) {
            if (!d || typeof d.hostname !== "string" || typeof d.ipAddress !== "string") {
              summary.devices.skipped++;
              summary.errors.push("invalid_device");
              summary.errorsDetailed.push({ code: "invalid_device", hostname: d?.hostname, ipAddress: d?.ipAddress });
              continue;
            }
            const existing = findDeviceByIpOrHostname(db, { ipAddress: d.ipAddress, hostname: d.hostname });
            if (existing) {
              summary.devices.updated++;
              if (!dryRun) {
                updateDevice(db, {
                  id: existing.id,
                  hostname: d.hostname,
                  ipAddress: d.ipAddress,
                  enabled: d.enabled !== undefined ? Boolean(d.enabled) : existing.enabled,
                  site: d.site ?? existing.site,
                  org: d.org ?? existing.org,
                });
              }
              deviceMap.set(d.ipAddress, existing.id);
              deviceMap.set(d.hostname, existing.id);
            } else {
              summary.devices.created++;
              if (!dryRun) {
                const created = createDevice(db, {
                  hostname: d.hostname,
                  ipAddress: d.ipAddress,
                  enabled: d.enabled !== undefined ? Boolean(d.enabled) : true,
                  site: d.site ?? null,
                  org: d.org ?? null,
                });
                deviceMap.set(d.ipAddress, created.id);
                deviceMap.set(d.hostname, created.id);
              }
            }
          }

          const profileMap = new Map<string, number>(); // name|kind -> id
          for (const p of profiles) {
            if (!p || typeof p.name !== "string" || typeof p.kind !== "string") {
              summary.profiles.skipped++;
              summary.errors.push("invalid_profile");
              summary.errorsDetailed.push({ code: "invalid_profile", name: p?.name, kind: p?.kind });
              continue;
            }
            const key = `${p.kind}|${p.name}`;
            const existing = findPollProfileByNameAndKind(db, { name: p.name, kind: p.kind });
            if (existing) {
              summary.profiles.updated++;
              if (!dryRun) {
                updatePollProfile(db, {
                  id: existing.id,
                  name: p.name,
                  intervalSec: p.intervalSec ?? existing.intervalSec,
                  timeoutMs: p.timeoutMs ?? existing.timeoutMs,
                  retries: p.retries ?? existing.retries,
                  enabled: p.enabled !== undefined ? Boolean(p.enabled) : existing.enabled,
                  config: p.config ?? existing.config,
                });
              }
              profileMap.set(key, existing.id);
            } else {
              summary.profiles.created++;
              if (!dryRun) {
                const created = createPollProfile(db, {
                  kind: p.kind,
                  name: p.name,
                  intervalSec: p.intervalSec ?? 300,
                  timeoutMs: p.timeoutMs ?? 2000,
                  retries: p.retries ?? 1,
                  enabled: p.enabled !== undefined ? Boolean(p.enabled) : true,
                  config: p.config ?? {},
                });
                profileMap.set(key, created.id);
              }
            }
          }

          for (const t of targets) {
            const deviceKey = t.deviceIp ?? t.deviceHostname ?? null;
            const profileKey = t.profileName && t.profileKind ? `${t.profileKind}|${t.profileName}` : null;
            if (!deviceKey || !profileKey) {
              summary.targets.skipped++;
              summary.errors.push("invalid_target");
              summary.errorsDetailed.push({ code: "invalid_target", deviceKey, profileKey });
              continue;
            }
            const deviceId = deviceMap.get(deviceKey);
            const profileId = profileMap.get(profileKey);
            if (!deviceId || !profileId) {
              summary.targets.skipped++;
              summary.errors.push("target_missing_refs");
              summary.errorsDetailed.push({ code: "target_missing_refs", deviceKey, profileKey });
              continue;
            }
            const existing = findPollTargetByDeviceProfile(db, deviceId, profileId);
            if (existing) {
              summary.targets.skipped++;
            } else {
              summary.targets.created++;
              if (!dryRun) {
                createPollTarget(db, {
                  deviceId,
                  profileId,
                  enabled: t.enabled !== undefined ? Boolean(t.enabled) : true,
                });
              }
            }
          }

          logAdminAuditEvent(db, {
            entityType: "backup",
            action: dryRun ? "import_inventory_dry_run" : "import_inventory_apply",
            actor: "local-user",
            after: summary,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, summary }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/auth/login") {
    readJsonBody<any>(req)
      .then(async (body) => {
        if (
          typeof body?.username !== "string" ||
          typeof body?.password !== "string" ||
          !body.username ||
          !body.password
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_login_payload" }));
          return;
        }

        try {
          if (!db) throw new Error("db missing");
          const result = await authenticateLocalUser(db, body.username, body.password);
          if (!result.ok) {
            const status = result.reason === "inactive_user" ? 403 : 401;
            res.writeHead(status);
            res.end(JSON.stringify({ ok: false, error: result.reason }));
            return;
          }

          const session = issueSession(db, result.user.id, config.sessionTtlSeconds);
          res.setHeader(
            "Set-Cookie",
            serializeSessionCookie(session.id, config.sessionTtlSeconds)
          );
          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              user: result.user,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "auth_error" }));
        }
      })
      .catch((err) => {
        const errorCode = err?.message === "request_body_too_large" ? "request_body_too_large" : "invalid_json";
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: errorCode }));
      });
    return;
  }

  if (method === "POST" && url === "/api/auth/logout") {
    try {
      const sessionId = extractSessionId(req.headers.cookie);
      if (sessionId && db) {
        clearSession(db, sessionId);
      }
      res.setHeader("Set-Cookie", serializeSessionCookie("", 0));
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "auth_error" }));
    }
    return;
  }

  if (method === "GET" && url === "/api/auth/me") {
    try {
      if (!db) throw new Error("db missing");
      const sessionId = extractSessionId(req.headers.cookie);
      if (!sessionId) {
        res.writeHead(401);
        res.end(JSON.stringify({ ok: false, error: "not_authenticated" }));
        return;
      }
      const session = readValidSession(db, sessionId);
      if (!session) {
        res.writeHead(401);
        res.end(JSON.stringify({ ok: false, error: "not_authenticated" }));
        return;
      }
      const user = getUserById(db, session.userId);
      if (!user || !user.isActive) {
        res.writeHead(401);
        res.end(JSON.stringify({ ok: false, error: "not_authenticated" }));
        return;
      }
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          user: { id: user.id, username: user.username, role: user.role, isActive: user.isActive },
        })
      );
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "auth_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/auth/change-password") {
    readJsonBody<any>(req)
      .then(async (body) => {
        if (!body || typeof body.currentPassword !== "string" || typeof body.newPassword !== "string" || !body.newPassword) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_password_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const sessionId = extractSessionId(req.headers.cookie);
          if (!sessionId) {
            res.writeHead(401);
            res.end(JSON.stringify({ ok: false, error: "not_authenticated" }));
            return;
          }
          const session = readValidSession(db, sessionId);
          if (!session) {
            res.writeHead(401);
            res.end(JSON.stringify({ ok: false, error: "not_authenticated" }));
            return;
          }
          const user = getUserById(db, session.userId);
          if (!user || !user.isActive) {
            res.writeHead(401);
            res.end(JSON.stringify({ ok: false, error: "not_authenticated" }));
            return;
          }
          const valid = await verifyPassword(body.currentPassword, user.passwordHash);
          if (!valid) {
            res.writeHead(401);
            res.end(JSON.stringify({ ok: false, error: "invalid_current_password" }));
            return;
          }
          const newHash = await hashPassword(body.newPassword);
          updateUserPassword(db, user.username, newHash);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "auth_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-targets")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-targets", "http://localhost");
      const deviceIdStr = parsed.searchParams.get("deviceId");
      const profileIdStr = parsed.searchParams.get("profileId");
      const deviceId = deviceIdStr ? Number(deviceIdStr) : undefined;
      const profileId = profileIdStr ? Number(profileIdStr) : undefined;
      if (
        (deviceIdStr && (!Number.isFinite(deviceId) || deviceId! <= 0 || !Number.isInteger(deviceId!))) ||
        (profileIdStr && (!Number.isFinite(profileId) || profileId! <= 0 || !Number.isInteger(profileId!)))
      ) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_target_query" }));
        return;
      }
      const targets = listPollTargets(db, { deviceId, profileId });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, targets }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-targets") {
    readJsonBody<any>(req)
      .then((body) => {
        if (
          !body ||
          !Number.isFinite(body.deviceId) ||
          !Number.isFinite(body.profileId) ||
          !Number.isInteger(Number(body.deviceId)) ||
          !Number.isInteger(Number(body.profileId)) ||
          body.deviceId <= 0 ||
          body.profileId <= 0
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_target_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const device = getDeviceById(db, Number(body.deviceId));
          if (!device) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "device_not_found" }));
            return;
          }
          const profile = getPollProfileById(db, Number(body.profileId));
          if (!profile) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "profile_not_found" }));
            return;
          }
          const target = createPollTarget(db, {
            deviceId: Number(body.deviceId),
            profileId: Number(body.profileId),
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : true,
          });
          logAdminAuditEvent(db, {
            entityType: "target",
            entityId: target.id,
            action: "create",
            actor: currentUsernameFromRequest(db, req),
            after: target,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, target }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-targets/update") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.id)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_target_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = updatePollTarget(db, {
            id: Number(body.id),
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : undefined,
          });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "target_not_found" }));
            return;
          }
          logAdminAuditEvent(db, {
            entityType: "target",
            entityId: updated.id,
            action: "update",
            actor: currentUsernameFromRequest(db, req),
            after: updated,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, target: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/poll-jobs") {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs", "http://localhost");
      const status = parsed.searchParams.get("status");
      if (status && !["pending", "running", "completed", "failed"].includes(status)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_status_filter" }));
        return;
      }
      const leaseOwnerRaw = parsed.searchParams.get("leaseOwner");
      if (leaseOwnerRaw !== null && leaseOwnerRaw.trim() === "") {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_lease_owner_filter" }));
        return;
      }
      const leaseOwner = leaseOwnerRaw === null ? undefined : leaseOwnerRaw;
      const jobs = listPollJobs(db, status as any, leaseOwner ?? undefined);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, jobs, limited: true }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/enqueue") {
    readJsonBody<any>(req)
      .then((body) => {
        try {
          if (!db) throw new Error("db missing");

      // Single-target enqueue
      if (body && body.targetId !== undefined) {
        if (!Number.isFinite(body.targetId) || !Number.isInteger(Number(body.targetId)) || Number(body.targetId) <= 0) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_enqueue_payload" }));
          return;
        }
        const target = getPollTargetById(db, Number(body.targetId));
        if (!target) {
          res.writeHead(404);
          res.end(JSON.stringify({ ok: false, error: "target_not_found" }));
          logger.warn("enqueue target not found", { targetId: body.targetId });
          return;
        }
        if (!target.enabled) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "target_disabled" }));
          logger.warn("enqueue target disabled", { targetId: target.id });
          return;
        }
        const lic = licenseAllowsCollection(db);
        if (!lic.allowed) {
          res.writeHead(403);
          res.end(JSON.stringify({ ok: false, error: lic.reason ?? "license_required" }));
          logger.warn("enqueue blocked by license", { targetId: target.id, reason: lic.reason ?? "license_required" });
          return;
        }
        const job = enqueuePollJobForTarget(db, target.id);
        logAdminAuditEvent(db, {
          entityType: "target",
          entityId: target.id,
          action: "enqueue_manual",
              actor: currentUsernameFromRequest(db, req),
              after: { jobId: job.id },
            });
            logger.info("enqueue manual", { targetId: target.id, jobId: job.id });
            res.writeHead(200);
            res.end(JSON.stringify({ ok: true, job }));
            return;
          }

          // Enqueue for all enabled targets
          const enabledTargets = listEnabledPollTargets(db);
          const jobs = enabledTargets.map((t) => createPollJob(db, { targetId: t.id }));
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
    })
    .catch(() => {
      res.writeHead(400);
      res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      logger.warn("enqueue invalid json");
    });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/enqueue-bulk") {
    readJsonBody<any>(req)
      .then((body) => {
        try {
          if (!db) throw new Error("db missing");
          if (!body || !Array.isArray(body.targetIds) || !body.targetIds.length) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "invalid_bulk_enqueue_payload" }));
            return;
          }
          const ids = body.targetIds.map((n: any) => Number(n)).filter((n: number) => Number.isFinite(n) && n > 0 && Number.isInteger(n));
          if (ids.length !== body.targetIds.length) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "invalid_bulk_enqueue_payload" }));
            return;
          }
          for (const id of ids) {
            const target = getPollTargetById(db, id);
            if (!target) {
              res.writeHead(404);
              res.end(JSON.stringify({ ok: false, error: "target_not_found" }));
              return;
            }
            if (!target.enabled) {
              res.writeHead(400);
              res.end(JSON.stringify({ ok: false, error: "target_disabled" }));
              return;
            }
          }
          const jobs = enqueuePollJobsForTargets(db, ids);
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/claim") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.leaseOwner !== "string" || !body.leaseOwner.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_claim_payload" }));
          return;
        }

        try {
          if (!db) throw new Error("db missing");
          const job = claimNextPendingPollJob(db, body.leaseOwner.trim());
          if (!job) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "no_pending_poll_jobs" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/retry") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.jobId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_retry_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const retryJob = retryPollJob(db, Number(body.jobId));
          if (!retryJob) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "failed_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job: retryJob }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/") && url.endsWith("/detail")) {
    const parts = url.split("/");
    const idStr = parts[parts.length - 2];
    const jobId = Number(idStr);
    if (!Number.isFinite(jobId) || jobId <= 0) {
      res.writeHead(400);
      res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_detail_path" }));
      return;
    }
    try {
      if (!db) throw new Error("db missing");
      const detail = getPollJobDetail(db, jobId);
      if (!detail) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "poll_job_not_found" }));
        return;
      }
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, detail }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/running-context-for-worker")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/running-context-for-worker", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      if (!workerNameRaw || !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_running_context_for_worker_query" }));
        return;
      }
      const worker = getWorkerRegistrationByName(db, workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const detail = getRunningPollJobDetailForLeaseOwner(db, workerNameRaw.trim());
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          workerName: workerNameRaw.trim(),
          detail: detail ?? null,
        })
      );
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/finish") {
    readJsonBody<any>(req)
      .then((body) => {
        const validStatus = body?.status === "completed" || body?.status === "failed";
        const resultProvided = Object.prototype.hasOwnProperty.call(body ?? {}, "result");
        const resultValid =
          !resultProvided ||
          body.result === null ||
          typeof body.result === "object" ||
          typeof body.result === "string" ||
          typeof body.result === "number" ||
          typeof body.result === "boolean";
        if (!body || !Number.isFinite(body.jobId) || !validStatus || !resultValid) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_finish_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = finishPollJob(db, {
            jobId: Number(body.jobId),
            status: body.status,
            result: resultProvided ? body.result : undefined,
          });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/release") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.jobId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_release_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = releasePollJob(db, Number(body.jobId));
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/heartbeat") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.jobId) || typeof body.leaseOwner !== "string" || !body.leaseOwner.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_heartbeat_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if ((job.leaseOwner ?? null) !== body.leaseOwner.trim()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }
          const updated = heartbeatPollJob(db, { jobId: job.id, leaseOwner: body.leaseOwner.trim() });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/abandon") {
    readJsonBody<any>(req)
      .then((body) => {
        const resultProvided = Object.prototype.hasOwnProperty.call(body ?? {}, "result");
        const resultValid =
          !resultProvided ||
          body.result === null ||
          typeof body.result === "object" ||
          typeof body.result === "string" ||
          typeof body.result === "number" ||
          typeof body.result === "boolean";
        if (!body || !Number.isFinite(body.jobId) || typeof body.leaseOwner !== "string" || !body.leaseOwner.trim() || !resultValid) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_abandon_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if ((job.leaseOwner ?? null) !== body.leaseOwner.trim()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }
          const updated = abandonPollJob(db, {
            jobId: job.id,
            leaseOwner: body.leaseOwner.trim(),
            result: resultProvided ? body.result : undefined,
          });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/unclaim-by-id") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.jobId) || typeof body.leaseOwner !== "string" || !body.leaseOwner.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_unclaim_by_id_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if ((job.leaseOwner ?? null) !== body.leaseOwner.trim()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }
          const updated = unclaimPollJobById(db, { jobId: job.id, leaseOwner: body.leaseOwner.trim() });
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/claim-for-worker-type") {
    readJsonBody<any>(req)
      .then((body) => {
        const validType = body?.workerType === "ping" || body?.workerType === "snmp";
        if (!body || typeof body.workerName !== "string" || !body.workerName.trim() || !validType) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_claim_for_worker_type_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = listWorkerRegistrations(db).find((w) => w.workerName === body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          if (worker.workerType !== body.workerType) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "worker_type_mismatch" }));
            return;
          }
          const job = claimNextPendingPollJobForWorker(db, {
            workerName: body.workerName.trim(),
            workerType: body.workerType,
          });
          if (!job) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "no_compatible_pending_poll_jobs" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/claim-for-worker-capabilities") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.workerName !== "string" || !body.workerName.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_claim_for_worker_capabilities_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = listWorkerRegistrations(db).find((w) => w.workerName === body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          const caps = (worker.capabilities ?? {}) as any;
          const rawKinds = Array.isArray(caps.supportedKinds)
            ? caps.supportedKinds
            : Array.isArray(caps.kinds)
            ? caps.kinds
            : [];
          const supportedKinds = Array.from(
            new Set(
              (rawKinds as any[])
                .filter((k: any) => typeof k === "string")
                .map((k: string) => k.trim().toLowerCase())
                .filter((k: string) => k === "ping" || k === "snmp")
            )
          ) as string[];
          if (!supportedKinds.length) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "worker_capabilities_missing_supported_kinds" }));
            return;
          }
          const job = claimNextPendingPollJobForWorkerCapabilities(db, {
            workerName: worker.workerName,
            supportedKinds,
          });
          if (!job) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "no_compatible_pending_poll_jobs" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/claim-batch-for-worker-capabilities") {
    readJsonBody<any>(req)
      .then((body) => {
        if (
          !body ||
          typeof body.workerName !== "string" ||
          !body.workerName.trim() ||
          !Number.isFinite(body.limit) ||
          body.limit <= 0 ||
          !Array.isArray(body.supportedKinds)
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_claim_batch_payload" }));
          return;
        }
        const limit = Math.min(1000, Math.max(1, Number(body.limit)));
        const supportedKinds = Array.from(
          new Set(
            (body.supportedKinds as any[])
              .filter((k: any) => typeof k === "string")
              .map((k: string) => k.trim().toLowerCase())
          )
        );
        if (!supportedKinds.length) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_claim_batch_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = getWorkerRegistrationByName(db, body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          const jobs = claimPendingPollJobsBatch(db, {
            workerName: worker.workerName,
            supportedKinds,
            limit,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/running-for-worker")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/running-for-worker", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      if (!workerNameRaw || !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_running_for_worker_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const jobs = listRunningPollJobsForWorker(db, workerNameRaw.trim());
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, jobs }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/stale-for-worker")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/stale-for-worker", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      const olderThanStr = parsed.searchParams.get("olderThanSeconds");
      const olderThanSeconds = olderThanStr ? Number(olderThanStr) : NaN;
      if (!workerNameRaw || !workerNameRaw.trim() || !Number.isFinite(olderThanSeconds) || olderThanSeconds <= 0 || !Number.isInteger(olderThanSeconds)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_stale_for_worker_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const jobs = listStaleRunningPollJobsForWorker(db, { workerName: workerNameRaw.trim(), olderThanSeconds });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, jobs }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/abandon-stale-for-worker") {
    readJsonBody<any>(req)
      .then((body) => {
        const resultProvided = Object.prototype.hasOwnProperty.call(body ?? {}, "result");
        const resultValid =
          !resultProvided ||
          body.result === null ||
          typeof body.result === "object" ||
          typeof body.result === "string" ||
          typeof body.result === "number" ||
          typeof body.result === "boolean";
        if (
          !body ||
          typeof body.workerName !== "string" ||
          !body.workerName.trim() ||
          !Number.isFinite(body.olderThanSeconds) ||
          body.olderThanSeconds <= 0 ||
          !Number.isInteger(body.olderThanSeconds) ||
          !resultValid
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_abandon_stale_for_worker_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = listWorkerRegistrations(db).find((w) => w.workerName === body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          const jobs = abandonStaleRunningPollJobsForWorker(db, {
            workerName: body.workerName.trim(),
            olderThanSeconds: Number(body.olderThanSeconds),
            result: resultProvided ? body.result : undefined,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/retry-failed-for-worker") {
    readJsonBody<any>(req)
      .then((body) => {
        const olderProvided = Object.prototype.hasOwnProperty.call(body ?? {}, "olderThanSeconds");
        const olderValid =
          !olderProvided ||
          (Number.isFinite(body.olderThanSeconds) && body.olderThanSeconds > 0 && Number.isInteger(body.olderThanSeconds));
        if (!body || typeof body.workerName !== "string" || !body.workerName.trim() || !olderValid) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_retry_failed_for_worker_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = listWorkerRegistrations(db).find((w) => w.workerName === body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          const jobs = retryFailedPollJobsForWorker(db, {
            workerName: body.workerName.trim(),
            olderThanSeconds: olderProvided ? Number(body.olderThanSeconds) : undefined,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/completed-for-worker")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/completed-for-worker", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      const olderThanStr = parsed.searchParams.get("olderThanSeconds");
      const olderProvided = olderThanStr !== null;
      const olderValid =
        !olderProvided ||
        (Number.isFinite(Number(olderThanStr)) && Number(olderThanStr) > 0 && Number.isInteger(Number(olderThanStr)));
      if (!workerNameRaw || !workerNameRaw.trim() || !olderValid) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_completed_for_worker_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const jobs = listCompletedPollJobsForWorker(db, {
        workerName: workerNameRaw.trim(),
        olderThanSeconds: olderProvided ? Number(olderThanStr) : undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, jobs }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/summary")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/summary", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      const workerType = parsed.searchParams.get("workerType");
      if (workerNameRaw !== null && !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_summary_query" }));
        return;
      }
      if (workerType && workerType !== "ping" && workerType !== "snmp") {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_summary_query" }));
        return;
      }
      if (workerNameRaw) {
        const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
        if (!worker) {
          res.writeHead(404);
          res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
          return;
        }
      }
      const summary = getPollJobSummary(db, {
        workerName: workerNameRaw ? workerNameRaw.trim() : undefined,
        workerType: workerType === "ping" || workerType === "snmp" ? (workerType as "ping" | "snmp") : undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, summary }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/stale-summary")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/stale-summary", "http://localhost");
      const olderThanStr = parsed.searchParams.get("olderThanSeconds");
      const workerNameRaw = parsed.searchParams.get("workerName");
      const workerType = parsed.searchParams.get("workerType");
      const olderThanSeconds = olderThanStr ? Number(olderThanStr) : NaN;
      if (!Number.isFinite(olderThanSeconds) || olderThanSeconds <= 0 || !Number.isInteger(olderThanSeconds)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_stale_summary_query" }));
        return;
      }
      if (workerNameRaw !== null && !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_stale_summary_query" }));
        return;
      }
      if (workerType && workerType !== "ping" && workerType !== "snmp") {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_stale_summary_query" }));
        return;
      }
      if (workerNameRaw) {
        const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
        if (!worker) {
          res.writeHead(404);
          res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
          return;
        }
      }
      const summary = getStalePollJobSummary(db, {
        olderThanSeconds,
        workerName: workerNameRaw ? workerNameRaw.trim() : undefined,
        workerType: workerType === "ping" || workerType === "snmp" ? (workerType as "ping" | "snmp") : undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, summary }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/availability-for-worker-capabilities")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/availability-for-worker-capabilities", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      if (!workerNameRaw || !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_availability_for_worker_capabilities_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const caps = (worker.capabilities ?? {}) as any;
      const rawKinds = Array.isArray(caps.supportedKinds)
        ? caps.supportedKinds
        : Array.isArray(caps.kinds)
        ? caps.kinds
        : [];
      const supportedKinds = Array.from(
        new Set(
          (rawKinds as any[])
            .filter((k: any) => typeof k === "string")
            .map((k: string) => k.trim().toLowerCase())
            .filter((k: string) => k === "ping" || k === "snmp")
        )
      ) as string[];
      if (!supportedKinds.length) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "worker_capabilities_missing_supported_kinds" }));
        return;
      }
      const availability = getPendingPollJobAvailabilityForWorkerCapabilities(db, { supportedKinds });
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          availability: {
            workerName: worker.workerName,
            supportedKinds,
            ...availability,
          },
        })
      );
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/workers/capabilities")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/workers/capabilities", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      if (!workerNameRaw || !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_worker_capabilities_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const caps = (worker.capabilities ?? {}) as any;
      const rawKinds = Array.isArray(caps.supportedKinds)
        ? caps.supportedKinds
        : Array.isArray(caps.kinds)
        ? caps.kinds
        : [];
      const supportedKinds = Array.from(
        new Set(
          (rawKinds as any[])
            .filter((k: any) => typeof k === "string")
            .map((k: string) => k.trim().toLowerCase())
            .filter((k: string) => k === "ping" || k === "snmp")
        )
      ) as string[];
      res.writeHead(200);
      res.end(
        JSON.stringify({
          ok: true,
          worker: {
            workerName: worker.workerName,
            workerType: worker.workerType,
            enabled: worker.enabled,
            lastHeartbeatAt: worker.lastHeartbeatAt,
            supportedKinds,
            rawCapabilities: worker.capabilities ?? {},
          },
        })
      );
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/summary-for-worker")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/summary-for-worker", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      if (!workerNameRaw || !workerNameRaw.trim()) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_summary_for_worker_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const summary = getPollJobSummaryForWorker(db, workerNameRaw.trim());
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, summary }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/failed-for-worker")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/failed-for-worker", "http://localhost");
      const workerNameRaw = parsed.searchParams.get("workerName");
      const olderThanStr = parsed.searchParams.get("olderThanSeconds");
      const olderProvided = olderThanStr !== null;
      const olderValid =
        !olderProvided ||
        (Number.isFinite(Number(olderThanStr)) && Number(olderThanStr) > 0 && Number.isInteger(Number(olderThanStr)));
      if (!workerNameRaw || !workerNameRaw.trim() || !olderValid) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_failed_for_worker_query" }));
        return;
      }
      const worker = listWorkerRegistrations(db).find((w) => w.workerName === workerNameRaw.trim());
      if (!worker) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
        return;
      }
      const jobs = listFailedPollJobsForWorker(db, {
        workerName: workerNameRaw.trim(),
        olderThanSeconds: olderProvided ? Number(olderThanStr) : undefined,
      });
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, jobs }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/requeue-stale-for-worker") {
    readJsonBody<any>(req)
      .then((body) => {
        if (
          !body ||
          typeof body.workerName !== "string" ||
          !body.workerName.trim() ||
          !Number.isFinite(body.olderThanSeconds) ||
          body.olderThanSeconds <= 0 ||
          !Number.isInteger(body.olderThanSeconds)
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_requeue_stale_for_worker_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const worker = listWorkerRegistrations(db).find((w) => w.workerName === body.workerName.trim());
          if (!worker) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "worker_not_found" }));
            return;
          }
          const jobs = requeueStaleRunningPollJobsForWorker(db, {
            workerName: body.workerName.trim(),
            olderThanSeconds: Number(body.olderThanSeconds),
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url.startsWith("/api/poll-jobs/stale")) {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/api/poll-jobs/stale", "http://localhost");
      const olderThanStr = parsed.searchParams.get("olderThanSeconds");
      const olderThanSeconds = olderThanStr ? Number(olderThanStr) : NaN;
      if (!Number.isFinite(olderThanSeconds) || olderThanSeconds <= 0 || !Number.isInteger(olderThanSeconds)) {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_stale_query" }));
        return;
      }
      const jobs = listStaleRunningPollJobs(db, olderThanSeconds);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, jobs }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/requeue-stale") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.olderThanSeconds) || body.olderThanSeconds <= 0 || !Number.isInteger(body.olderThanSeconds)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_requeue_stale_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const jobs = requeueStaleRunningPollJobs(db, Number(body.olderThanSeconds));
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, jobs }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/poll-jobs/claim-by-id") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || !Number.isFinite(body.jobId) || typeof body.leaseOwner !== "string" || !body.leaseOwner.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_poll_job_claim_by_id_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const job = claimPollJobById(db, { jobId: Number(body.jobId), leaseOwner: body.leaseOwner.trim() });
          if (!job) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "pending_poll_job_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, job }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "GET" && url === "/api/agent-enrollment") {
    try {
      if (!db) throw new Error("db missing");
      const parsed = new URL(req.url ?? "/", "http://localhost");
      const status = parsed.searchParams.get("status");
      if (status && status !== "active" && status !== "revoked") {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_agent_enrollment_status_filter" }));
        return;
      }
      const enrollments = listAgentEnrollments(db, status as "active" | "revoked" | undefined);
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, enrollments }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "GET" && url.startsWith("/api/agent-enrollment/")) {
    const parts = url.split("/");
    const enrollmentId = parts[parts.length - 1];
    try {
      if (!db) throw new Error("db missing");
      const enrollment = getAgentEnrollmentByEnrollmentId(db, enrollmentId);
      if (!enrollment) {
        res.writeHead(404);
        res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
        return;
      }
      res.writeHead(200);
      res.end(JSON.stringify({ ok: true, enrollment }));
    } catch {
      res.writeHead(500);
      res.end(JSON.stringify({ ok: false, error: "db_error" }));
    }
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment") {
    readJsonBody<any>(req)
      .then((body) => {
        const validType = body?.agentType === "windows" || body?.agentType === "linux";
        const expiresOk = body?.expiresAt === undefined || body.expiresAt === null || typeof body.expiresAt === "string";
        if (!validType || !expiresOk) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_enrollment_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const deployment = getDeployment(db);
          const company = getCompany(db);
          const enrollment = createAgentEnrollment(db, {
            enrollmentId: generateEnrollmentId(),
            agentType: body.agentType,
            token: generateEnrollmentToken(),
            status: "active",
            companySlug: company?.companySlug ?? null,
            deploymentId: deployment?.deploymentId ?? null,
            expiresAt: body.expiresAt ?? null,
          });
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, enrollment }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/verify") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_enrollment_verify_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token);
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }
          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);
          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                status: enrollment.status,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
                expiresAt: enrollment.expiresAt,
                changedBy: enrollment.changedBy,
              },
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/bootstrap-snapshot") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_bootstrap_snapshot_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const supportedKinds =
            enrollment.agentType === "windows" || enrollment.agentType === "linux" ? ["ping"] : [];
          const queueAvailability =
            supportedKinds.length > 0
              ? getPendingPollJobAvailabilityForWorkerCapabilities(db, { supportedKinds })
              : { totalPendingCompatible: 0, byKind: [] };

          const deployment = getDeployment(db);
          const company = getCompany(db);
          const cloudSync = getCloudSyncState(db);

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              snapshot: {
                enrollment: {
                  enrollmentId: enrollment.enrollmentId,
                  agentType: enrollment.agentType,
                  status: enrollment.status,
                  companySlug: enrollment.companySlug,
                  deploymentId: enrollment.deploymentId,
                  expiresAt: enrollment.expiresAt,
                  changedBy: enrollment.changedBy,
                },
                deployment: deployment
                  ? {
                      deploymentId: deployment.deploymentId,
                      deploymentName: deployment.deploymentName,
                      mode: deployment.mode,
                      registeredToCloud: deployment.registeredToCloud,
                      buildChannel: deployment.buildChannel,
                    }
                  : null,
                company: company
                  ? {
                      companyName: company.companyName,
                      companySlug: company.companySlug,
                      orgId: company.orgId,
                    }
                  : null,
                cloudSync: {
                  enabled: cloudSync.enabled,
                  status: cloudSync.status,
                  lastSyncAt: cloudSync.lastSyncAt,
                  cloudEndpoint: cloudSync.cloudEndpoint,
                  tenantKey: cloudSync.tenantKey,
                },
                queueAvailability: {
                  supportedKinds,
                  totalPendingCompatible: queueAvailability.totalPendingCompatible,
                  byKind: queueAvailability.byKind,
                },
              },
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/claim-job") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_claim_job_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const supportedKinds =
            enrollment.agentType === "windows" || enrollment.agentType === "linux" ? ["ping"] : [];
          if (!supportedKinds.length) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_no_supported_kinds" }));
            return;
          }

          const job = claimNextPendingPollJobForWorkerCapabilities(db, {
            workerName: enrollment.enrollmentId,
            supportedKinds,
          });
          if (!job) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "no_compatible_pending_poll_jobs" }));
            return;
          }

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              job,
              supportedKinds,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/release-job") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token.trim() || !Number.isFinite(body.jobId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_release_job_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if (job.leaseOwner !== enrollment.enrollmentId) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }

          const released = unclaimPollJobById(db, { jobId: Number(body.jobId), leaseOwner: enrollment.enrollmentId });
          if (!released) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              job: released,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/heartbeat-job") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token.trim() || !Number.isFinite(body.jobId)) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_heartbeat_job_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if (job.leaseOwner !== enrollment.enrollmentId) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }

          const heartbeat = heartbeatPollJob(db, { jobId: Number(body.jobId), leaseOwner: enrollment.enrollmentId });
          if (!heartbeat) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              job: heartbeat,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/finish-job") {
    readJsonBody<any>(req)
      .then((body) => {
        const resultAllowed =
          body?.result === undefined ||
          body.result === null ||
          typeof body.result === "object" ||
          typeof body.result === "string" ||
          typeof body.result === "number" ||
          typeof body.result === "boolean";
        if (
          !body ||
          typeof body.token !== "string" ||
          !body.token.trim() ||
          !Number.isFinite(body.jobId) ||
          (body.status !== "completed" && body.status !== "failed") ||
          !resultAllowed
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_finish_job_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if (job.leaseOwner !== enrollment.enrollmentId) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }

          const finished = finishPollJob(db, {
            jobId: Number(body.jobId),
            status: body.status,
            result: body.result,
          });
          if (!finished) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              job: finished,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/abandon-job") {
    readJsonBody<any>(req)
      .then((body) => {
        const resultAllowed =
          body?.result === undefined ||
          body.result === null ||
          typeof body.result === "object" ||
          typeof body.result === "string" ||
          typeof body.result === "number" ||
          typeof body.result === "boolean";
        if (!body || typeof body.token !== "string" || !body.token.trim() || !Number.isFinite(body.jobId) || !resultAllowed) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_abandon_job_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const job = getPollJobById(db, Number(body.jobId));
          if (!job || job.status !== "running") {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }
          if (job.leaseOwner !== enrollment.enrollmentId) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "poll_job_lease_owner_mismatch" }));
            return;
          }

          const abandoned = abandonPollJob(db, {
            jobId: Number(body.jobId),
            leaseOwner: enrollment.enrollmentId,
            result: body.result,
          });
          if (!abandoned) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "running_poll_job_not_found" }));
            return;
          }

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              job: abandoned,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/current-job") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_current_job_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const job = getRunningPollJobForLeaseOwner(db, enrollment.enrollmentId);
          if (job) {
            touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);
          }

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              job: job ?? null,
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url === "/api/agent-enrollment/queue-availability") {
    readJsonBody<any>(req)
      .then((body) => {
        if (!body || typeof body.token !== "string" || !body.token.trim()) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_queue_availability_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const enrollment = getAgentEnrollmentByToken(db, body.token.trim());
          if (!enrollment) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          if (enrollment.status === "revoked") {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_revoked" }));
            return;
          }
          if (enrollment.expiresAt && new Date(enrollment.expiresAt).getTime() < Date.now()) {
            res.writeHead(403);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_expired" }));
            return;
          }

          const supportedKinds =
            enrollment.agentType === "windows" || enrollment.agentType === "linux" ? ["ping"] : [];
          const availability =
            supportedKinds.length > 0
              ? getPendingPollJobAvailabilityForWorkerCapabilities(db, { supportedKinds })
              : { totalPendingCompatible: 0, byKind: [] };

          touchAgentEnrollmentLastUsedAt(db, enrollment.enrollmentId);

          res.writeHead(200);
          res.end(
            JSON.stringify({
              ok: true,
              enrollment: {
                enrollmentId: enrollment.enrollmentId,
                agentType: enrollment.agentType,
                companySlug: enrollment.companySlug,
                deploymentId: enrollment.deploymentId,
              },
              availability: {
                supportedKinds,
                totalPendingCompatible: availability.totalPendingCompatible,
                byKind: availability.byKind,
              },
            })
          );
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url.startsWith("/api/agent-enrollment/") && url.endsWith("/revoke")) {
    const parts = url.split("/");
    const enrollmentId = parts[parts.length - 2];
    readJsonBody<any>(req)
      .then((body) => {
        if (body && body.changedBy !== undefined && typeof body.changedBy !== "string") {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_enrollment_revoke_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const revoked = revokeAgentEnrollment(db, enrollmentId, body?.changedBy ?? null);
          if (!revoked) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, enrollment: revoked }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  if (method === "POST" && url.startsWith("/api/agent-enrollment/") && url.endsWith("/status")) {
    const parts = url.split("/");
    const enrollmentId = parts[parts.length - 2];
    readJsonBody<any>(req)
      .then((body) => {
        if (
          !body ||
          (body.status !== "active" && body.status !== "revoked") ||
          (body.changedBy !== undefined && typeof body.changedBy !== "string")
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_agent_enrollment_status_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const updated = updateAgentEnrollmentStatus(db, enrollmentId, body.status, body.changedBy ?? null);
          if (!updated) {
            res.writeHead(404);
            res.end(JSON.stringify({ ok: false, error: "agent_enrollment_not_found" }));
            return;
          }
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, enrollment: updated }));
        } catch {
          res.writeHead(500);
          res.end(JSON.stringify({ ok: false, error: "db_error" }));
        }
      })
      .catch(() => {
        res.writeHead(400);
        res.end(JSON.stringify({ ok: false, error: "invalid_json" }));
      });
    return;
  }

  // Static frontend assets
  if (method === "GET" && !url.startsWith("/api/")) {
    const pathname = (url.split("?")[0] ?? "/") || "/";
    if (serveStatic(pathname, res)) return;
    // SPA fallback if assets exist
    if (existsSync(distIndexPath)) {
      serveFile(distIndexPath, res, "text/html; charset=utf-8");
      return;
    }
    // fallthrough to 404 if no assets
  }

  res.writeHead(404);
  res.end(
    JSON.stringify({
      ok: false,
      error: "not_found",
      path: url,
    })
  );
});

start().catch((err) => {
  console.error(
    JSON.stringify({
      level: "fatal",
      msg: "server startup failed",
      error: err?.message ?? String(err),
      time: new Date().toISOString(),
    })
  );
  process.exit(1);
});

let shuttingDown = false;

function handleShutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;

  if (stopApplianceSync) {
    try {
      stopApplianceSync();
    } catch {
      /* ignore */
    }
  }

  console.log(
    JSON.stringify({
      level: "info",
      msg: "xbyte-collector server shutting down",
      signal,
      time: new Date().toISOString(),
    })
  );

  server.close((err) => {
    if (err) {
      console.error(
        JSON.stringify({
          level: "error",
          msg: "server close failed",
          error: err.message,
          time: new Date().toISOString(),
        })
      );
      process.exit(1);
      return;
    }
    process.exit(0);
  });
}

process.once("SIGINT", () => handleShutdown("SIGINT"));
process.once("SIGTERM", () => handleShutdown("SIGTERM"));

function syncConfigFromDb(db: DB) {
  const rows = getAllAppConfig(db);
  updateConfig({
    applianceName: rows.applianceName ?? "",
    companyName: rows.companyName ?? "",
    orgId: rows.orgId ?? "",
    cloudEnabled: (rows.cloudEnabled ?? "false") === "true",
  });
}

function syncBootstrapFromDb(db: DB) {
  const row = getBootstrapState(db);
  runtimeState.bootstrap = {
    configured: row.configured,
    status: row.status as any,
  };
}

async function seedInitialAdmin(db: DB) {
  const passwordHash = await hashPassword(config.bootstrapAdminPassword);
  const result = createInitialAdminIfMissing(db, {
    username: config.bootstrapAdminUsername,
    passwordHash,
  });

  console.log(
    JSON.stringify({
      level: "info",
      msg: "admin seed checked",
      created: result.created,
      username: result.user?.username ?? config.bootstrapAdminUsername,
      time: new Date().toISOString(),
    })
  );
}

async function start() {
  if (dbInitFailed || !db) {
    throw new Error("database initialization failed");
  }

  try {
    await seedInitialAdmin(db);
  } catch (err: any) {
    console.error(
      JSON.stringify({
        level: "fatal",
        msg: "admin seed failed",
        error: err?.message ?? String(err),
        time: new Date().toISOString(),
      })
    );
    throw err;
  }

  server.listen(config.port, config.host, () => {
    console.log(
      JSON.stringify({
        level: "info",
        msg: "xbyte-collector server listening",
        host: config.host,
        port: config.port,
        phase: "phase-1-foundation",
        time: new Date().toISOString(),
      })
    );
  });
}

function serializeSessionCookie(sessionId: string, ttlSeconds: number): string {
  const parts = [`xbyte_sid=${encodeURIComponent(sessionId)}`, "HttpOnly", "Path=/", "SameSite=Lax"];
  if (ttlSeconds > 0) {
    parts.push(`Max-Age=${ttlSeconds}`);
  } else {
    parts.push("Max-Age=0");
  }
  return parts.join("; ");
}

function extractSessionId(cookieHeader?: string): string | null {
  if (!cookieHeader) return null;
  const cookies = cookieHeader.split(";").map((c) => c.trim());
  for (const c of cookies) {
    if (c.startsWith("xbyte_sid=")) {
      const val = c.slice("xbyte_sid=".length);
      return decodeURIComponent(val);
    }
  }
  return null;
}

function currentUsernameFromRequest(dbRef: DB | null, req: any): string {
  try {
    if (!dbRef) return "local-user";
    const sessionId = extractSessionId(req.headers.cookie);
    if (!sessionId) return "local-user";
    const session = readValidSession(dbRef, sessionId);
    if (!session) return "local-user";
    const user = getUserById(dbRef, session.userId);
    return user?.username ?? "local-user";
  } catch {
    return "local-user";
  }
}

function serveStatic(pathname: string, res: import("node:http").ServerResponse): boolean {
  if (!existsSync(distDir)) return false;
  const safePath = normalize(pathname);
  const target = resolve(distDir, "." + (safePath.startsWith("/") ? safePath : "/" + safePath));
  if (!target.startsWith(distDir)) return false;
  if (existsSync(target) && statSync(target).isFile()) {
    serveFile(target, res, contentTypeFor(target));
    return true;
  }
  return false;
}

function serveFile(filePath: string, res: import("node:http").ServerResponse, contentType: string) {
  try {
    const data = readFileSync(filePath);
    res.writeHead(200, { "Content-Type": contentType });
    res.end(data);
  } catch {
    res.writeHead(404);
    res.end(JSON.stringify({ ok: false, error: "not_found" }));
  }
}

function contentTypeFor(filePath: string): string {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".json")) return "application/json; charset=utf-8";
  if (filePath.endsWith(".svg")) return "image/svg+xml";
  if (filePath.endsWith(".png")) return "image/png";
  return "application/octet-stream";
}
