import { createServer } from "node:http";
import { readFileSync, existsSync, statSync } from "node:fs";
import { resolve, join, normalize } from "node:path";
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
  listDevices,
  createPollProfile,
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
  getRunningPollJobDetailForLeaseOwner,
  getRunningPollJobForLeaseOwner,
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

const config = loadConfig();
let dbInitFailed = false;
let db: DB | null = null;
const distDir = resolve("web", "dist");
const distIndexPath = join(distDir, "index.html");

try {
  db = initDatabase(config);
  syncConfigFromDb(db);
  syncBootstrapFromDb(db);
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
      const devices = listDevices(db);
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
          res.writeHead(200);
          res.end(JSON.stringify({ ok: true, device }));
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
          !Number.isFinite(body.timeoutMs) ||
          !Number.isFinite(body.retries)
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_profile_payload" }));
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

  if (method === "GET" && url === "/api/poll-targets") {
    try {
      if (!db) throw new Error("db missing");
      const targets = listPollTargets(db);
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
          !Number.isFinite(body.profileId)
        ) {
          res.writeHead(400);
          res.end(JSON.stringify({ ok: false, error: "invalid_target_payload" }));
          return;
        }
        try {
          if (!db) throw new Error("db missing");
          const device = getDeviceById(db, Number(body.deviceId));
          if (!device) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "device_not_found" }));
            return;
          }
          const profile = getPollProfileById(db, Number(body.profileId));
          if (!profile) {
            res.writeHead(400);
            res.end(JSON.stringify({ ok: false, error: "profile_not_found" }));
            return;
          }
          const target = createPollTarget(db, {
            deviceId: Number(body.deviceId),
            profileId: Number(body.profileId),
            enabled: body.enabled !== undefined ? Boolean(body.enabled) : true,
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
      res.end(JSON.stringify({ ok: true, jobs }));
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
            if (!Number.isFinite(body.targetId)) {
              res.writeHead(400);
              res.end(JSON.stringify({ ok: false, error: "invalid_target_id" }));
              return;
            }
            const target = getPollTargetById(db, Number(body.targetId));
            if (!target) {
              res.writeHead(400);
              res.end(JSON.stringify({ ok: false, error: "target_not_found" }));
              return;
            }
            if (!target.enabled) {
              res.writeHead(400);
              res.end(JSON.stringify({ ok: false, error: "target_disabled" }));
              return;
            }
            const job = createPollJob(db, { targetId: target.id });
            res.writeHead(200);
            res.end(JSON.stringify({ ok: true, jobs: [job] }));
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
