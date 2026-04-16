import { setTimeout as delay } from "node:timers/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execFileAsync = promisify(execFile);
import { loadConfig, type AppConfig } from "./config.js";
import {
  initDatabase,
  upsertWorkerRegistration,
  heartbeatWorkerRegistration,
  claimPendingPollJobsBatch,
  finishPollJob,
  abandonPollJob,
  unclaimPollJobById,
  getWorkerRegistrationByName,
  getPollJobDetail,
  recordAvailabilityPingResult,
  licenseAllowsCollection,
  getAllAppConfig,
  type DB,
} from "./db.js";
import { enqueueTelemetry, enqueueDeviceSnapshot, enqueueDeviceState, startTelemetryQueue } from "./telemetry-queue.js";
import { publishAvailabilityUpdate } from "./xmon-client.js";

type WorkerConfig = {
  workerName: string;
  heartbeatMs: number;
  loopMs: number;
  stubDelayMs: number;
  batchSize: number;
  fpingPath: string;
};

function buildWorkerConfig(): WorkerConfig {
  const cfg = loadConfig();
  return {
    workerName: cfg.pingWorkerName?.trim() || "ping-worker-local",
    heartbeatMs: cfg.pingWorkerHeartbeatMs ?? 15_000,
    loopMs: cfg.pingWorkerLoopMs ?? 5_000,
    stubDelayMs: cfg.pingWorkerStubDelayMs ?? 250,
    batchSize: cfg.pingWorkerBatchSize ?? 200,
    fpingPath: cfg.pingWorkerFpingPath ?? "fping",
  };
}

function log(data: Record<string, any>) {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      workerType: "ping",
      ...data,
    })
  );
}

async function runLoop(db: DB, workerCfg: WorkerConfig, shuttingDown: { flag: boolean }, resolveCloudCfg: () => AppConfig) {
  let currentJobIds = new Set<number>();
  let heartbeatHandle: NodeJS.Timeout | null = null;

  async function releaseInFlight(reason: string) {
    if (!currentJobIds.size) return;
    for (const jobId of Array.from(currentJobIds)) {
      try {
        const released = unclaimPollJobById(db, { jobId, leaseOwner: workerCfg.workerName });
        log({
          level: "info",
          msg: "released in-flight job on shutdown",
          jobId,
          released: Boolean(released),
          reason,
          workerName: workerCfg.workerName,
        });
      } catch (err: any) {
        log({
          level: "error",
          msg: "failed to release in-flight job on shutdown",
          jobId,
          reason,
          error: err?.message ?? String(err),
          workerName: workerCfg.workerName,
        });
      }
    }
    currentJobIds.clear();
  }

  // register once on start
  upsertWorkerRegistration(db, {
    workerType: "ping",
    workerName: workerCfg.workerName,
    capabilities: { supportedKinds: ["ping"] },
    enabled: true,
  });
  log({ level: "info", msg: "worker registered", workerName: workerCfg.workerName });

  // heartbeat ticker
  heartbeatHandle = setInterval(() => {
    try {
      const worker = heartbeatWorkerRegistration(db, workerCfg.workerName);
      if (!worker) {
        log({ level: "error", msg: "heartbeat failed: worker missing", workerName: workerCfg.workerName });
      }
    } catch (err: any) {
      log({ level: "error", msg: "heartbeat exception", error: err?.message ?? String(err), workerName: workerCfg.workerName });
    }
  }, workerCfg.heartbeatMs);

  while (!shuttingDown.flag) {
    try {
      if (shuttingDown.flag) break;
      const self = getWorkerRegistrationByName(db, workerCfg.workerName);
      if (!self || self.enabled === false) {
        await delay(workerCfg.loopMs);
        continue;
      }
      const licPre = licenseAllowsCollection(db);
      if (!licPre.allowed) {
        enqueueTelemetry({
          messageId: `blocked-${workerCfg.workerName}-${Date.now()}`,
          kind: "event",
          ts: new Date().toISOString(),
          payload: { type: "worker_blocked", workerName: workerCfg.workerName, reason: licPre.reason ?? "license_required" },
        });
        await delay(workerCfg.loopMs);
        continue;
      }
      const jobs = claimPendingPollJobsBatch(db, {
        workerName: workerCfg.workerName,
        supportedKinds: ["ping"],
        limit: workerCfg.batchSize,
      });

      if (!jobs.length) {
        await delay(workerCfg.loopMs);
        continue;
      }
      currentJobIds = new Set(jobs.map((j) => j.id));

      log({ level: "info", msg: "batch claimed", workerName: workerCfg.workerName, count: jobs.length });

      const details = await Promise.all(
        jobs.map(async (job) => {
          const detail = getPollJobDetail(db, job.id);
          return { job, detail };
        })
      );

      const validDetails = details.filter((d) => d.detail) as Array<{ job: any; detail: any }>;
      const timeoutMs =
        validDetails.length > 0
          ? Math.max(
              ...validDetails.map((d) =>
                typeof d.detail.profile.timeoutMs === "number" ? d.detail.profile.timeoutMs : 2000
              )
            )
          : 2000;

      const lic = licenseAllowsCollection(db);
      if (!lic.allowed) {
        let blockedCount = 0;
        for (const { job, detail } of validDetails) {
          finishPollJob(db, {
            jobId: job.id,
            status: "failed",
            result: {
              workerType: "ping",
              success: false,
              blockedByLicense: true,
              code: lic.reason ?? "license_required",
              effectiveStatus: lic.effectiveStatus,
              message: `Collection blocked: ${lic.reason ?? "license_required"}`,
              processedAt: new Date().toISOString(),
              context: {
                jobId: detail.job.id,
                targetId: detail.target.id,
                deviceId: detail.device.id,
                deviceHostname: detail.device.hostname,
                deviceIpAddress: detail.device.ipAddress,
                profileId: detail.profile.id,
                profileKind: detail.profile.kind,
              },
            },
          });
          currentJobIds.delete(job.id);
          blockedCount++;
        }
        log({
          level: "warn",
          msg: "batch blocked by license",
          workerName: workerCfg.workerName,
          effectiveStatus: lic.effectiveStatus,
          reason: lic.reason,
          blocked: blockedCount,
          claimed: jobs.length,
        });
        // abandon invalids as usual
        const invalids = details.filter((d) => !d.detail);
        for (const { job } of invalids) {
          abandonPollJob(db, {
            jobId: job.id,
            leaseOwner: workerCfg.workerName,
            result: {
              stub: true,
              workerType: "ping",
              error: "missing_context_after_claim",
              failedAt: new Date().toISOString(),
            },
          });
          currentJobIds.delete(job.id);
        }
        continue;
      }

      const targets = validDetails.map((d) => ({ jobId: d.job.id, ip: d.detail.device.ipAddress }));

      log({
        level: "info",
        msg: "fping batch start",
        workerName: workerCfg.workerName,
        count: targets.length,
        timeoutMs,
      });

      const probeResults = await runPingBatch(targets, workerCfg.fpingPath, timeoutMs);

      let successCount = 0;
      let failCount = 0;

      for (const { job, detail } of validDetails) {
        const probe = probeResults.get(detail.device.ipAddress);
        const success = probe?.success === true;
        const status = success ? "completed" : "failed";
        if (success) successCount++;
        else failCount++;
        const resultPayload = {
          stub: false,
          workerType: "ping",
          success,
          target: detail.device.ipAddress,
          latencyMs: probe?.latencyMs ?? null,
          error: success ? undefined : probe?.error ?? "unreachable",
          processedAt: new Date().toISOString(),
          context: {
            jobId: detail.job.id,
            targetId: detail.target.id,
            deviceId: detail.device.id,
            deviceHostname: detail.device.hostname,
            deviceIpAddress: detail.device.ipAddress,
            profileId: detail.profile.id,
            profileKind: detail.profile.kind,
          },
        };
        finishPollJob(db, {
          jobId: job.id,
          status,
          result: resultPayload,
        });

        // emit a lightweight device snapshot for inventory/state
        enqueueDeviceSnapshot({
          deviceId: detail.device.id,
          name: detail.device.hostname,
          ip: detail.device.ipAddress,
          deviceType: detail.device.type ?? detail.device.kind ?? undefined,
          status: success ? "up" : "down",
          snmpProfileId: detail.device.snmpProfileId ?? null,
          snmpPollerIds: detail.device.snmpPollerIds ?? undefined,
          successCount: success ? 1 : 0,
          failureCount: success ? 0 : 1,
          ts: resultPayload.processedAt,
        });
        enqueueDeviceState({
          deviceId: detail.device.id,
          status: success ? "up" : "down",
          successCountDelta: success ? 1 : 0,
          failureCountDelta: success ? 0 : 1,
          ts: resultPayload.processedAt,
          lastSuccessAt: success ? resultPayload.processedAt : undefined,
          lastFailureAt: success ? undefined : resultPayload.processedAt,
          lastPollAt: resultPayload.processedAt,
          lastError: success ? null : resultPayload.error ?? null,
          latencyMs: probe?.latencyMs ?? null,
        });
        recordAvailabilityPingResult(db, {
          deviceId: detail.device.id,
          state: success ? "up" : "down",
          ts: resultPayload.processedAt,
          latencyMs: probe?.latencyMs ?? null,
        });
        void publishAvailabilityUpdate(resolveCloudCfg(), {
          deviceId: String(detail.device.id),
          updatedAt: resultPayload.processedAt,
          revision: `${detail.device.id}:${resultPayload.processedAt}`,
        }).catch(() => {
          /* ignore */
        });

        currentJobIds.delete(job.id);
      }

      const invalids = details.filter((d) => !d.detail);
      for (const { job } of invalids) {
        abandonPollJob(db, {
          jobId: job.id,
          leaseOwner: workerCfg.workerName,
          result: {
            stub: true,
            workerType: "ping",
            error: "missing_context_after_claim",
            failedAt: new Date().toISOString(),
          },
        });
        currentJobIds.delete(job.id);
        failCount++;
      }

      log({
        level: "info",
        msg: "fping batch complete",
        workerName: workerCfg.workerName,
        claimed: jobs.length,
        succeeded: successCount,
        failed: failCount,
        timeoutMs,
      });
    } catch (err: any) {
      log({ level: "error", msg: "loop exception", error: err?.message ?? String(err), workerName: workerCfg.workerName });
      await delay(workerCfg.loopMs);
    }
  }

  if (heartbeatHandle) clearInterval(heartbeatHandle);
  await releaseInFlight("shutdown_exit");
}

async function main() {
  const workerCfg = buildWorkerConfig();
  const config = loadConfig();
  const db = initDatabase(config);
  const saved = getAllAppConfig(db);
  startTelemetryQueue(() => {
    const latest = getAllAppConfig(db);
    return {
      ...config,
      xmonApiBase: latest["XMON_API_BASE"] || config.xmonApiBase,
      xmonCollectorId: latest["XMON_COLLECTOR_ID"] || config.xmonCollectorId,
      xmonApiKey: latest["XMON_API_KEY"] || config.xmonApiKey,
    };
  });
  const shuttingDown = { flag: false };

  const shutdown = async (signal: string) => {
    if (shuttingDown.flag) return;
    shuttingDown.flag = true;
    log({ level: "info", msg: "shutdown requested", signal, workerName: workerCfg.workerName });
    await delay(10); // allow loop to notice flag
    log({ level: "info", msg: "shutdown complete", signal, workerName: workerCfg.workerName });
    process.exit(0);
  };

  process.on("SIGINT", () => {
    shutdown("SIGINT").catch(() => process.exit(1));
  });
  process.on("SIGTERM", () => {
    shutdown("SIGTERM").catch(() => process.exit(1));
  });

  const resolveCloudCfg = () => {
    const latest = getAllAppConfig(db);
    return {
      ...config,
      xmonApiBase: latest["XMON_API_BASE"] || config.xmonApiBase,
      xmonCollectorId: latest["XMON_COLLECTOR_ID"] || config.xmonCollectorId,
      xmonApiKey: latest["XMON_API_KEY"] || config.xmonApiKey,
    };
  };

  await runLoop(db, workerCfg, shuttingDown, resolveCloudCfg);
}

main().catch((err) => {
  log({ level: "fatal", msg: "worker crashed", error: err?.message ?? String(err) });
  process.exit(1);
});

async function runPingBatch(
  targets: Array<{ jobId: number; ip: string }>,
  fpingPath: string,
  timeoutMs: number
): Promise<Map<string, { success: boolean; latencyMs: number | null; error?: string }>> {
  const result = new Map<string, { success: boolean; latencyMs: number | null; error?: string }>();
  if (!targets.length) return result;
  // Use -C (count) without -a so fping prints one line per host (including unreachable)
  const args = ["-C", "1", "-t", timeoutMs.toString(), ...targets.map((t) => t.ip)];
  try {
    const { stdout } = await execFileAsync(fpingPath, args, { timeout: timeoutMs + 1000, encoding: "utf8" });
    parseFpingOutput(stdout, result);
  } catch (err: any) {
    const stdout = err?.stdout as string | undefined;
    const stderr = err?.stderr as string | undefined;
    const combined = [stdout, stderr].filter(Boolean).join("\n");
    if (combined) {
      parseFpingOutput(combined, result);
    }
  }
  for (const t of targets) {
    if (!result.has(t.ip)) {
      result.set(t.ip, { success: false, latencyMs: null, error: "no_reply" });
    }
  }
  return result;
}

function parseFpingOutput(
  stdout: string,
  result: Map<string, { success: boolean; latencyMs: number | null; error?: string }>
) {
  const lines = stdout.trim().split("\n").filter((l) => l.trim().length);
  for (const line of lines) {
    const [ip, rest] = line.split(" : ").map((s) => s.trim());
    const latencyPart = rest?.split(" ").find((p) => p.length);
    const latencyMs = latencyPart && latencyPart !== "-" ? Number(latencyPart) : null;
    result.set(ip, { success: latencyMs !== null, latencyMs, error: latencyMs === null ? "timeout" : undefined });
  }
}
