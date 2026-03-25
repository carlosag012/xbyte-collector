import { setTimeout as delay } from "node:timers/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execFileAsync = promisify(execFile);
import { loadConfig } from "./config.js";
import {
  initDatabase,
  upsertWorkerRegistration,
  heartbeatWorkerRegistration,
  claimNextPendingPollJobForWorkerCapabilities,
  finishPollJob,
  abandonPollJob,
  unclaimPollJobById,
  getWorkerRegistrationByName,
  getRunningPollJobDetailForLeaseOwner,
  type DB,
} from "./db.js";

type WorkerConfig = {
  workerName: string;
  heartbeatMs: number;
  loopMs: number;
  stubDelayMs: number;
};

function buildWorkerConfig(): WorkerConfig {
  const cfg = loadConfig();
  return {
    workerName: cfg.snmpWorkerName?.trim() || "snmp-worker-local",
    heartbeatMs: cfg.snmpWorkerHeartbeatMs ?? 15_000,
    loopMs: cfg.snmpWorkerLoopMs ?? 5_000,
    stubDelayMs: cfg.snmpWorkerStubDelayMs ?? 250,
  };
}

function log(data: Record<string, any>) {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      workerType: "snmp",
      ...data,
    })
  );
}

async function runLoop(db: DB, workerCfg: WorkerConfig, shuttingDown: { flag: boolean }) {
  let currentJobId: number | null = null;
  let heartbeatHandle: NodeJS.Timeout | null = null;

  async function releaseInFlight(reason: string) {
    if (currentJobId === null) return;
    try {
      const released = unclaimPollJobById(db, { jobId: currentJobId, leaseOwner: workerCfg.workerName });
      log({
        level: "info",
        msg: "released in-flight job on shutdown",
        jobId: currentJobId,
        released: Boolean(released),
        reason,
        workerName: workerCfg.workerName,
      });
    } catch (err: any) {
      log({
        level: "error",
        msg: "failed to release in-flight job on shutdown",
        jobId: currentJobId,
        reason,
        error: err?.message ?? String(err),
        workerName: workerCfg.workerName,
      });
    } finally {
      currentJobId = null;
    }
  }

  upsertWorkerRegistration(db, {
    workerType: "snmp",
    workerName: workerCfg.workerName,
    capabilities: { supportedKinds: ["snmp"] },
    enabled: true,
  });
  log({ level: "info", msg: "worker registered", workerName: workerCfg.workerName });

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
      const job = claimNextPendingPollJobForWorkerCapabilities(db, {
        workerName: workerCfg.workerName,
        supportedKinds: ["snmp"],
      });

      if (!job) {
        await delay(workerCfg.loopMs);
        continue;
      }

      currentJobId = job.id;
      const detail = getRunningPollJobDetailForLeaseOwner(db, workerCfg.workerName);

      if (!detail) {
        abandonPollJob(db, {
          jobId: job.id,
          leaseOwner: workerCfg.workerName,
          result: {
            stub: true,
            workerType: "snmp",
            error: "missing_context_after_claim",
            failedAt: new Date().toISOString(),
          },
        });
        currentJobId = null;
        await delay(workerCfg.loopMs);
        continue;
      }

      log({
        level: "info",
        msg: "claimed job",
        jobId: job.id,
        targetId: job.targetId,
        deviceId: detail.device.id,
        deviceHostname: detail.device.hostname,
        profileKind: detail.profile.kind,
        workerName: workerCfg.workerName,
      });

      try {
        await delay(workerCfg.stubDelayMs);
        if (shuttingDown.flag) {
          await releaseInFlight("shutdown_during_processing");
          break;
        }
        const probeTarget = detail.device.ipAddress;
        const profileConfig = (detail.profile?.config ?? {}) as any;
        const community = typeof profileConfig.community === "string" && profileConfig.community ? profileConfig.community : "public";
        const version = typeof profileConfig.version === "string" && profileConfig.version ? profileConfig.version : "2c";
        const oid = typeof profileConfig.oid === "string" && profileConfig.oid ? profileConfig.oid : "1.3.6.1.2.1.1.3.0";
        const timeoutMs = typeof detail.profile.timeoutMs === "number" ? detail.profile.timeoutMs : 2000;

        log({
          level: "info",
          msg: "snmp probe start",
          jobId: job.id,
          deviceHostname: detail.device.hostname,
          deviceIpAddress: probeTarget,
          oid,
          workerName: workerCfg.workerName,
        });

        const { success, value, error } = await runSnmpProbe(probeTarget, oid, community, version, timeoutMs);
        const status = success ? "completed" : "failed";
        const resultPayload = {
          stub: false,
          workerType: "snmp",
          success,
          target: probeTarget,
          oid,
          value: success ? value ?? null : undefined,
          error: success ? undefined : error ?? "snmp_failed",
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

        const finished = finishPollJob(db, {
          jobId: job.id,
          status,
          result: resultPayload,
        });
        if (finished && !shuttingDown.flag) {
          log({
            level: success ? "info" : "warn",
            msg: success ? "snmp probe success" : "snmp probe failed",
            jobId: finished.id,
            status: finished.status,
            oid,
            workerName: workerCfg.workerName,
            error: success ? undefined : error ?? "snmp_failed",
          });
        }
        currentJobId = null;
      } catch (err: any) {
        const abandoned = abandonPollJob(db, {
          jobId: job.id,
          leaseOwner: workerCfg.workerName,
          result: {
            stub: true,
            workerType: "snmp",
            error: err?.message ?? String(err),
            failedAt: new Date().toISOString(),
          },
        });
        log({
          level: "error",
          msg: "job abandon due to error",
          jobId: job.id,
          status: abandoned?.status ?? "failed",
          workerName: workerCfg.workerName,
          error: err?.message ?? String(err),
        });
        currentJobId = null;
      }
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
  const shuttingDown = { flag: false };

  const shutdown = async (signal: string) => {
    if (shuttingDown.flag) return;
    shuttingDown.flag = true;
    log({ level: "info", msg: "shutdown requested", signal, workerName: workerCfg.workerName });
    await delay(10);
    log({ level: "info", msg: "shutdown complete", signal, workerName: workerCfg.workerName });
    process.exit(0);
  };

  process.on("SIGINT", () => {
    shutdown("SIGINT").catch(() => process.exit(1));
  });
  process.on("SIGTERM", () => {
    shutdown("SIGTERM").catch(() => process.exit(1));
  });

  await runLoop(db, workerCfg, shuttingDown);
}

main().catch((err) => {
  log({ level: "fatal", msg: "worker crashed", error: err?.message ?? String(err) });
  process.exit(1);
});

async function runSnmpProbe(
  target: string,
  oid: string,
  community: string,
  version: string,
  timeoutMs: number
): Promise<{ success: boolean; value?: string | null; error?: string }> {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const args = ["-v", version, "-c", community, "-t", timeoutSec.toString(), "-r", "1", target, oid];
  try {
    const { stdout } = await execFileAsync("snmpget", args, { timeout: timeoutMs + 500, encoding: "utf8" });
    const line = stdout.trim().split("\n").pop() ?? "";
    const parts = line.split("=");
    const value = parts.length > 1 ? parts.slice(1).join("=").trim() : line;
    return { success: true, value };
  } catch (err: any) {
    return { success: false, error: err?.message ?? "snmp_failed" };
  }
}
