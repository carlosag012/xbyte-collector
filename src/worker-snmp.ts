import { setTimeout as delay } from "node:timers/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
const execFileAsync = promisify(execFile);
import { loadConfig } from "./config.js";
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
  saveSnmpSystemSnapshot,
  replaceInterfaceSnapshotsForDevice,
  replaceLldpNeighborsForDevice,
  upsertDiscoveredDeviceCandidatesFromLldp,
  updateDeviceIdentity,
  licenseAllowsCollection,
  getAllAppConfig,
  getLastInterfaceCounters,
  upsertLastInterfaceCounters,
  type DB,
} from "./db.js";
import {
  enqueueTelemetry,
  enqueueDeviceSnapshot,
  enqueueDeviceState,
  enqueueInterfaceSnapshot,
  enqueueSnmpSystemSnapshot,
  enqueueLldpNeighbors,
  startTelemetryQueue,
  flushTelemetryNow,
} from "./telemetry-queue.js";
import { publishDeviceIdentityUpdate } from "./xmon-client.js";

type WorkerConfig = {
  workerName: string;
  heartbeatMs: number;
  loopMs: number;
  stubDelayMs: number;
  batchSize: number;
  concurrency: number;
  snmpWalkPath: string;
  snmpGetPath: string;
};

type SnmpWalkFailure = {
  phase: "interfaces" | "lldp";
  oid: string;
  reason: string;
};

type InterfacesWalkOutcome = {
  interfaces: any[];
  attemptedOids: string[];
  successfulOids: string[];
  failures: SnmpWalkFailure[];
};

type LldpWalkOutcome = {
  neighbors: any[];
  attemptedOids: string[];
  successfulOids: string[];
  successfulKeys: string[];
  failures: SnmpWalkFailure[];
};

type SnmpCollectionDiagnostics = {
  degraded: boolean;
  interfaces: {
    attemptedOids: string[];
    successfulOids: string[];
    failedOids: string[];
    failures: SnmpWalkFailure[];
    replaceAllowed: boolean;
  };
  lldp: {
    attemptedOids: string[];
    successfulOids: string[];
    failedOids: string[];
    failures: SnmpWalkFailure[];
    replaceAllowed: boolean;
  };
};

type SnmpPersistenceInfo = {
  interfaces: {
    replaceApplied: boolean;
    preservedPreviousState: boolean;
    collectedCount: number;
  };
  lldp: {
    replaceApplied: boolean;
    preservedPreviousState: boolean;
    collectedCount: number;
  };
};

type SnmpJobResult = {
  success: boolean;
  summary: any;
  discovery: any;
  error?: string;
  warnings?: string[];
  collection?: SnmpCollectionDiagnostics;
};

type PersistDiscoveryOptions = {
  allowInterfaceReplace?: boolean;
  allowLldpReplace?: boolean;
};

type PersistDiscoveryResult = {
  identity: { assetTag: string | null; serialNumber: string | null } | null;
  persistence: SnmpPersistenceInfo;
};

function buildWorkerConfig(): WorkerConfig {
  const cfg = loadConfig();
  return {
    workerName: cfg.snmpWorkerName?.trim() || "snmp-worker-local",
    heartbeatMs: cfg.snmpWorkerHeartbeatMs ?? 15_000,
    loopMs: cfg.snmpWorkerLoopMs ?? 5_000,
    stubDelayMs: cfg.snmpWorkerStubDelayMs ?? 250,
    batchSize: cfg.snmpWorkerBatchSize ?? 50,
    concurrency: cfg.snmpWorkerConcurrency ?? 16,
    snmpWalkPath: cfg.snmpWalkPath ?? "snmpwalk",
    snmpGetPath: cfg.snmpGetPath ?? "snmpget",
  };
}

const SNMP_TASK_TIMEOUT_MS = 60_000; // hard cap per SNMP job to avoid stuck running rows

function log(data: Record<string, any>) {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      workerType: "snmp",
      ...data,
    })
  );
}

function compactErrorReason(err: unknown): string {
  const raw = String((err as any)?.message ?? err ?? "unknown_error");
  return raw.replace(/\s+/g, " ").trim().slice(0, 280);
}

async function runLoop(
  db: DB,
  workerCfg: WorkerConfig,
  shuttingDown: { flag: boolean },
  baseCfg: ReturnType<typeof loadConfig>
) {
  let currentJobIds = new Set<number>();
  let heartbeatHandle: NodeJS.Timeout | null = null;

  async function releaseInFlight(reason: string) {
    if (!currentJobIds.size) return;
    for (const jobId of Array.from(currentJobIds)) {
      try {
        const released = unclaimPollJobById(db, { jobId, leaseOwner: workerCfg.workerName });
        log({ level: "info", msg: "released in-flight job on shutdown", jobId, released: Boolean(released), reason, workerName: workerCfg.workerName });
      } catch (err: any) {
        log({ level: "error", msg: "failed to release in-flight job on shutdown", jobId, reason, error: err?.message ?? String(err), workerName: workerCfg.workerName });
      }
    }
    currentJobIds.clear();
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
        supportedKinds: ["snmp"],
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
      const invalids = details.filter((d) => !d.detail);

      let successCount = 0;
      let failCount = invalids.length;

      for (const { job } of invalids) {
        abandonPollJob(db, {
          jobId: job.id,
          leaseOwner: workerCfg.workerName,
          result: { stub: true, workerType: "snmp", error: "missing_context_after_claim", failedAt: new Date().toISOString() },
        });
        currentJobIds.delete(job.id);
      }

      const lic = licenseAllowsCollection(db);
      if (!lic.allowed) {
        for (const { job, detail } of validDetails) {
          finishPollJob(db, {
            jobId: job.id,
            status: "failed",
            result: {
              workerType: "snmp",
              success: false,
              blockedByLicense: true,
              code: lic.reason ?? "license_required",
              effectiveStatus: lic.effectiveStatus,
              message: `Collection blocked: ${lic.reason ?? "license_required"}`,
              processedAt: new Date().toISOString(),
              summary: { system: null, interfacesCount: 0, lldpNeighborsCount: 0 },
              discovery: { system: null, interfaces: [], lldpNeighbors: [] },
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
          failCount++;
        }
        log({
          level: "warn",
          msg: "snmp batch blocked by license",
          workerName: workerCfg.workerName,
          effectiveStatus: lic.effectiveStatus,
          reason: lic.reason,
          blocked: validDetails.length,
          claimed: jobs.length,
        });
        continue;
      }

      const tasks = validDetails.map(({ job, detail }) => async () => {
        try {
          await withTimeout(
            (async () => {
              const licExec = licenseAllowsCollection(db);
              if (!licExec.allowed) {
                finishPollJob(db, {
                  jobId: job.id,
                  status: "failed",
                  result: {
                    workerType: "snmp",
                    success: false,
                    blockedByLicense: true,
                    code: licExec.reason ?? "license_required",
                    effectiveStatus: licExec.effectiveStatus,
                    message: `Collection blocked: ${licExec.reason ?? "license_required"}`,
                    processedAt: new Date().toISOString(),
                    summary: { system: null, interfacesCount: 0, lldpNeighborsCount: 0 },
                    discovery: { system: null, interfaces: [], lldpNeighbors: [] },
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
                failCount++;
                log({
                  level: "warn",
                  msg: "snmp execution blocked by license",
                  workerName: workerCfg.workerName,
                  jobId: detail.job.id,
                  effectiveStatus: licExec.effectiveStatus,
                  reason: licExec.reason,
                });
                return;
              }

              log({
                level: "info",
                msg: "snmp discovery start",
                workerName: workerCfg.workerName,
                jobId: detail.job.id,
                targetId: detail.target.id,
                deviceId: detail.device.id,
                deviceHostname: detail.device.hostname,
                deviceIpAddress: detail.device.ipAddress,
              });

              const res = await processSnmpJob(detail, workerCfg);
              const processedAt = new Date().toISOString();
              let persistedIdentity: { assetTag: string | null; serialNumber: string | null } | null = null;
              let persistenceInfo: SnmpPersistenceInfo | null = null;

              if (res.discovery?.system) {
                try {
                  const persisted = persistDiscoveryNormalization(db, detail.device.id, res.discovery, processedAt, {
                    allowInterfaceReplace: res.collection?.interfaces?.replaceAllowed ?? true,
                    allowLldpReplace: res.collection?.lldp?.replaceAllowed ?? true,
                  });
                  persistedIdentity = persisted.identity;
                  persistenceInfo = persisted.persistence;
                } catch (err: any) {
                  const reason = compactErrorReason(err);
                  res.success = false;
                  if (res.collection) res.collection.degraded = true;
                  res.warnings = [...(res.warnings ?? []), `persistence_failed phase=persist reason=${reason}`];
                  res.error = res.error ? `${res.error}; ${reason}` : reason;
                }
              }

              if (!persistenceInfo) {
                persistenceInfo = {
                  interfaces: {
                    replaceApplied: Boolean(res.collection?.interfaces?.replaceAllowed),
                    preservedPreviousState: Boolean(res.collection && !res.collection.interfaces.replaceAllowed),
                    collectedCount: Number(res.summary?.interfacesCount ?? 0),
                  },
                  lldp: {
                    replaceApplied: Boolean(res.collection?.lldp?.replaceAllowed),
                    preservedPreviousState: Boolean(res.collection && !res.collection.lldp.replaceAllowed),
                    collectedCount: Number(res.summary?.lldpNeighborsCount ?? 0),
                  },
                };
              }

              const status: "completed" | "failed" = res.success ? "completed" : "failed";

              finishPollJob(db, {
                jobId: job.id,
                status,
            result: {
              stub: false,
              workerType: "snmp",
              success: res.success,
              target: detail.device.ipAddress,
              processedAt,
              summary: res.summary,
              discovery: res.discovery,
              error: res.error,
              warnings: res.warnings,
              collection: res.collection,
              persistence: persistenceInfo,
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
              if (res.success) successCount++;
              else failCount++;

              // emit snapshot
              enqueueDeviceSnapshot({
                deviceId: detail.device.id,
                name: detail.device.hostname,
                ip: detail.device.ipAddress,
                deviceType: detail.device.type ?? detail.device.kind ?? undefined,
                assetTag: persistedIdentity?.assetTag ?? detail.device.assetTag ?? null,
                serialNumber: persistedIdentity?.serialNumber ?? detail.device.serialNumber ?? null,
                status: res.success ? "up" : "down",
                snmpProfileId: detail.profile?.id ? String(detail.profile.id) : null,
                snmpPollerIds: detail.profile?.id ? [`poller-${detail.profile.id}`] : undefined,
                successCount: res.success ? 1 : 0,
                failureCount: res.success ? 0 : 1,
                ts: processedAt,
              });
              if (persistedIdentity) {
                const latest = getAllAppConfig(db);
                void publishDeviceIdentityUpdate(
                  {
                    ...baseCfg,
                    xmonApiBase: latest["XMON_API_BASE"] || baseCfg.xmonApiBase,
                    xmonCollectorId: latest["XMON_COLLECTOR_ID"] || baseCfg.xmonCollectorId,
                    xmonApiKey: latest["XMON_API_KEY"] || baseCfg.xmonApiKey,
                  },
                  {
                    deviceId: String(detail.device.id),
                    updatedAt: processedAt,
                    assetTag: persistedIdentity.assetTag,
                    serialNumber: persistedIdentity.serialNumber,
                  },
                ).catch(() => {
                  /* ignore */
                });
              }
          enqueueDeviceState({
            deviceId: detail.device.id,
            status: res.success ? "up" : "unknown", // ping is authoritative; avoid flipping down on SNMP-only failures
            successCountDelta: res.success ? 1 : 0,
            failureCountDelta: res.success ? 0 : 1,
            ts: processedAt,
            lastSuccessAt: res.success ? processedAt : undefined,
            lastFailureAt: res.success ? undefined : processedAt,
            lastPollAt: processedAt,
            lastError: res.success ? null : res.error ?? null,
          });

              log({
                level: res.success ? "info" : "warn",
                msg: res.success ? "snmp discovery success" : "snmp discovery failed",
                workerName: workerCfg.workerName,
                jobId: detail.job.id,
                deviceHostname: detail.device.hostname,
                deviceIpAddress: detail.device.ipAddress,
                interfacesCount: res.summary.interfacesCount,
                lldpNeighborsCount: res.summary.lldpNeighborsCount,
                error: res.error,
              });
            })(),
            SNMP_TASK_TIMEOUT_MS,
            "snmp_task_timeout"
          );
        } catch (err: any) {
          currentJobIds.delete(job.id);
          finishPollJob(db, {
            jobId: job.id,
            status: "failed",
            result: {
              stub: false,
              workerType: "snmp",
              success: false,
              target: detail.device.ipAddress,
              processedAt: new Date().toISOString(),
              summary: { interfacesCount: 0, lldpNeighborsCount: 0 },
              discovery: { system: null, interfaces: [], lldpNeighbors: [] },
              error: err?.message ?? "snmp_task_exception",
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
          failCount++;
          log({
            level: "error",
            msg: "snmp task exception",
            workerName: workerCfg.workerName,
            jobId: detail.job.id,
            error: err?.message ?? String(err),
          });
        }
      });

      log({
        level: "info",
        msg: "snmp batch start",
        workerName: workerCfg.workerName,
        count: validDetails.length,
        concurrency: workerCfg.concurrency,
      });

      await runWithConcurrency(tasks, workerCfg.concurrency, shuttingDown);

      log({
        level: "info",
        msg: "snmp batch complete",
        workerName: workerCfg.workerName,
        claimed: jobs.length,
        succeeded: successCount,
        failed: failCount,
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

  await runLoop(db, workerCfg, shuttingDown, config);
}

main().catch((err) => {
  log({ level: "fatal", msg: "worker crashed", error: err?.message ?? String(err) });
  process.exit(1);
});

async function processSnmpJob(detail: any, workerCfg: WorkerConfig): Promise<SnmpJobResult> {
  const target = detail.device.ipAddress;
  const profileConfig = (detail.profile?.config ?? {}) as any;
  const timeoutMs = typeof detail.profile.timeoutMs === "number" ? detail.profile.timeoutMs : 2000;

  try {
    // Core reachability: system fetch must succeed
    const system = await snmpGetSystem(workerCfg.snmpGetPath, target, profileConfig, timeoutMs);
    const serialNumber = await snmpGetSerialNumber(workerCfg.snmpWalkPath, target, profileConfig, timeoutMs).catch(() => null);
    if (serialNumber) {
      system.serialNumber = serialNumber;
    }

    const interfacesOutcome = await snmpWalkInterfaces(workerCfg.snmpWalkPath, target, profileConfig, timeoutMs);
    const lldpOutcome = await snmpWalkLldp(workerCfg.snmpWalkPath, target, profileConfig, timeoutMs);

    const interfaces = interfacesOutcome.interfaces;
    const lldpNeighbors = lldpOutcome.neighbors;
    const interfacesReplaceAllowed = interfacesOutcome.failures.length === 0 || interfaces.length > 0;
    const lldpHasLocalPortTree = lldpOutcome.successfulKeys.includes("localPort");
    const lldpHasRemoteIdentityTree = lldpOutcome.successfulKeys.some((k) => k !== "localPort");
    const lldpReplaceAllowed =
      lldpOutcome.failures.length === 0 ||
      lldpNeighbors.length > 0 ||
      (lldpHasLocalPortTree && lldpHasRemoteIdentityTree);
    const degraded = !interfacesReplaceAllowed || !lldpReplaceAllowed;
    const warnings: string[] = [];
    for (const f of interfacesOutcome.failures) {
      warnings.push(`walk_failed phase=${f.phase} oid=${f.oid} reason=${f.reason}`);
    }
    for (const f of lldpOutcome.failures) {
      warnings.push(`walk_failed phase=${f.phase} oid=${f.oid} reason=${f.reason}`);
    }
    if (!interfacesReplaceAllowed) {
      const failed = interfacesOutcome.failures.map((f) => f.oid).join(",") || "none";
      warnings.push(`collection_degraded phase=interfaces action=preserve_previous_state reason=no_usable_data failed_oids=${failed}`);
    }
    if (!lldpReplaceAllowed) {
      const failed = lldpOutcome.failures.map((f) => f.oid).join(",") || "none";
      warnings.push(`collection_degraded phase=lldp action=preserve_previous_state reason=no_usable_data failed_oids=${failed}`);
    }

    const interfacesFailed = interfacesOutcome.failures.map((f) => f.oid).join(",") || "none";
    const lldpFailed = lldpOutcome.failures.map((f) => f.oid).join(",") || "none";
    const error = degraded
      ? `material_collection_failure interfaces_replace_allowed=${interfacesReplaceAllowed} lldp_replace_allowed=${lldpReplaceAllowed} interfaces_failed_oids=${interfacesFailed} lldp_failed_oids=${lldpFailed}`
      : undefined;

    return {
      success: !degraded,
      summary: {
        system,
        interfacesCount: interfaces.length,
        lldpNeighborsCount: lldpNeighbors.length,
        degraded,
      },
      discovery: {
        system,
        interfaces,
        lldpNeighbors,
      },
      error,
      warnings: warnings.length ? warnings : undefined,
      collection: {
        degraded,
        interfaces: {
          attemptedOids: interfacesOutcome.attemptedOids,
          successfulOids: interfacesOutcome.successfulOids,
          failedOids: interfacesOutcome.failures.map((f) => f.oid),
          failures: interfacesOutcome.failures,
          replaceAllowed: interfacesReplaceAllowed,
        },
        lldp: {
          attemptedOids: lldpOutcome.attemptedOids,
          successfulOids: lldpOutcome.successfulOids,
          failedOids: lldpOutcome.failures.map((f) => f.oid),
          failures: lldpOutcome.failures,
          replaceAllowed: lldpReplaceAllowed,
        },
      },
    };
  } catch (err: any) {
    return {
      success: false,
      summary: { system: null, interfacesCount: 0, lldpNeighborsCount: 0 },
      discovery: { system: null, interfaces: [], lldpNeighbors: [] },
      error: err?.message ?? "snmp_failed",
    };
  }
}

async function snmpGetSystem(snmpGetPath: string, target: string, profileConfig: any, timeoutMs: number) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const oids = ["1.3.6.1.2.1.1.5.0", "1.3.6.1.2.1.1.1.0", "1.3.6.1.2.1.1.2.0", "1.3.6.1.2.1.1.3.0"];
  const args = [...buildSnmpBaseArgs(profileConfig, target, timeoutSec), ...oids];
  const { stdout } = await execFileAsync(snmpGetPath, args, { timeout: timeoutMs + 500, encoding: "utf8" });
  const parsed = parseSnmpGet(stdout);
  return {
    sysName: parsed["1.3.6.1.2.1.1.5.0"] ?? null,
    sysDescr: parsed["1.3.6.1.2.1.1.1.0"] ?? null,
    sysObjectId: parsed["1.3.6.1.2.1.1.2.0"] ?? null,
    sysUpTime: parsed["1.3.6.1.2.1.1.3.0"] ?? null,
    serialNumber: null as string | null,
  };
}

async function snmpGetSerialNumber(snmpWalkPath: string, target: string, profileConfig: any, timeoutMs: number) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const entPhysicalSerialNum = "1.3.6.1.2.1.47.1.1.1.1.11";
  const args = [...buildSnmpBaseArgs(profileConfig, target, timeoutSec), entPhysicalSerialNum];
  const { stdout } = await execFileAsync(snmpWalkPath, args, { timeout: timeoutMs + 1500, encoding: "utf8" });
  return parseSnmpSerial(stdout);
}

async function snmpWalkInterfaces(snmpWalkPath: string, target: string, profileConfig: any, timeoutMs: number): Promise<InterfacesWalkOutcome> {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const attemptedOids = [
    "1.3.6.1.2.1.31.1.1.1.1",
    "1.3.6.1.2.1.2.2.1.2",
    "1.3.6.1.2.1.31.1.1.1.18",
    "1.3.6.1.2.1.2.2.1.7",
    "1.3.6.1.2.1.2.2.1.8",
    "1.3.6.1.2.1.2.2.1.5",
    "1.3.6.1.2.1.2.2.1.3",
    "1.3.6.1.2.1.31.1.1.1.15",
    "1.3.6.1.2.1.2.2.1.4",
    "1.3.6.1.2.1.2.2.1.6",
    "1.3.6.1.2.1.31.1.1.1.6", // ifHCInOctets
    "1.3.6.1.2.1.31.1.1.1.10", // ifHCOutOctets
    "1.3.6.1.2.1.2.2.1.10", // ifInOctets (fallback)
    "1.3.6.1.2.1.2.2.1.16", // ifOutOctets (fallback)
  ];
  const results: string[] = [];
  const successfulOids: string[] = [];
  const failures: SnmpWalkFailure[] = [];
  for (const oid of attemptedOids) {
    try {
      const args = [...buildSnmpBaseArgs(profileConfig, target, timeoutSec), oid];
      const { stdout } = await execFileAsync(snmpWalkPath, args, { timeout: timeoutMs + 1500, encoding: "utf8" });
      results.push(stdout);
      successfulOids.push(oid);
    } catch (err) {
      failures.push({
        phase: "interfaces",
        oid,
        reason: compactErrorReason(err),
      });
    }
  }
  const interfaces = results.length ? parseInterfaces(results.join("\n")) : [];
  return {
    interfaces,
    attemptedOids,
    successfulOids,
    failures,
  };
}

async function snmpWalkLldp(snmpWalkPath: string, target: string, profileConfig: any, timeoutMs: number): Promise<LldpWalkOutcome> {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const trees = {
    remoteChassisId: "1.0.8802.1.1.2.1.4.1.1.5",
    remotePortId: "1.0.8802.1.1.2.1.4.1.1.7",
    remoteSysName: "1.0.8802.1.1.2.1.4.1.1.9",
    remoteSysDesc: "1.0.8802.1.1.2.1.4.1.1.10",
    remoteMgmtIp: "1.0.8802.1.1.2.1.4.2.1.4",
    localPort: "1.0.8802.1.1.2.1.3.7.1.3",
  };
  const attemptedOids = Object.values(trees);
  const outputs: Record<string, string> = {};
  const successfulOids: string[] = [];
  const successfulKeys: string[] = [];
  const failures: SnmpWalkFailure[] = [];
  for (const [key, oid] of Object.entries(trees)) {
    try {
      const args = [...buildSnmpBaseArgs(profileConfig, target, timeoutSec), oid];
      const { stdout } = await execFileAsync(snmpWalkPath, args, { timeout: timeoutMs + 1500, encoding: "utf8" });
      outputs[key] = stdout;
      successfulOids.push(oid);
      successfulKeys.push(key);
    } catch (err) {
      failures.push({
        phase: "lldp",
        oid,
        reason: compactErrorReason(err),
      });
    }
  }
  const neighbors = successfulKeys.length ? parseLldp(outputs) : [];
  return {
    neighbors,
    attemptedOids,
    successfulOids,
    successfulKeys,
    failures,
  };
}

function buildSnmpBaseArgs(profileConfig: any, target: string, timeoutSec: number): string[] {
  const version = (profileConfig?.version as string) || "2c";
  if (version === "3") {
    const username = profileConfig?.username || profileConfig?.user;
    if (!username) throw new Error("snmpv3_missing_username");
    const securityLevel = profileConfig?.securityLevel || "noAuthNoPriv";
    const args = ["-v", "3", "-l", securityLevel, "-u", username];
    const normAuth = normalizeAuthProtocol(profileConfig?.authProtocol);
    const normPriv = normalizePrivProtocol(profileConfig?.privProtocol);
    if (normAuth && profileConfig?.authPassword) {
      args.push("-a", normAuth, "-A", profileConfig.authPassword);
    }
    if (normPriv && profileConfig?.privPassword) {
      args.push("-x", normPriv, "-X", profileConfig.privPassword);
    }
    args.push("-On", "-t", timeoutSec.toString(), "-r", "1", target);
    return args;
  }
  const community = profileConfig?.community || "public";
  const versionClean = version || "2c";
  return ["-v", versionClean, "-c", community, "-On", "-t", timeoutSec.toString(), "-r", "1", target];
}

function parseSnmpGet(stdout: string): Record<string, string> {
  const lines = stdout.trim().split("\n");
  const out: Record<string, string> = {};
  for (const line of lines) {
    const [oidPartRaw, restRaw] = line.split(" = ").map((s) => s.trim());
    if (!oidPartRaw || !restRaw) continue;
    const oidPart = normalizeOidNumeric(oidPartRaw).split(" ")[0];
    const value = snmpValueToString(restRaw);
    out[oidPart] = value;
  }
  return out;
}

function parseSnmpSerial(stdout: string): string | null {
  const lines = stdout
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  for (const line of lines) {
    const [, restRaw] = line.split(" = ").map((s) => s.trim());
    if (!restRaw) continue;
    const value = snmpValueToString(restRaw).trim();
    if (!value) continue;
    const lower = value.toLowerCase();
    if (
      lower === "unknown" ||
      lower === "none" ||
      lower === "n/a" ||
      lower === "not set" ||
      lower.includes("no such object") ||
      lower.includes("no such instance") ||
      lower.includes("unknown object identifier")
    ) {
      continue;
    }
    return value;
  }
  return null;
}

function parseInterfaces(stdout: string) {
  const lines = stdout.trim().split("\n").filter((l) => l.trim().length);
  const map = new Map<number, any>();
  const prefixes = {
    ifName: "1.3.6.1.2.1.31.1.1.1.1",
    ifDescr: "1.3.6.1.2.1.2.2.1.2",
    ifAlias: "1.3.6.1.2.1.31.1.1.1.18",
    ifType: "1.3.6.1.2.1.2.2.1.3",
    adminStatus: "1.3.6.1.2.1.2.2.1.7",
    operStatus: "1.3.6.1.2.1.2.2.1.8",
    ifSpeed: "1.3.6.1.2.1.2.2.1.5",
    ifHighSpeed: "1.3.6.1.2.1.31.1.1.1.15",
    ifMtu: "1.3.6.1.2.1.2.2.1.4",
    ifPhysAddress: "1.3.6.1.2.1.2.2.1.6",
    ifHCInOctets: "1.3.6.1.2.1.31.1.1.1.6",
    ifHCOutOctets: "1.3.6.1.2.1.31.1.1.1.10",
    ifInOctets: "1.3.6.1.2.1.2.2.1.10",
    ifOutOctets: "1.3.6.1.2.1.2.2.1.16",
  };
  for (const line of lines) {
    const [oidPart, rest] = line.split(" = ").map((s) => s.trim());
    if (!oidPart || !rest) continue;
    const oidFull = normalizeOidNumeric(oidPart);
    const val = snmpValueToString(rest);
    const matchKey = Object.entries(prefixes).find(([_, prefix]) => oidFull.startsWith(prefix + "."));
    if (!matchKey) continue;
    const [key, prefix] = matchKey;
    const idxStr = oidFull.slice(prefix.length + 1);
    const idx = Number(idxStr);
    if (!Number.isFinite(idx)) continue;
    const rec = map.get(idx) ?? {};
    if (key === "ifName") rec.ifName = val;
    else if (key === "ifDescr") rec.ifDescr = val;
    else if (key === "ifAlias") rec.ifAlias = val;
    else if (key === "ifType") rec.ifType = Number(val) || null;
    else if (key === "adminStatus") rec.adminStatus = val;
    else if (key === "operStatus") rec.operStatus = val;
    else if (key === "ifSpeed") rec.ifSpeed = Number(val) || null;
    else if (key === "ifHighSpeed") rec.ifHighSpeed = Number(val) || null;
    else if (key === "ifMtu") rec.ifMtu = Number(val) || null;
    else if (key === "ifPhysAddress") rec.ifPhysAddress = val;
    else if (key === "ifHCInOctets") rec.ifHCInOctets = Number(val);
    else if (key === "ifHCOutOctets") rec.ifHCOutOctets = Number(val);
    else if (key === "ifInOctets") rec.ifInOctets = Number(val);
    else if (key === "ifOutOctets") rec.ifOutOctets = Number(val);
    map.set(idx, rec);
  }
  return Array.from(map.entries()).map(([ifIndex, rec]) => ({
    ifIndex,
    ifName: rec.ifName ?? null,
    ifDescr: rec.ifDescr ?? null,
    ifAlias: rec.ifAlias ?? null,
    ifType: rec.ifType ?? null,
    adminStatus: rec.adminStatus ?? null,
    operStatus: rec.operStatus ?? null,
    speed: rec.ifHighSpeed ?? rec.ifSpeed ?? null,
    mtu: rec.ifMtu ?? null,
    mac: rec.ifPhysAddress ?? null,
    hcInOctets: rec.ifHCInOctets ?? null,
    hcOutOctets: rec.ifHCOutOctets ?? null,
    inOctets: rec.ifInOctets ?? null,
    outOctets: rec.ifOutOctets ?? null,
  }));
}

function parseLldp(outputs: Record<string, string>) {
  const keyToPrefixMap: Record<string, string> = {
    remoteChassisId: "1.0.8802.1.1.2.1.4.1.1.5",
    remotePortId: "1.0.8802.1.1.2.1.4.1.1.7",
    remoteSysName: "1.0.8802.1.1.2.1.4.1.1.9",
    remoteSysDesc: "1.0.8802.1.1.2.1.4.1.1.10",
    remoteMgmtIp: "1.0.8802.1.1.2.1.4.2.1.4",
    localPort: "1.0.8802.1.1.2.1.3.7.1.3",
  };

  const remMap = new Map<string, any>();

  const parseTree = (data: string, key: string, prefix: string, sink: Map<string, any>) => {
    const lines = data.trim().split("\n").filter((l) => l.trim().length);
    for (const line of lines) {
      const [oidPart, rest] = line.split(" = ").map((s) => s.trim());
      if (!oidPart || !rest) continue;
      const oidFull = normalizeOidNumeric(oidPart);
      if (!oidFull.startsWith(prefix + ".")) continue;
      const suffix = oidFull.slice(prefix.length + 1);
      let val: string | null = snmpValueToString(rest);
      let tuple = suffix.split(".").slice(0, 3).join(".");
      if (key === "remoteMgmtIp") {
        const parts = suffix.split(".");
        const addrOctets = parts.slice(-4).map((p) => Number(p));
        if (addrOctets.length === 4 && addrOctets.every((n) => Number.isFinite(n))) {
          val = addrOctets.join(".");
        } else {
          val = null;
        }
      }
      if (key === "localPort" && val !== null) {
        // keep string as-is; tuple for localPort may be shorter, so use tuple of suffix without truncation if needed
        const locSuffixParts = suffix.split(".");
        tuple = locSuffixParts.slice(0, 3).join(".");
      }
      const rec = sink.get(tuple) ?? {};
      if (val !== null) {
        rec[key] = val;
        sink.set(tuple, rec);
      }
    }
  };

  for (const [k, p] of Object.entries(keyToPrefixMap)) {
    parseTree(outputs[k] ?? "", k, p, remMap);
  }

  const neighbors: any[] = [];
  for (const [, rec] of remMap.entries()) {
    if (!rec.localPort) continue; // required for ingest
    if (!rec.remoteSysName && !rec.remotePortId && !rec.remoteChassisId && !rec.remoteMgmtIp) continue;
    neighbors.push({
      localPort: rec.localPort ?? null,
      remoteSysName: rec.remoteSysName ?? null,
      remotePortId: rec.remotePortId ?? null,
      remoteChassisId: rec.remoteChassisId ?? null,
      remoteMgmtIp: rec.remoteMgmtIp ?? null,
    });
  }
  return neighbors;
}

function normalizeOidNumeric(oidPart: string): string {
  return oidPart.replace(/^iso\./i, "").replace(/^\./, "").replace(/^SNMPv2-SMI::/, "");
}

function snmpValueToString(raw: string): string {
  const parts = raw.split(":");
  if (parts.length < 2) return raw.trim();
  const [, ...rest] = parts;
  let val = rest.join(":").trim();
  if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
    val = val.slice(1, -1);
  }
  if (val.startsWith("(") && val.includes(")") && /\d/.test(val[1])) {
    const inside = val.slice(1, val.indexOf(")"));
    const after = val.slice(val.indexOf(")") + 1).trim();
    val = after ? `${inside} ${after}` : inside;
  }
  return val;
}

function normalizeAuthProtocol(proto: any): string | null {
  if (!proto || typeof proto !== "string") return null;
  let p = proto.replace(/-/g, "").toUpperCase();
  if (p === "SHA1") p = "SHA";
  if (p === "SHA") return "SHA";
  if (p === "MD5") return "MD5";
  if (p === "SHA224") return "SHA224";
  if (p === "SHA256") return "SHA256";
  if (p === "SHA384") return "SHA384";
  if (p === "SHA512") return "SHA512";
  return null;
}

function normalizePrivProtocol(proto: any): string | null {
  if (!proto || typeof proto !== "string") return null;
  let p = proto.replace(/-/g, "").toUpperCase();
  if (p === "AES" || p === "AES128" || p === "AES128C") return "AES";
  if (p === "AES192") return "AES192";
  if (p === "AES192C") return "AES192C";
  if (p === "AES256") return "AES256";
  if (p === "AES256C") return "AES256C";
  if (p === "DES") return "DES";
  return null;
}

function withTimeout<T>(promise: Promise<T>, ms: number, label = "timeout"): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(label)), ms);
    promise
      .then((v) => {
        clearTimeout(timer);
        resolve(v);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });
}

async function runWithConcurrency(tasks: Array<() => Promise<void>>, limit: number, shuttingDown: { flag: boolean }) {
  let index = 0;
  const workers = new Array(Math.min(limit, tasks.length)).fill(0).map(async () => {
    while (!shuttingDown.flag) {
      const i = index++;
      if (i >= tasks.length) break;
      await tasks[i]();
    }
  });
  await Promise.all(workers);
}

function parseSysUptimeValue(raw: unknown): number | null {
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const direct = Number(trimmed);
  if (Number.isFinite(direct)) return direct;
  const firstNumber = trimmed.match(/^\(?\s*(\d+)/);
  if (!firstNumber) return null;
  const parsed = Number(firstNumber[1]);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeCollectedSerialValue(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  const lower = trimmed.toLowerCase();
  if (lower.includes("no such object") || lower.includes("no such instance") || lower.includes("unknown object identifier")) {
    return null;
  }
  return trimmed;
}

function persistDiscoveryNormalization(
  db: DB,
  deviceId: number,
  discovery: { system: any; interfaces: any[]; lldpNeighbors: any[] },
  collectedAt: string,
  options: PersistDiscoveryOptions = {}
): PersistDiscoveryResult {
  const allowInterfaceReplace = options.allowInterfaceReplace !== false;
  const allowLldpReplace = options.allowLldpReplace !== false;
  const normalizedSerial = normalizeCollectedSerialValue(discovery?.system?.serialNumber);
  saveSnmpSystemSnapshot(db, {
    deviceId,
    system: {
      ...(discovery.system ?? {}),
      serialNumber: normalizedSerial,
    },
    collectedAt,
  });
  const updatedIdentity = updateDeviceIdentity(db, {
    id: deviceId,
    ...(normalizedSerial ? { serialNumber: normalizedSerial } : {}),
  });
  try {
    const sys = discovery.system ?? {};
    enqueueSnmpSystemSnapshot({
      deviceId: String(deviceId),
      sysName: sys.sysName ?? sys.sys_name ?? null,
      sysDescr: sys.sysDescr ?? sys.sys_descr ?? null,
      sysLocation: sys.sysLocation ?? sys.sys_location ?? null,
      sysContact: sys.sysContact ?? sys.sys_contact ?? null,
      sysUptime: parseSysUptimeValue(sys.sysUpTime ?? sys.sys_uptime),
      serialNumber: normalizedSerial,
      collectedAt,
    });
  } catch (err) {
    console.error(JSON.stringify({ level: "warn", msg: "system_enqueue_failed", deviceId, err: (err as any)?.message }));
  }
  // Derive utilization from counters locally; keep raw history local only
  const prevCounters = getLastInterfaceCounters(db, deviceId);
  const prevMap = new Map<number, { hcIn?: number | null; hcOut?: number | null; inOctets?: number | null; outOctets?: number | null; collectedAt: string }>();
  for (const p of prevCounters) {
    prevMap.set(p.ifIndex, {
      hcIn: p.hcIn ?? undefined,
      hcOut: p.hcOut ?? undefined,
      inOctets: p.inOctets ?? undefined,
      outOctets: p.outOctets ?? undefined,
      collectedAt: p.collectedAt,
    });
  }
  const collectedMs = new Date(collectedAt).getTime();
  const nextCounters: Array<{ ifIndex: number; hcIn?: number | null; hcOut?: number | null; inOctets?: number | null; outOctets?: number | null; collectedAt: string }> = [];
  const derivedInterfaces = (discovery.interfaces ?? []).map((intf: any) => {
    const ifIndex = intf.ifIndex ?? intf.if_index;
    const curr = {
      hcIn: intf.hcInOctets ?? intf.ifHCInOctets ?? null,
      hcOut: intf.hcOutOctets ?? intf.ifHCOutOctets ?? null,
      inOctets: intf.inOctets ?? intf.ifInOctets ?? null,
      outOctets: intf.outOctets ?? intf.ifOutOctets ?? null,
    };
    if (Number.isFinite(ifIndex)) {
      nextCounters.push({ ifIndex: Number(ifIndex), hcIn: curr.hcIn, hcOut: curr.hcOut, inOctets: curr.inOctets, outOctets: curr.outOctets, collectedAt });
    }
    let bpsIn: number | null = null;
    let bpsOut: number | null = null;
    let utilIn: number | null = null;
    let utilOut: number | null = null;
    let utilAvg: number | null = null;
    const prev = Number.isFinite(ifIndex) ? prevMap.get(Number(ifIndex)) : undefined;
    const prevMs = prev ? new Date(prev.collectedAt).getTime() : null;
    const deltaSec = prevMs && collectedMs > prevMs ? (collectedMs - prevMs) / 1000 : null;
    const chooseCounter = (currVal?: number | null, prevVal?: number | null) =>
      typeof currVal === "number" && typeof prevVal === "number" ? currVal - prevVal : null;
    if (deltaSec && deltaSec > 0) {
      const currIn = curr.hcIn ?? curr.inOctets;
      const currOut = curr.hcOut ?? curr.outOctets;
      const prevIn = prev?.hcIn ?? prev?.inOctets;
      const prevOut = prev?.hcOut ?? prev?.outOctets;
      const dIn = chooseCounter(currIn, prevIn);
      const dOut = chooseCounter(currOut, prevOut);
      if (dIn !== null && dIn >= 0) bpsIn = (dIn * 8) / deltaSec;
      if (dOut !== null && dOut >= 0) bpsOut = (dOut * 8) / deltaSec;
      const linkBps =
        typeof intf.ifHighSpeed === "number" && intf.ifHighSpeed > 0
          ? intf.ifHighSpeed * 1_000_000
          : typeof intf.ifSpeed === "number" && intf.ifSpeed > 0
          ? intf.ifSpeed
          : null;
      if (linkBps && linkBps > 0) {
        if (bpsIn !== null) utilIn = (bpsIn / linkBps) * 100;
        if (bpsOut !== null) utilOut = (bpsOut / linkBps) * 100;
        const vals = [utilIn, utilOut].filter((v) => v !== null) as number[];
        utilAvg = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
      }
    }
    return {
      ...intf,
      bpsIn: bpsIn ?? null,
      bpsOut: bpsOut ?? null,
      utilIn: utilIn ?? null,
      utilOut: utilOut ?? null,
      utilAvg: utilAvg ?? null,
      rateCollectedAt: deltaSec ? collectedAt : null,
    };
  });
  upsertLastInterfaceCounters(db, deviceId, nextCounters.filter((c) => Number.isFinite(c.ifIndex)));

  // Temporary debug: log first few physical interfaces with counters/util
  try {
    const phys = derivedInterfaces
      .filter((i: any) => typeof i.ifName === "string" && /^(gi|te|fo|eth)/i.test(i.ifName))
      .slice(0, 5)
      .map((i: any) => ({
        ifIndex: i.ifIndex,
        ifName: i.ifName,
        hcIn: i.hcInOctets ?? i.ifHCInOctets ?? i.inOctets ?? i.ifInOctets ?? null,
        hcOut: i.hcOutOctets ?? i.ifHCOutOctets ?? i.outOctets ?? i.ifOutOctets ?? null,
        bpsIn: i.bpsIn ?? null,
        bpsOut: i.bpsOut ?? null,
        utilAvg: i.utilAvg ?? null,
        rateCollectedAt: i.rateCollectedAt ?? null,
      }));
    if (phys.length) {
      console.log(
        JSON.stringify({
          level: "info",
          msg: "iface_util_debug",
          deviceId,
          sampleCount: phys.length,
          sample: phys,
        }),
      );
    }
  } catch {}

  if (allowInterfaceReplace) {
    replaceInterfaceSnapshotsForDevice(db, deviceId, derivedInterfaces ?? [], collectedAt);
  } else {
    console.error(
      JSON.stringify({
        level: "warn",
        msg: "interface_replace_preserved_previous_state",
        deviceId,
        collectedAt,
        discoveredCount: derivedInterfaces.length,
      }),
    );
  }
  if (allowLldpReplace) {
    replaceLldpNeighborsForDevice(db, deviceId, discovery.lldpNeighbors ?? [], collectedAt);
    upsertDiscoveredDeviceCandidatesFromLldp(db, deviceId, discovery.lldpNeighbors ?? [], collectedAt);
  } else {
    console.error(
      JSON.stringify({
        level: "warn",
        msg: "lldp_replace_preserved_previous_state",
        deviceId,
        collectedAt,
        discoveredCount: discovery.lldpNeighbors?.length ?? 0,
      }),
    );
  }
  try {
    const neighborsRaw = (discovery.lldpNeighbors ?? []).map((n: any) => ({
      localPort: n?.localPort ?? n?.local_port ?? n?.localPortId ?? null,
      remoteSysName: n?.remoteSysName ?? n?.remote_sys_name ?? null,
      remotePortId: n?.remotePortId ?? n?.remote_port_id ?? null,
      remotePortDesc: n?.remotePortDesc ?? n?.remote_port_desc ?? null,
      remoteChassisId: n?.remoteChassisId ?? n?.remote_chassis_id ?? null,
      remoteMgmtIp: n?.remoteMgmtIp ?? n?.remote_mgmt_ip ?? null,
    }));
    const neighbors = neighborsRaw.filter((n) => n.localPort && String(n.localPort).trim().length > 0);
    const droppedNeighbors = neighborsRaw.length - neighbors.length;
    if (droppedNeighbors > 0) {
      console.error(
        JSON.stringify({
          level: "warn",
          msg: "lldp_neighbors_dropped_missing_local_port",
          deviceId,
          dropped: droppedNeighbors,
          total: neighborsRaw.length,
        }),
      );
    }
    if (neighbors.length) {
      enqueueLldpNeighbors({
        deviceId: String(deviceId),
        neighbors,
        collectedAt,
      });
    }
  } catch (err) {
    console.error(JSON.stringify({ level: "warn", msg: "lldp_enqueue_failed", deviceId, err: (err as any)?.message }));
  }
  try {
    const ifacePayload = (derivedInterfaces ?? [])
      .map((i: any) => ({
        ifIndex: i?.ifIndex ?? i?.if_index,
        ifName: i?.ifName ?? i?.if_name,
        ifDescr: i?.ifDescr ?? i?.if_descr,
        ifAlias: i?.ifAlias ?? i?.if_alias,
        ifType: i?.ifType ?? i?.if_type ?? null,
        ifAdminStatus: i?.adminStatus ?? i?.ifAdminStatus ?? i?.admin_status,
        ifOperStatus: i?.operStatus ?? i?.ifOperStatus ?? i?.oper_status,
        ifSpeed: i?.speed ?? i?.ifSpeed,
        ifHighSpeed: i?.ifHighSpeed,
        mtu: i?.mtu,
        mac: i?.mac,
        bpsIn: i?.bpsIn ?? null,
        bpsOut: i?.bpsOut ?? null,
        utilIn: i?.utilIn ?? null,
        utilOut: i?.utilOut ?? null,
        utilAvg: i?.utilAvg ?? null,
        rateCollectedAt: i?.rateCollectedAt ?? collectedAt,
        collectedAt: collectedAt,
      }))
      .map((i) => ({ ...i, ifIndex: Number(i.ifIndex) }))
      .filter((i) => Number.isFinite(i.ifIndex) && Number.isInteger(i.ifIndex));
    const droppedIfaces = (discovery.interfaces?.length ?? 0) - ifacePayload.length;
    if (droppedIfaces > 0) {
      console.error(
        JSON.stringify({
          level: "warn",
          msg: "interface_enqueue_filtered_missing_ifIndex",
          dropped: droppedIfaces,
          total: discovery.interfaces?.length ?? 0,
          deviceId,
        }),
      );
    }
    if (ifacePayload.length) {
      console.log(
        JSON.stringify({
          level: "info",
          msg: "interface_enqueue",
          deviceId: String(deviceId),
          discovered: discovery.interfaces?.length ?? 0,
          enqueued: ifacePayload.length,
          dropped: droppedIfaces,
        }),
      );
      enqueueInterfaceSnapshot({
        deviceId: String(deviceId),
        interfaces: ifacePayload,
        collectedAt,
      });
    }
  } catch (err) {
    // non-fatal; ignore enqueue failure
    console.error(JSON.stringify({ level: "warn", msg: "interface_enqueue_failed", deviceId, err: (err as any)?.message }));
  }

  try {
    void flushTelemetryNow().catch(() => {});
  } catch {
    // ignore fast flush errors; periodic flush will handle
  }

  return {
    identity: updatedIdentity
      ? {
          assetTag: updatedIdentity.assetTag ?? null,
          serialNumber: updatedIdentity.serialNumber ?? null,
        }
      : null,
    persistence: {
      interfaces: {
        replaceApplied: allowInterfaceReplace,
        preservedPreviousState: !allowInterfaceReplace,
        collectedCount: derivedInterfaces.length,
      },
      lldp: {
        replaceApplied: allowLldpReplace,
        preservedPreviousState: !allowLldpReplace,
        collectedCount: discovery.lldpNeighbors?.length ?? 0,
      },
    },
  };
}
