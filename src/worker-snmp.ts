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
  type DB,
} from "./db.js";

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

      const tasks = validDetails.map(({ job, detail }) => async () => {
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
        let status: "completed" | "failed" = res.success ? "completed" : "failed";
        const processedAt = new Date().toISOString();

        if (res.success) {
          try {
            persistDiscoveryNormalization(db, detail.device.id, res.discovery, processedAt);
          } catch (err: any) {
            status = "failed";
            res.success = false;
            res.error = err?.message ?? "snmp_persistence_failed";
          }
        }

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

async function processSnmpJob(detail: any, workerCfg: WorkerConfig): Promise<{ success: boolean; summary: any; discovery: any; error?: string }> {
  const target = detail.device.ipAddress;
  const profileConfig = (detail.profile?.config ?? {}) as any;
  const timeoutMs = typeof detail.profile.timeoutMs === "number" ? detail.profile.timeoutMs : 2000;

  try {
    const system = await snmpGetSystem(workerCfg.snmpGetPath, target, profileConfig, timeoutMs);
    const interfaces = await snmpWalkInterfaces(workerCfg.snmpWalkPath, target, profileConfig, timeoutMs);
    const lldpNeighbors = await snmpWalkLldp(workerCfg.snmpWalkPath, target, profileConfig, timeoutMs);

    return {
      success: true,
      summary: {
        system,
        interfacesCount: interfaces.length,
        lldpNeighborsCount: lldpNeighbors.length,
      },
      discovery: {
        system,
        interfaces,
        lldpNeighbors,
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
  };
}

async function snmpWalkInterfaces(snmpWalkPath: string, target: string, profileConfig: any, timeoutMs: number) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const oids = [
    "IF-MIB::ifName",
    "IF-MIB::ifDescr",
    "IF-MIB::ifAlias",
    "IF-MIB::ifAdminStatus",
    "IF-MIB::ifOperStatus",
    "IF-MIB::ifSpeed",
    "IF-MIB::ifHighSpeed",
    "IF-MIB::ifMtu",
    "IF-MIB::ifPhysAddress",
  ];
  const args = [...buildSnmpBaseArgs(profileConfig, target, timeoutSec), ...oids];
  const { stdout } = await execFileAsync(snmpWalkPath, args, { timeout: timeoutMs + 1500, encoding: "utf8" });
  return parseInterfaces(stdout);
}

async function snmpWalkLldp(snmpWalkPath: string, target: string, profileConfig: any, timeoutMs: number) {
  const timeoutSec = Math.max(1, Math.ceil(timeoutMs / 1000));
  const oids = ["LLDP-MIB::lldpLocPortTable", "LLDP-MIB::lldpRemTable", "LLDP-MIB::lldpRemManAddrTable"];
  const args = [...buildSnmpBaseArgs(profileConfig, target, timeoutSec), ...oids];
  const { stdout } = await execFileAsync(snmpWalkPath, args, { timeout: timeoutMs + 1500, encoding: "utf8" });
  return parseLldp(stdout);
}

function buildSnmpBaseArgs(profileConfig: any, target: string, timeoutSec: number): string[] {
  const version = (profileConfig?.version as string) || "2c";
  if (version === "3") {
    const username = profileConfig?.username || profileConfig?.user;
    if (!username) throw new Error("snmpv3_missing_username");
    const securityLevel = profileConfig?.securityLevel || "noAuthNoPriv";
    const args = ["-v", "3", "-l", securityLevel, "-u", username];
    if (profileConfig?.authProtocol && profileConfig?.authPassword) {
      args.push("-a", profileConfig.authProtocol, "-A", profileConfig.authPassword);
    }
    if (profileConfig?.privProtocol && profileConfig?.privPassword) {
      args.push("-x", profileConfig.privProtocol, "-X", profileConfig.privPassword);
    }
    args.push("-t", timeoutSec.toString(), "-r", "1", target);
    return args;
  }
  const community = profileConfig?.community || "public";
  const versionClean = version || "2c";
  return ["-v", versionClean, "-c", community, "-t", timeoutSec.toString(), "-r", "1", target];
}

function parseSnmpGet(stdout: string): Record<string, string> {
  const lines = stdout.trim().split("\n");
  const out: Record<string, string> = {};
  for (const line of lines) {
    const [oidPart, rest] = line.split(" = ").map((s) => s.trim());
    if (!oidPart || !rest) continue;
    const oidFull = (oidPart.includes("::") ? oidPart.split("::")[1] : oidPart).split(" ")[0];
    const val = rest.split(":").slice(1).join(":").trim();
    out[oidFull] = val;
  }
  return out;
}

function parseInterfaces(stdout: string) {
  const lines = stdout.trim().split("\n").filter((l) => l.trim().length);
  const map = new Map<number, any>();
  for (const line of lines) {
    const [oidPart, rest] = line.split(" = ").map((s) => s.trim());
    if (!oidPart || !rest) continue;
    const [oidNameFull, idxStr] = oidPart.split(".");
    const idx = Number(idxStr);
    if (!Number.isFinite(idx)) continue;
    const oidName = oidNameFull.includes("::") ? oidNameFull.split("::")[1] : oidNameFull;
    const val = rest.split(":").slice(1).join(":").trim();
    const rec = map.get(idx) ?? {};
    if (oidName.endsWith("ifName")) rec.ifName = val;
    else if (oidName.endsWith("ifDescr")) rec.ifDescr = val;
    else if (oidName.endsWith("ifAlias")) rec.ifAlias = val;
    else if (oidName.endsWith("ifAdminStatus")) rec.adminStatus = val;
    else if (oidName.endsWith("ifOperStatus")) rec.operStatus = val;
    else if (oidName.endsWith("ifSpeed")) rec.ifSpeed = Number(val) || null;
    else if (oidName.endsWith("ifHighSpeed")) rec.ifHighSpeed = Number(val) || null;
    else if (oidName.endsWith("ifMtu")) rec.ifMtu = Number(val) || null;
    else if (oidName.endsWith("ifPhysAddress")) rec.ifPhysAddress = val;
    map.set(idx, rec);
  }
  return Array.from(map.entries()).map(([ifIndex, rec]) => ({
    ifIndex,
    ifName: rec.ifName ?? null,
    ifDescr: rec.ifDescr ?? null,
    ifAlias: rec.ifAlias ?? null,
    adminStatus: rec.adminStatus ?? null,
    operStatus: rec.operStatus ?? null,
    speed: rec.ifHighSpeed ?? rec.ifSpeed ?? null,
    mtu: rec.ifMtu ?? null,
    mac: rec.ifPhysAddress ?? null,
  }));
}

function parseLldp(stdout: string) {
  const neighbors: any[] = [];
  const lines = stdout.trim().split("\n").filter((l) => l.trim().length);
  for (const line of lines) {
    const [oidPart, rest] = line.split(" = ").map((s) => s.trim());
    if (!oidPart || !rest) continue;
    if (oidPart.includes("lldpRemSysName")) {
      const key = oidPart.split(".").slice(-2).join(".");
      const existing = neighbors.find((n) => n.key === key) ?? { key };
      existing.remoteSysName = rest.split(":").slice(1).join(":").trim();
      if (!neighbors.includes(existing)) neighbors.push(existing);
    } else if (oidPart.includes("lldpRemPortId")) {
      const key = oidPart.split(".").slice(-2).join(".");
      const existing = neighbors.find((n) => n.key === key) ?? { key };
      existing.remotePortId = rest.split(":").slice(1).join(":").trim();
      if (!neighbors.includes(existing)) neighbors.push(existing);
    } else if (oidPart.includes("lldpRemChassisId")) {
      const key = oidPart.split(".").slice(-2).join(".");
      const existing = neighbors.find((n) => n.key === key) ?? { key };
      existing.remoteChassisId = rest.split(":").slice(1).join(":").trim();
      if (!neighbors.includes(existing)) neighbors.push(existing);
    } else if (oidPart.includes("lldpRemManAddrIfSubtype")) {
      const key = oidPart.split(".").slice(-2).join(".");
      const existing = neighbors.find((n) => n.key === key) ?? { key };
      existing.remoteMgmtIp = rest.split(":").slice(1).join(":").trim();
      if (!neighbors.includes(existing)) neighbors.push(existing);
    } else if (oidPart.includes("lldpLocPortId")) {
      const key = oidPart.split(".").slice(-1).join(".");
      const existing = neighbors.find((n) => n.localPort === key) ?? { key };
      existing.localPort = rest.split(":").slice(1).join(":").trim();
      if (!neighbors.includes(existing)) neighbors.push(existing);
    }
  }
  return neighbors.map((n) => ({
    localPort: n.localPort ?? null,
    remoteSysName: n.remoteSysName ?? null,
    remotePortId: n.remotePortId ?? null,
    remoteChassisId: n.remoteChassisId ?? null,
    remoteMgmtIp: n.remoteMgmtIp ?? null,
  }));
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

function persistDiscoveryNormalization(
  db: DB,
  deviceId: number,
  discovery: { system: any; interfaces: any[]; lldpNeighbors: any[] },
  collectedAt: string
) {
  saveSnmpSystemSnapshot(db, {
    deviceId,
    system: discovery.system ?? {},
    collectedAt,
  });
  replaceInterfaceSnapshotsForDevice(db, deviceId, discovery.interfaces ?? [], collectedAt);
  replaceLldpNeighborsForDevice(db, deviceId, discovery.lldpNeighbors ?? [], collectedAt);
  upsertDiscoveredDeviceCandidatesFromLldp(db, deviceId, discovery.lldpNeighbors ?? [], collectedAt);
}
