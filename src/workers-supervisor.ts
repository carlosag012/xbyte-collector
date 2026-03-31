import { fork, ChildProcess } from "node:child_process";
import { resolve } from "node:path";
import { loadConfig } from "./config.js";
import { initDatabase, getWorkerRegistrationByName, type DB } from "./db.js";
import { enqueueTelemetry } from "./telemetry-queue.js";

function log(data: Record<string, any>) {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      component: "workers-supervisor",
      ...data,
    })
  );
}

type ManagedChild = {
  proc: ChildProcess;
  kind: "ping" | "snmp";
  name: string;
};

function spawnWorker(kind: "ping" | "snmp", name: string): ManagedChild {
  const script = kind === "ping" ? resolve("src", "worker-ping.ts") : resolve("src", "worker-snmp.ts");
  const env = { ...process.env };
  if (kind === "ping") env.PING_WORKER_NAME = name;
  if (kind === "snmp") env.SNMP_WORKER_NAME = name;

  const proc = fork(script, {
    stdio: "inherit",
    env,
  });
  log({ level: "info", msg: "worker spawned", kind, name, pid: proc.pid });
  return { proc, kind, name };
}

async function main() {
  const cfg = loadConfig();
  const db: DB = initDatabase(cfg);
  const children: ManagedChild[] = [];
  let shuttingDown = false;
  const restartHistory = new Map<string, number[]>();

  const pingCount = Math.max(1, cfg.pingWorkerCount ?? 1);
  const snmpCount = Math.max(1, cfg.snmpWorkerCount ?? 1);
  const restartDelayMs = cfg.workerRestartDelayMs ?? 2000;
  const maxRestarts = cfg.workerMaxRestartsPerWindow ?? 10;
  const restartWindowMs = cfg.workerRestartWindowMs ?? 60_000;

  for (let i = 1; i <= pingCount; i++) {
    const name = `ping-worker-${i}`;
    const reg = getWorkerRegistrationByName(db, name);
    if (reg && reg.enabled === false) {
      log({ level: "info", msg: "worker launch skipped (disabled)", kind: "ping", name });
    } else {
      children.push(spawnWorker("ping", name));
      enqueueTelemetry({
        messageId: `worker-start-${name}-${Date.now()}`,
        kind: "event",
        ts: new Date().toISOString(),
        payload: { type: "worker_started", workerName: name, workerKind: "ping" },
      });
    }
  }
  for (let i = 1; i <= snmpCount; i++) {
    const name = `snmp-worker-${i}`;
    const reg = getWorkerRegistrationByName(db, name);
    if (reg && reg.enabled === false) {
      log({ level: "info", msg: "worker launch skipped (disabled)", kind: "snmp", name });
    } else {
      children.push(spawnWorker("snmp", name));
      enqueueTelemetry({
        messageId: `worker-start-${name}-${Date.now()}`,
        kind: "event",
        ts: new Date().toISOString(),
        payload: { type: "worker_started", workerName: name, workerKind: "snmp" },
      });
    }
  }

  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log({ level: "info", msg: "supervisor shutdown requested", signal });
    children.forEach((c) => {
      if (!c.proc.killed) {
        c.proc.kill("SIGTERM");
      }
    });
    // allow children time to exit
    setTimeout(() => {
      log({ level: "info", msg: "supervisor exiting", signal });
      process.exit(0);
    }, 2000);
  };

  process.on("SIGINT", () => {
    shutdown("SIGINT").catch(() => process.exit(1));
  });
  process.on("SIGTERM", () => {
    shutdown("SIGTERM").catch(() => process.exit(1));
  });

  const attachExitHandler = (child: ManagedChild) => {
    child.proc.on("exit", (code, signal) => {
      log({ level: "info", msg: "worker exited", kind: child.kind, name: child.name, code, signal, shuttingDown });
      if (shuttingDown) return;
      const key = `${child.kind}:${child.name}`;
      const now = Date.now();
      const history = restartHistory.get(key) ?? [];
      const recent = history.filter((ts) => now - ts <= restartWindowMs);
      recent.push(now);
      restartHistory.set(key, recent);

      if (recent.length > maxRestarts) {
        log({
          level: "error",
          msg: "worker restart suppressed",
          kind: child.kind,
          name: child.name,
          restartCount: recent.length,
          restartWindowMs,
        });
        return;
      }

      log({
        level: "warn",
        msg: "worker exited unexpectedly, scheduling restart",
        kind: child.kind,
        name: child.name,
        code,
        signal,
        restartDelayMs,
        restartCount: recent.length,
      });

      setTimeout(() => {
        if (shuttingDown) return;
        const reg = getWorkerRegistrationByName(db, child.name);
        if (reg && reg.enabled === false) {
          log({ level: "info", msg: "worker restart skipped (disabled)", kind: child.kind, name: child.name });
          return;
        }
        const replacement = spawnWorker(child.kind, child.name);
        const idx = children.findIndex((c) => c.name === child.name && c.kind === child.kind);
        if (idx >= 0) {
          children.splice(idx, 1, replacement);
        } else {
          children.push(replacement);
        }
        attachExitHandler(replacement);
      }, restartDelayMs);
    });
  };

  children.forEach((c) => attachExitHandler(c));
}

main().catch((err) => {
  log({ level: "fatal", msg: "supervisor crashed", error: err?.message ?? String(err) });
  process.exit(1);
});
