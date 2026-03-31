import { z } from "zod";

const EnvSchema = z
  .object({
    NODE_ENV: z.string().default("development"),
    HOST: z.string().default("127.0.0.1"),
    PORT: z.coerce.number().int().positive().min(1).max(65535).default(4100),
    SQLITE_PATH: z.string().default("./var/xbyte-collector.sqlite"),
    BOOTSTRAP_ADMIN_USERNAME: z.string().min(1).default("admin"),
    BOOTSTRAP_ADMIN_PASSWORD: z.string().min(1).default("changeme"),
    SESSION_TTL_SECONDS: z.coerce.number().int().positive().default(28800),
    PING_WORKER_NAME: z.string().optional(),
    PING_WORKER_HEARTBEAT_MS: z.coerce.number().int().positive().optional(),
    PING_WORKER_LOOP_MS: z.coerce.number().int().positive().optional(),
    PING_WORKER_STUB_DELAY_MS: z.coerce.number().int().positive().optional(),
    PING_WORKER_BATCH_SIZE: z.coerce.number().int().positive().optional(),
    PING_WORKER_FPING_PATH: z.string().optional(),
    SNMP_WORKER_NAME: z.string().optional(),
    SNMP_WORKER_HEARTBEAT_MS: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_LOOP_MS: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_STUB_DELAY_MS: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_BATCH_SIZE: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_CONCURRENCY: z.coerce.number().int().positive().optional(),
    SNMP_WALK_PATH: z.string().optional(),
    SNMP_GET_PATH: z.string().optional(),
    PING_WORKER_COUNT: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_COUNT: z.coerce.number().int().positive().optional(),
    WORKER_RESTART_DELAY_MS: z.coerce.number().int().positive().optional(),
    WORKER_MAX_RESTARTS_PER_WINDOW: z.coerce.number().int().positive().optional(),
    WORKER_RESTART_WINDOW_MS: z.coerce.number().int().positive().optional(),
    LOG_LEVEL: z.string().optional(),
    XMON_API_BASE: z.string().optional(),
    XMON_COLLECTOR_ID: z.string().optional(),
    XMON_API_KEY: z.string().optional(),
    XMON_HEARTBEAT_MS: z.coerce.number().int().positive().optional(),
    XMON_CONFIG_REFRESH_MS: z.coerce.number().int().positive().optional(),
  })
  .transform((v) => ({
    nodeEnv: v.NODE_ENV,
    host: v.HOST,
    port: v.PORT,
    sqlitePath: v.SQLITE_PATH,
    bootstrapAdminUsername: v.BOOTSTRAP_ADMIN_USERNAME,
    bootstrapAdminPassword: v.BOOTSTRAP_ADMIN_PASSWORD,
    sessionTtlSeconds: v.SESSION_TTL_SECONDS,
    pingWorkerName: v.PING_WORKER_NAME,
    pingWorkerHeartbeatMs: v.PING_WORKER_HEARTBEAT_MS,
    pingWorkerLoopMs: v.PING_WORKER_LOOP_MS,
    pingWorkerStubDelayMs: v.PING_WORKER_STUB_DELAY_MS,
    pingWorkerBatchSize: v.PING_WORKER_BATCH_SIZE ?? 200,
    pingWorkerFpingPath: v.PING_WORKER_FPING_PATH ?? "fping",
    snmpWorkerName: v.SNMP_WORKER_NAME,
    snmpWorkerHeartbeatMs: v.SNMP_WORKER_HEARTBEAT_MS,
    snmpWorkerLoopMs: v.SNMP_WORKER_LOOP_MS,
    snmpWorkerStubDelayMs: v.SNMP_WORKER_STUB_DELAY_MS,
    snmpWorkerBatchSize: v.SNMP_WORKER_BATCH_SIZE ?? 50,
    snmpWorkerConcurrency: v.SNMP_WORKER_CONCURRENCY ?? 16,
    snmpWalkPath: v.SNMP_WALK_PATH ?? "snmpwalk",
    snmpGetPath: v.SNMP_GET_PATH ?? "snmpget",
    pingWorkerCount: v.PING_WORKER_COUNT ?? 1,
    snmpWorkerCount: v.SNMP_WORKER_COUNT ?? 1,
    workerRestartDelayMs: v.WORKER_RESTART_DELAY_MS ?? 2000,
    workerMaxRestartsPerWindow: v.WORKER_MAX_RESTARTS_PER_WINDOW ?? 10,
    workerRestartWindowMs: v.WORKER_RESTART_WINDOW_MS ?? 60000,
    logLevel: (v.LOG_LEVEL ?? "info") as any,
    xmonApiBase: v.XMON_API_BASE?.trim() || "http://localhost:4000/api/xmon",
    xmonCollectorId: v.XMON_COLLECTOR_ID?.trim() || null,
    xmonApiKey: v.XMON_API_KEY?.trim() || null,
    xmonHeartbeatMs: v.XMON_HEARTBEAT_MS ?? 15000,
    xmonConfigRefreshMs: v.XMON_CONFIG_REFRESH_MS ?? 60000,
  }));

export type AppConfig = z.infer<typeof EnvSchema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  const parsed = EnvSchema.safeParse(env);
  if (!parsed.success) {
    const issues = parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ");
    throw new Error(`Invalid configuration: ${issues}`);
  }
  return parsed.data;
}
