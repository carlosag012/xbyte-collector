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
    SNMP_WORKER_NAME: z.string().optional(),
    SNMP_WORKER_HEARTBEAT_MS: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_LOOP_MS: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_STUB_DELAY_MS: z.coerce.number().int().positive().optional(),
    PING_WORKER_COUNT: z.coerce.number().int().positive().optional(),
    SNMP_WORKER_COUNT: z.coerce.number().int().positive().optional(),
    WORKER_RESTART_DELAY_MS: z.coerce.number().int().positive().optional(),
    WORKER_MAX_RESTARTS_PER_WINDOW: z.coerce.number().int().positive().optional(),
    WORKER_RESTART_WINDOW_MS: z.coerce.number().int().positive().optional(),
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
    snmpWorkerName: v.SNMP_WORKER_NAME,
    snmpWorkerHeartbeatMs: v.SNMP_WORKER_HEARTBEAT_MS,
    snmpWorkerLoopMs: v.SNMP_WORKER_LOOP_MS,
    snmpWorkerStubDelayMs: v.SNMP_WORKER_STUB_DELAY_MS,
    pingWorkerCount: v.PING_WORKER_COUNT ?? 1,
    snmpWorkerCount: v.SNMP_WORKER_COUNT ?? 1,
    workerRestartDelayMs: v.WORKER_RESTART_DELAY_MS ?? 2000,
    workerMaxRestartsPerWindow: v.WORKER_MAX_RESTARTS_PER_WINDOW ?? 10,
    workerRestartWindowMs: v.WORKER_RESTART_WINDOW_MS ?? 60000,
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
