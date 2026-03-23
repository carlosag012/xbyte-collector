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
  })
  .transform((v) => ({
    nodeEnv: v.NODE_ENV,
    host: v.HOST,
    port: v.PORT,
    sqlitePath: v.SQLITE_PATH,
    bootstrapAdminUsername: v.BOOTSTRAP_ADMIN_USERNAME,
    bootstrapAdminPassword: v.BOOTSTRAP_ADMIN_PASSWORD,
    sessionTtlSeconds: v.SESSION_TTL_SECONDS,
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
