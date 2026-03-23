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
