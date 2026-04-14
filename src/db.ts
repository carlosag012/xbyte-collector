import Database from "better-sqlite3";
import { randomUUID } from "node:crypto";
import { mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import type { AppConfig } from "./config.js";

export type DB = Database.Database;

export function initDatabase(config: AppConfig): DB {
  ensureDir(dirname(config.sqlitePath));

  const db = new Database(config.sqlitePath);
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS app_config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS bootstrap_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      configured INTEGER NOT NULL,
      status TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL,
      is_active INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      user_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS devices (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      hostname TEXT NOT NULL,
      ip_address TEXT NOT NULL,
      enabled INTEGER NOT NULL,
      site TEXT,
      type TEXT,
      org TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS poll_profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kind TEXT NOT NULL,
      name TEXT NOT NULL,
      interval_sec INTEGER NOT NULL,
      timeout_ms INTEGER NOT NULL,
      retries INTEGER NOT NULL,
      enabled INTEGER NOT NULL,
      config_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS poll_targets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id INTEGER NOT NULL,
      profile_id INTEGER NOT NULL,
      enabled INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(device_id) REFERENCES devices(id),
      FOREIGN KEY(profile_id) REFERENCES poll_profiles(id)
    );
    CREATE TABLE IF NOT EXISTS worker_registrations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      worker_type TEXT NOT NULL,
      worker_name TEXT NOT NULL UNIQUE,
      capabilities_json TEXT NOT NULL,
      last_heartbeat_at TEXT NOT NULL,
      enabled INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS poll_jobs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      target_id INTEGER NOT NULL,
      status TEXT NOT NULL,
      scheduled_at TEXT NOT NULL,
      started_at TEXT,
      finished_at TEXT,
      lease_owner TEXT,
      result_json TEXT,
      attempt_count INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      FOREIGN KEY(target_id) REFERENCES poll_targets(id)
    );
    CREATE TABLE IF NOT EXISTS companies (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      company_name TEXT NOT NULL,
      company_slug TEXT NOT NULL UNIQUE,
      org_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS deployments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      deployment_id TEXT NOT NULL UNIQUE,
      deployment_name TEXT NOT NULL,
      mode TEXT NOT NULL,
      registered_to_cloud INTEGER NOT NULL,
      build_channel TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS cloud_sync_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      enabled INTEGER NOT NULL,
      status TEXT NOT NULL,
      last_sync_at TEXT,
      cloud_endpoint TEXT,
      tenant_key TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS snmp_system_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id INTEGER NOT NULL,
      sys_name TEXT,
      sys_descr TEXT,
      sys_object_id TEXT,
      sys_uptime TEXT,
      collected_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS interface_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id INTEGER NOT NULL,
      if_index INTEGER,
      if_type INTEGER,
      if_name TEXT,
      if_descr TEXT,
      if_alias TEXT,
      admin_status TEXT,
      oper_status TEXT,
      speed INTEGER,
      mtu INTEGER,
      mac TEXT,
      bps_in REAL,
      bps_out REAL,
      util_in REAL,
      util_out REAL,
      util_avg REAL,
      rate_collected_at TEXT,
      collected_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS interface_counters_last (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id INTEGER NOT NULL,
      if_index INTEGER NOT NULL,
      hc_in_octets INTEGER,
      hc_out_octets INTEGER,
      in_octets INTEGER,
      out_octets INTEGER,
      collected_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(device_id, if_index)
    );
    CREATE TABLE IF NOT EXISTS lldp_neighbors (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      device_id INTEGER NOT NULL,
      local_port TEXT,
      remote_sys_name TEXT,
      remote_port_id TEXT,
      remote_chassis_id TEXT,
      remote_mgmt_ip TEXT,
      collected_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS neighbor_reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      neighbor_id INTEGER NOT NULL UNIQUE,
      status TEXT NOT NULL DEFAULT 'new',
      linked_device_id INTEGER,
      promoted_device_id INTEGER,
      note TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS neighbor_review_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      neighbor_id INTEGER NOT NULL,
      action TEXT NOT NULL,
      actor TEXT,
      device_id INTEGER,
      note TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS admin_audit_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_type TEXT NOT NULL,
      entity_id INTEGER,
      action TEXT NOT NULL,
      actor TEXT,
      note TEXT,
      before_json TEXT,
      after_json TEXT,
      created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS license_state (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      status TEXT NOT NULL,
      subscription_status TEXT NOT NULL,
      license_key TEXT,
      customer TEXT,
      validated_at TEXT,
      expires_at TEXT,
      last_error TEXT,
      updated_at TEXT NOT NULL
    );
    INSERT OR IGNORE INTO license_state (id, status, subscription_status, updated_at) VALUES (1, 'unlicensed', 'inactive', CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS discovered_device_candidates (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_device_id INTEGER NOT NULL,
      remote_sys_name TEXT,
      remote_chassis_id TEXT,
      remote_mgmt_ip TEXT,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'candidate',
      notes TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS agent_enrollments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      enrollment_id TEXT NOT NULL UNIQUE,
      agent_type TEXT NOT NULL,
      token TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL,
      company_slug TEXT,
      deployment_id TEXT,
      created_at TEXT NOT NULL,
      expires_at TEXT,
      last_used_at TEXT,
      changed_by TEXT,
      updated_at TEXT NOT NULL
    );
  `);

  // Additive schema safety for existing databases
  try {
    db.prepare(`ALTER TABLE agent_enrollments ADD COLUMN changed_by TEXT`).run();
  } catch {
    // ignore if it already exists
  }

  try {
    db.prepare(`ALTER TABLE lldp_neighbors ADD COLUMN remote_port_id TEXT`).run();
  } catch {
    // ignore if it already exists
  }

  try {
    db.prepare(`ALTER TABLE devices ADD COLUMN type TEXT`).run();
  } catch {
    // ignore if it already exists
  }

  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN bps_in REAL`).run();
  } catch {}
  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN if_type INTEGER`).run();
  } catch {}
  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN bps_out REAL`).run();
  } catch {}
  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN util_in REAL`).run();
  } catch {}
  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN util_out REAL`).run();
  } catch {}
  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN util_avg REAL`).run();
  } catch {}
  try {
    db.prepare(`ALTER TABLE interface_snapshots ADD COLUMN rate_collected_at TEXT`).run();
  } catch {}

  // Ensure a default ping profile exists (authoritative availability)
  try {
    const existingPing = db
      .prepare(`SELECT id FROM poll_profiles WHERE kind = 'ping' ORDER BY id LIMIT 1`)
      .get() as { id: number } | undefined;
    if (!existingPing) {
      db.prepare(
        `INSERT INTO poll_profiles (kind, name, interval_sec, timeout_ms, retries, enabled, config_json, created_at, updated_at)
         VALUES ('ping', 'Default Ping', 120, 1000, 1, 1, '{}', ?, ?)`
      ).run(new Date().toISOString(), new Date().toISOString());
    }
  } catch {
    // ignore if cannot insert; scheduler will proceed with existing profiles
  }

  try {
    db.prepare(`ALTER TABLE poll_jobs ADD COLUMN result_json TEXT`).run();
  } catch {
    // ignore if already exists
  }

  const row = db
    .prepare(`SELECT id FROM bootstrap_state WHERE id = 1`)
    .get() as { id: number } | undefined;

  if (!row) {
    db.prepare(
      `INSERT INTO bootstrap_state (id, configured, status, updated_at) VALUES (1, ?, ?, ?)`
    ).run(0, "not-initialized", new Date().toISOString());
  }

  const cloudSyncRow = db
    .prepare(`SELECT id FROM cloud_sync_state WHERE id = 1`)
    .get() as { id: number } | undefined;
  if (!cloudSyncRow) {
    const now = new Date().toISOString();
    db.prepare(
      `INSERT INTO cloud_sync_state (id, enabled, status, last_sync_at, cloud_endpoint, tenant_key, created_at, updated_at)
       VALUES (1, ?, ?, ?, ?, ?, ?, ?)`
    ).run(0, "not-configured", null, null, null, now, now);
  }

  return db;
}

function ensureDir(path: string) {
  if (!path || path === "." || existsSync(path)) return;
  mkdirSync(path, { recursive: true });
}

export function getAllAppConfig(db: DB): Record<string, string> {
  const rows = db.prepare(`SELECT key, value FROM app_config`).all() as Array<{ key: string; value: string }>;
  const out: Record<string, string> = {};
  for (const r of rows) out[r.key] = r.value;
  return out;
}

export function upsertAppConfigEntries(db: DB, updates: Record<string, string>) {
  const stmt = db.prepare(
    `INSERT INTO app_config (key, value, updated_at)
     VALUES (@key, @value, @updatedAt)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`
  );
  const updatedAt = new Date().toISOString();
  const entries = Object.entries(updates);
  const tx = db.transaction(() => {
    for (const [key, value] of entries) {
      stmt.run({ key, value, updatedAt });
    }
  });
  tx();
}

export type BootstrapStateRow = {
  configured: boolean;
  status: string;
  updatedAt: string;
};

export function getBootstrapState(db: DB): BootstrapStateRow {
  const row = db
    .prepare(
      `SELECT configured, status, updated_at as updatedAt
       FROM bootstrap_state
       WHERE id = 1
       LIMIT 1`
    )
    .get() as { configured: number; status: string; updatedAt: string } | undefined;

  if (!row) {
    return { configured: false, status: "not-initialized", updatedAt: new Date().toISOString() };
  }
  return {
    configured: Boolean(row.configured),
    status: row.status,
    updatedAt: row.updatedAt,
  };
}

export function setBootstrapStateRow(db: DB, input: { configured: boolean; status: string }) {
  const updatedAt = new Date().toISOString();
  db.prepare(
    `UPDATE bootstrap_state
     SET configured = ?, status = ?, updated_at = ?
     WHERE id = 1`
  ).run(input.configured ? 1 : 0, input.status, updatedAt);
}

export type UserRow = {
  id: number;
  username: string;
  passwordHash: string;
  role: string;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
};

export function getUserByUsername(db: DB, username: string): UserRow | null {
  const row = db
    .prepare(
      `SELECT id, username, password_hash as passwordHash, role, is_active as isActive,
              created_at as createdAt, updated_at as updatedAt
       FROM users
       WHERE username = ?
       LIMIT 1`
    )
    .get(username) as
    | {
        id: number;
        username: string;
        passwordHash: string;
        role: string;
        isActive: number;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;

  if (!row) return null;
  return {
    ...row,
    isActive: Boolean(row.isActive),
  };
}

export function createUser(db: DB, input: { username: string; passwordHash: string; role: string; isActive: boolean }): UserRow {
  const now = new Date().toISOString();
  const result = db
    .prepare(
      `INSERT INTO users (username, password_hash, role, is_active, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?)`
    )
    .run(input.username, input.passwordHash, input.role, input.isActive ? 1 : 0, now, now);

  return {
    id: Number(result.lastInsertRowid),
    username: input.username,
    passwordHash: input.passwordHash,
    role: input.role,
    isActive: input.isActive,
    createdAt: now,
    updatedAt: now,
  };
}

export function updateUserPassword(db: DB, username: string, passwordHash: string) {
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE users
     SET password_hash = ?, updated_at = ?
     WHERE username = ?`
  ).run(passwordHash, now, username);
}

export function getUserById(db: DB, id: number): UserRow | null {
  const row = db
    .prepare(
      `SELECT id, username, password_hash as passwordHash, role, is_active as isActive,
              created_at as createdAt, updated_at as updatedAt
       FROM users
       WHERE id = ?
       LIMIT 1`
    )
    .get(id) as
    | {
        id: number;
        username: string;
        passwordHash: string;
        role: string;
        isActive: number;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;

  if (!row) return null;
  return { ...row, isActive: Boolean(row.isActive) };
}

export function countUsers(db: DB): number {
  const row = db.prepare(`SELECT COUNT(*) as c FROM users`).get() as { c: number };
  return row?.c ?? 0;
}

export function countAdmins(db: DB): number {
  const row = db.prepare(`SELECT COUNT(*) as c FROM users WHERE role = 'admin'`).get() as { c: number };
  return row?.c ?? 0;
}

export function hasAnyAdmin(db: DB): boolean {
  return countAdmins(db) > 0;
}

export function createInitialAdminIfMissing(db: DB, input: { username: string; passwordHash: string }): {
  created: boolean;
  user?: UserRow;
} {
  if (hasAnyAdmin(db)) return { created: false };

  const user = createUser(db, {
    username: input.username,
    passwordHash: input.passwordHash,
    role: "admin",
    isActive: true,
  });

  return { created: true, user };
}

export type SessionRow = {
  id: string;
  userId: number;
  createdAt: string;
  expiresAt: string;
};

export function createSession(db: DB, input: { id: string; userId: number; createdAt: string; expiresAt: string }) {
  db.prepare(
    `INSERT INTO sessions (id, user_id, created_at, expires_at)
     VALUES (?, ?, ?, ?)`
  ).run(input.id, input.userId, input.createdAt, input.expiresAt);
}

export function getSessionById(db: DB, id: string): SessionRow | null {
  const row = db
    .prepare(
      `SELECT id, user_id as userId, created_at as createdAt, expires_at as expiresAt
       FROM sessions WHERE id = ? LIMIT 1`
    )
    .get(id) as SessionRow | undefined;
  return row ?? null;
}

export function deleteSession(db: DB, id: string) {
  db.prepare(`DELETE FROM sessions WHERE id = ?`).run(id);
}

export type DeviceRow = {
  id: number;
  hostname: string;
  ipAddress: string;
  enabled: boolean;
  site: string | null;
  type: string | null;
  org: string | null;
  createdAt: string;
  updatedAt: string;
};

export type DevicePollHealth = {
  currentStatus: string;
  lastPollAt: string | null;
  lastSuccessAt: string | null;
  lastFailureAt: string | null;
  successCount: number;
  failureCount: number;
  lastError: string | null;
  activeSnmpProfile: string | null;
  activeSnmpPollerId: string | null;
  hasSnmpBinding: boolean;
  hasPingBinding: boolean;
  lastSnmpPollAt: string | null;
  lastSnmpSuccessAt: string | null;
  lastSnmpFailureAt: string | null;
  lastSnmpError: string | null;
  lastPingSuccessAt: string | null;
  lastPingFailureAt: string | null;
  cloudSyncConfigured: boolean;
};

export function listDevices(db: DB): DeviceRow[] {
  const rows = db
    .prepare(
      `SELECT id, hostname, ip_address as ipAddress, enabled, site, type, org, created_at as createdAt, updated_at as updatedAt
       FROM devices ORDER BY id`
    )
    .all() as Array<{
    id: number;
    hostname: string;
    ipAddress: string;
    enabled: number;
    site: string | null;
    type: string | null;
    org: string | null;
    createdAt: string;
    updatedAt: string;
  }>;
  return rows.map((r) => ({ ...r, enabled: Boolean(r.enabled), type: r.type ?? null }));
}

export function findDeviceByIpOrHostname(db: DB, input: { ipAddress?: string; hostname?: string }): DeviceRow | null {
  const row = db
    .prepare(
      `SELECT id, hostname, ip_address as ipAddress, enabled, site, type, org, created_at as createdAt, updated_at as updatedAt
       FROM devices
       WHERE (${input.ipAddress ? "ip_address = ?" : "0"}) OR (${input.hostname ? "hostname = ?" : "0"})
       ORDER BY id LIMIT 1`
    )
    .get(
      ...(input.ipAddress && input.hostname
        ? [input.ipAddress, input.hostname]
        : input.ipAddress
        ? [input.ipAddress]
        : input.hostname
        ? [input.hostname]
        : [])
    ) as
    | {
        id: number;
        hostname: string;
        ipAddress: string;
        enabled: number;
        site: string | null;
        type: string | null;
        org: string | null;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  return row ? { ...row, enabled: Boolean(row.enabled), type: row.type ?? null } : null;
}

export function createDevice(db: DB, input: { hostname: string; ipAddress: string; enabled?: boolean; site?: string; org?: string; type?: string }): DeviceRow {
  const now = new Date().toISOString();
  const enabled = input.enabled ?? true;
  const dup = db
    .prepare(`SELECT id FROM devices WHERE ip_address = ? LIMIT 1`)
    .get(input.ipAddress) as { id: number } | undefined;
  if (dup) {
    const err = new Error("device_ip_exists");
    throw err;
  }
  const result = db
    .prepare(
      `INSERT INTO devices (hostname, ip_address, enabled, site, type, org, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(input.hostname, input.ipAddress, enabled ? 1 : 0, input.site ?? null, input.type ?? null, input.org ?? null, now, now);
  return {
    id: Number(result.lastInsertRowid),
    hostname: input.hostname,
    ipAddress: input.ipAddress,
    enabled,
    site: input.site ?? null,
    type: input.type ?? null,
    org: input.org ?? null,
    createdAt: now,
    updatedAt: now,
  };
}

export function getDevicePollHealth(
  db: DB,
  deviceId: number,
  cloudCfg?: { xmonApiBase?: string | null; xmonCollectorId?: string | null; xmonApiKey?: string | null }
): DevicePollHealth {
  const latest = db
    .prepare(
      `SELECT pj.status, pj.finished_at as finishedAt, pj.started_at as startedAt, pj.scheduled_at as scheduledAt,
              pj.created_at as createdAt, pj.result_json as resultJson,
              pp.name as profileName, pp.kind as profileKind, pt.id as targetId, pp.id as profileId
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       LEFT JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ?
       ORDER BY pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as
    | {
        status: string;
        finishedAt: string | null;
        startedAt: string | null;
        scheduledAt: string | null;
        createdAt: string;
        resultJson: string | null;
        profileName: string | null;
        profileKind: string | null;
        targetId: number | null;
        profileId: number | null;
      }
    | undefined;

  const lastSuccess = db
    .prepare(
      `SELECT pj.finished_at as finishedAt
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       WHERE pt.device_id = ? AND pj.status = 'completed'
       ORDER BY pj.finished_at DESC, pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { finishedAt: string | null } | undefined;

  const lastFailureRow = db
    .prepare(
      `SELECT pj.finished_at as finishedAt, pj.result_json as resultJson
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       WHERE pt.device_id = ? AND pj.status = 'failed'
       ORDER BY pj.finished_at DESC, pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { finishedAt: string | null; resultJson: string | null } | undefined;

  const counts = db
    .prepare(
      `SELECT
         SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successCount,
         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failureCount
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       WHERE pt.device_id = ?`
    )
    .get(deviceId) as { successCount: number; failureCount: number };

  const activeSnmp = db
    .prepare(
      `SELECT pp.name as profileName, pt.id as targetId, pp.id as profileId
       FROM poll_targets pt
       JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ? AND pt.enabled = 1 AND pp.kind = 'snmp'
       ORDER BY pt.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { profileName: string | null; targetId: number | null; profileId: number | null } | undefined;

  const hasSnmpBinding =
    (db
      .prepare(
        `SELECT COUNT(*) as cnt
         FROM poll_targets pt
         JOIN poll_profiles pp ON pp.id = pt.profile_id
         WHERE pt.device_id = ? AND pt.enabled = 1 AND pp.kind = 'snmp'`
      )
      .get(deviceId) as { cnt: number }).cnt > 0;

  const hasPingBinding =
    (db
      .prepare(
        `SELECT COUNT(*) as cnt
         FROM poll_targets pt
         JOIN poll_profiles pp ON pp.id = pt.profile_id
         WHERE pt.device_id = ? AND pt.enabled = 1 AND pp.kind = 'ping'`
      )
      .get(deviceId) as { cnt: number }).cnt > 0;

  const latestSnmp = db
    .prepare(
      `SELECT pj.status, pj.finished_at as finishedAt, pj.started_at as startedAt, pj.result_json as resultJson
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ? AND pp.kind = 'snmp'
       ORDER BY pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { status: string; finishedAt: string | null; startedAt: string | null; resultJson: string | null } | undefined;

  const lastSnmpSuccess = db
    .prepare(
      `SELECT pj.finished_at as finishedAt
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ? AND pp.kind = 'snmp' AND pj.status = 'completed'
       ORDER BY pj.finished_at DESC, pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { finishedAt: string | null } | undefined;

  const lastSnmpFailureRow = db
    .prepare(
      `SELECT pj.finished_at as finishedAt, pj.result_json as resultJson
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ? AND pp.kind = 'snmp' AND pj.status = 'failed'
       ORDER BY pj.finished_at DESC, pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { finishedAt: string | null; resultJson: string | null } | undefined;

  const lastPingSuccess = db
    .prepare(
      `SELECT pj.finished_at as finishedAt
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ? AND pp.kind = 'ping' AND pj.status = 'completed'
       ORDER BY pj.finished_at DESC, pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { finishedAt: string | null } | undefined;

  const lastPingFailure = db
    .prepare(
      `SELECT pj.finished_at as finishedAt
       FROM poll_jobs pj
       JOIN poll_targets pt ON pt.id = pj.target_id
       JOIN poll_profiles pp ON pp.id = pt.profile_id
       WHERE pt.device_id = ? AND pp.kind = 'ping' AND pj.status = 'failed'
       ORDER BY pj.finished_at DESC, pj.updated_at DESC
       LIMIT 1`
    )
    .get(deviceId) as { finishedAt: string | null } | undefined;

  const parseResult = (json: string | null) => {
    try {
      return json ? JSON.parse(json) : null;
    } catch {
      return null;
    }
  };

  const lastPollAt = latest?.finishedAt ?? latest?.startedAt ?? latest?.scheduledAt ?? latest?.createdAt ?? null;
  const lastErrorObj = parseResult(lastFailureRow?.resultJson ?? latest?.resultJson ?? null);
  const lastError =
    lastErrorObj?.error || lastErrorObj?.message || lastErrorObj?.summary || (typeof lastErrorObj === "string" ? lastErrorObj : null);
  const lastSnmpErrorObj = parseResult(lastSnmpFailureRow?.resultJson ?? latestSnmp?.resultJson ?? null);
  const lastSnmpError =
    lastSnmpErrorObj?.error ||
    lastSnmpErrorObj?.message ||
    lastSnmpErrorObj?.summary ||
    (typeof lastSnmpErrorObj === "string" ? lastSnmpErrorObj : null);

  const cloudSyncConfigured = Boolean(cloudCfg?.xmonApiBase && cloudCfg?.xmonCollectorId && cloudCfg?.xmonApiKey);

  const toMs = (ts: string | null | undefined) => {
    if (!ts) return null;
    const ms = Date.parse(ts);
    return Number.isFinite(ms) ? ms : null;
  };
  const lastSuccessMs = toMs(lastSuccess?.finishedAt ?? null);
  const lastFailureMs = toMs(lastFailureRow?.finishedAt ?? null);
  const lastSnmpSuccessMs = toMs(lastSnmpSuccess?.finishedAt ?? null);
  const lastSnmpFailureMs = toMs(lastSnmpFailureRow?.finishedAt ?? null);
  const activeLastError = lastSuccessMs !== null && lastFailureMs !== null && lastSuccessMs > lastFailureMs ? null : lastError;
  const activeLastSnmpError =
    lastSnmpSuccessMs !== null && lastSnmpFailureMs !== null && lastSnmpSuccessMs > lastSnmpFailureMs ? null : lastSnmpError;

  return {
    currentStatus: latest?.status ?? "idle",
    lastPollAt,
    lastSuccessAt: lastSuccess?.finishedAt ?? null,
    lastFailureAt: lastFailureRow?.finishedAt ?? null,
    successCount: counts?.successCount ?? 0,
    failureCount: counts?.failureCount ?? 0,
    lastError: activeLastError ?? null,
    activeSnmpProfile: activeSnmp?.profileName ?? null,
    activeSnmpPollerId: activeSnmp?.profileId ? `poller-${activeSnmp.profileId}` : null,
    hasSnmpBinding,
    hasPingBinding,
    lastSnmpPollAt: latestSnmp?.finishedAt ?? latestSnmp?.startedAt ?? null,
    lastSnmpSuccessAt: lastSnmpSuccess?.finishedAt ?? null,
    lastSnmpFailureAt: lastSnmpFailureRow?.finishedAt ?? null,
    lastSnmpError: activeLastSnmpError ?? null,
    lastPingSuccessAt: lastPingSuccess?.finishedAt ?? null,
    lastPingFailureAt: lastPingFailure?.finishedAt ?? null,
    cloudSyncConfigured,
  };
}

export function getDeviceById(db: DB, id: number): DeviceRow | null {
  const row = db
    .prepare(
      `SELECT id, hostname, ip_address as ipAddress, enabled, site, type, org, created_at as createdAt, updated_at as updatedAt
       FROM devices WHERE id = ? LIMIT 1`
    )
    .get(id) as
    | {
        id: number;
        hostname: string;
        ipAddress: string;
        enabled: number;
        site: string | null;
        type: string | null;
        org: string | null;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) return null;
  return { ...row, enabled: Boolean(row.enabled), type: row.type ?? null };
}

export function updateDevice(
  db: DB,
  input: { id: number; hostname?: string; ipAddress?: string; enabled?: boolean; site?: string | null; org?: string | null; type?: string | null }
): DeviceRow | null {
  const existing = getDeviceById(db, input.id);
  if (!existing) return null;
  const now = new Date().toISOString();
  const next = {
    hostname: input.hostname ?? existing.hostname,
    ipAddress: input.ipAddress ?? existing.ipAddress,
    enabled: input.enabled ?? existing.enabled,
    site: input.site === undefined ? existing.site : input.site,
    org: input.org === undefined ? existing.org : input.org,
    type: input.type === undefined ? existing.type : input.type,
  };
  db.prepare(
    `UPDATE devices
     SET hostname = ?, ip_address = ?, enabled = ?, site = ?, type = ?, org = ?, updated_at = ?
     WHERE id = ?`
  ).run(next.hostname, next.ipAddress, next.enabled ? 1 : 0, next.site, next.type ?? null, next.org, now, input.id);
  return getDeviceById(db, input.id);
}

export type PollProfileRow = {
  id: number;
  kind: string;
  name: string;
  intervalSec: number;
  timeoutMs: number;
  retries: number;
  enabled: boolean;
  config: any;
  createdAt: string;
  updatedAt: string;
};

export function listPollProfiles(db: DB): PollProfileRow[] {
  const rows = db
    .prepare(
      `SELECT id, kind, name, interval_sec as intervalSec, timeout_ms as timeoutMs, retries, enabled,
              config_json as configJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_profiles ORDER BY id`
    )
    .all() as Array<{
    id: number;
    kind: string;
    name: string;
    intervalSec: number;
    timeoutMs: number;
    retries: number;
    enabled: number;
    configJson: string;
    createdAt: string;
    updatedAt: string;
  }>;
  return rows.map((r) => ({
    ...r,
    enabled: Boolean(r.enabled),
    config: safeParseJson(r.configJson),
  }));
}

export function createPollProfile(db: DB, input: {
  kind: string;
  name: string;
  intervalSec: number;
  timeoutMs: number;
  retries: number;
  enabled?: boolean;
  config?: any;
}): PollProfileRow {
  const now = new Date().toISOString();
  const enabled = input.enabled ?? true;
  const configJson = JSON.stringify(input.config ?? {});
  const result = db
    .prepare(
      `INSERT INTO poll_profiles (kind, name, interval_sec, timeout_ms, retries, enabled, config_json, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(input.kind, input.name, input.intervalSec, input.timeoutMs, input.retries, enabled ? 1 : 0, configJson, now, now);
  return {
    id: Number(result.lastInsertRowid),
    kind: input.kind,
    name: input.name,
    intervalSec: input.intervalSec,
    timeoutMs: input.timeoutMs,
    retries: input.retries,
    enabled,
    config: input.config ?? {},
    createdAt: now,
    updatedAt: now,
  };
}

export function getPollProfileById(db: DB, id: number): PollProfileRow | null {
  const row = db
    .prepare(
      `SELECT id, kind, name, interval_sec as intervalSec, timeout_ms as timeoutMs, retries, enabled,
              config_json as configJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_profiles WHERE id = ? LIMIT 1`
    )
    .get(id) as
    | {
        id: number;
        kind: string;
        name: string;
        intervalSec: number;
        timeoutMs: number;
        retries: number;
        enabled: number;
        configJson: string;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) return null;
  return {
    ...row,
    enabled: Boolean(row.enabled),
    config: safeParseJson(row.configJson),
  };
}

export function findPollProfileByNameAndKind(db: DB, input: { name: string; kind: string }): PollProfileRow | null {
  const row = db
    .prepare(
      `SELECT id, kind, name, interval_sec as intervalSec, timeout_ms as timeoutMs, retries, enabled,
              config_json as configJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_profiles
       WHERE kind = ? AND name = ?
       LIMIT 1`
    )
    .get(input.kind, input.name) as
    | {
        id: number;
        kind: string;
        name: string;
        intervalSec: number;
        timeoutMs: number;
        retries: number;
        enabled: number;
        configJson: string;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) return null;
  return {
    ...row,
    enabled: Boolean(row.enabled),
    config: safeParseJson(row.configJson),
  };
}

export function updatePollProfile(
  db: DB,
  input: {
    id: number;
    name?: string;
    intervalSec?: number;
    timeoutMs?: number;
    retries?: number;
    enabled?: boolean;
    config?: any;
  }
): PollProfileRow | null {
  const existing = getPollProfileById(db, input.id);
  if (!existing) return null;
  const now = new Date().toISOString();
  const next = {
    name: input.name ?? existing.name,
    intervalSec: input.intervalSec ?? existing.intervalSec,
    timeoutMs: input.timeoutMs ?? existing.timeoutMs,
    retries: input.retries ?? existing.retries,
    enabled: input.enabled ?? existing.enabled,
    config: input.config ?? existing.config,
  };
  db.prepare(
    `UPDATE poll_profiles
     SET name = ?, interval_sec = ?, timeout_ms = ?, retries = ?, enabled = ?, config_json = ?, updated_at = ?
     WHERE id = ?`
  ).run(
    next.name,
    next.intervalSec,
    next.timeoutMs,
    next.retries,
    next.enabled ? 1 : 0,
    JSON.stringify(next.config ?? {}),
    now,
    input.id
  );
  return getPollProfileById(db, input.id);
}

export type WorkerRegistrationRow = {
  id: number;
  workerType: string;
  workerName: string;
  capabilities: any;
  lastHeartbeatAt: string;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export function listWorkerRegistrations(db: DB): WorkerRegistrationRow[] {
  const rows = db
    .prepare(
      `SELECT id, worker_type as workerType, worker_name as workerName, capabilities_json as capabilitiesJson,
              last_heartbeat_at as lastHeartbeatAt, enabled, created_at as createdAt, updated_at as updatedAt
       FROM worker_registrations ORDER BY id`
    )
    .all() as Array<{
    id: number;
    workerType: string;
    workerName: string;
    capabilitiesJson: string;
    lastHeartbeatAt: string;
    enabled: number;
    createdAt: string;
    updatedAt: string;
  }>;
  return rows.map((r) => ({
    ...r,
    enabled: Boolean(r.enabled),
    capabilities: safeParseJson(r.capabilitiesJson),
  }));
}

export function getWorkerRegistrationByName(db: DB, workerName: string): WorkerRegistrationRow | null {
  const row = db
    .prepare(
      `SELECT id, worker_type as workerType, worker_name as workerName, capabilities_json as capabilitiesJson,
              last_heartbeat_at as lastHeartbeatAt, enabled, created_at as createdAt, updated_at as updatedAt
       FROM worker_registrations
       WHERE worker_name = ?
       LIMIT 1`
    )
    .get(workerName) as
    | {
        id: number;
        workerType: string;
        workerName: string;
        capabilitiesJson: string;
        lastHeartbeatAt: string;
        enabled: number;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) return null;
  return {
    ...row,
    enabled: Boolean(row.enabled),
    capabilities: safeParseJson(row.capabilitiesJson),
  };
}

export function updateWorkerRegistrationEnabled(db: DB, workerName: string, enabled: boolean): WorkerRegistrationRow | null {
  const existing = getWorkerRegistrationByName(db, workerName);
  if (!existing) return null;
  const now = new Date().toISOString();
  db.prepare(`UPDATE worker_registrations SET enabled = ?, updated_at = ? WHERE worker_name = ?`).run(
    enabled ? 1 : 0,
    now,
    workerName
  );
  return { ...existing, enabled, updatedAt: now };
}

export function getWorkerMetricsSnapshot(
  db: DB,
  input?: { workerName?: string; workerType?: "ping" | "snmp" }
): {
  workers: Array<{
    workerName: string;
    workerType: string;
    enabled: boolean;
    lastHeartbeatAt: string;
    runningJobs: number;
    completedJobs: number;
    failedJobs: number;
  }>;
  totals: {
    workers: number;
    runningJobs: number;
    completedJobs: number;
    failedJobs: number;
  };
  filters: {
    workerName?: string;
    workerType?: "ping" | "snmp";
  };
} {
  const nameFilter = input?.workerName;
  const typeFilter = input?.workerType;

  const workers = listWorkerRegistrations(db).filter((w) => {
    if (nameFilter && w.workerName !== nameFilter) return false;
    if (typeFilter && w.workerType !== typeFilter) return false;
    return true;
  });

  const countsFor = (workerName: string, status: "running" | "completed" | "failed") => {
    const row = db
      .prepare(
        `SELECT COUNT(*) as count
         FROM poll_jobs
         WHERE status = ? AND lease_owner = ?`
      )
      .get(status, workerName) as { count: number };
    return row?.count ?? 0;
  };

  const workersWithCounts = workers.map((w) => {
    const running = countsFor(w.workerName, "running");
    const completed = countsFor(w.workerName, "completed");
    const failed = countsFor(w.workerName, "failed");
    return {
      workerName: w.workerName,
      workerType: w.workerType,
      enabled: w.enabled,
      lastHeartbeatAt: w.lastHeartbeatAt,
      runningJobs: running,
      completedJobs: completed,
      failedJobs: failed,
    };
  });

  const totals = workersWithCounts.reduce(
    (acc, w) => {
      acc.workers += 1;
      acc.runningJobs += w.runningJobs;
      acc.completedJobs += w.completedJobs;
      acc.failedJobs += w.failedJobs;
      return acc;
    },
    { workers: 0, runningJobs: 0, completedJobs: 0, failedJobs: 0 }
  );

  return {
    workers: workersWithCounts,
    totals,
    filters: {
      ...(nameFilter ? { workerName: nameFilter } : {}),
      ...(typeFilter ? { workerType: typeFilter } : {}),
    },
  };
}

export function getWorkerMetricsSummaryByType(
  db: DB
): {
  types: Array<{
    workerType: "ping" | "snmp";
    workers: number;
    enabledWorkers: number;
    disabledWorkers: number;
    runningJobs: number;
    completedJobs: number;
    failedJobs: number;
  }>;
  totals: {
    workers: number;
    enabledWorkers: number;
    disabledWorkers: number;
    runningJobs: number;
    completedJobs: number;
    failedJobs: number;
  };
} {
  const types: Array<"ping" | "snmp"> = ["ping", "snmp"];
  const allWorkers = listWorkerRegistrations(db).filter((w) => types.includes(w.workerType as "ping" | "snmp"));

  const countJobs = (names: string[], status: "running" | "completed" | "failed") => {
    if (!names.length) return 0;
    const placeholders = names.map(() => "?").join(",");
    const row = db
      .prepare(
        `SELECT COUNT(*) as count
         FROM poll_jobs
         WHERE status = ? AND lease_owner IN (${placeholders})`
      )
      .get(status, ...names) as { count: number };
    return row?.count ?? 0;
  };

  const summaries = types.map((t) => {
    const workersOfType = allWorkers.filter((w) => w.workerType === t);
    const names = workersOfType.map((w) => w.workerName);
    return {
      workerType: t,
      workers: workersOfType.length,
      enabledWorkers: workersOfType.filter((w) => w.enabled).length,
      disabledWorkers: workersOfType.filter((w) => !w.enabled).length,
      runningJobs: countJobs(names, "running"),
      completedJobs: countJobs(names, "completed"),
      failedJobs: countJobs(names, "failed"),
    };
  });

  const totals = summaries.reduce(
    (acc, s) => {
      acc.workers += s.workers;
      acc.enabledWorkers += s.enabledWorkers;
      acc.disabledWorkers += s.disabledWorkers;
      acc.runningJobs += s.runningJobs;
      acc.completedJobs += s.completedJobs;
      acc.failedJobs += s.failedJobs;
      return acc;
    },
    { workers: 0, enabledWorkers: 0, disabledWorkers: 0, runningJobs: 0, completedJobs: 0, failedJobs: 0 }
  );

  return { types: summaries, totals };
}

export function getWorkerExecutionSummary(
  db: DB,
  input?: { workerType?: "ping" | "snmp"; sinceSeconds?: number }
): {
  types: Array<{
    workerType: "ping" | "snmp";
    totalFinishedJobs: number;
    successfulJobs: number;
    failedJobs: number;
    avgLatencyMs: number | null;
    lastProcessedAt: string | null;
  }>;
  totals: {
    totalFinishedJobs: number;
    successfulJobs: number;
    failedJobs: number;
  };
  filters: {
    workerType?: "ping" | "snmp";
    sinceSeconds?: number;
  };
} {
  const types: Array<"ping" | "snmp"> = input?.workerType ? [input.workerType] : ["ping", "snmp"];
  const sinceThreshold =
    input?.sinceSeconds && input.sinceSeconds > 0 ? new Date(Date.now() - input.sinceSeconds * 1000).toISOString() : null;

  const rows = db
    .prepare(
      `SELECT status, result_json as resultJson, updated_at as updatedAt
       FROM poll_jobs
       WHERE status IN ('completed', 'failed') ${sinceThreshold ? "AND updated_at >= ?" : ""}`
    )
    .all(...(sinceThreshold ? [sinceThreshold] : [])) as Array<{ status: string; resultJson: string | null; updatedAt: string }>;

  const summaryByType = new Map<
    "ping" | "snmp",
    {
      totalFinishedJobs: number;
      successfulJobs: number;
      failedJobs: number;
      latencies: number[];
      lastProcessedAt: string | null;
    }
  >();

  types.forEach((t) => {
    summaryByType.set(t, {
      totalFinishedJobs: 0,
      successfulJobs: 0,
      failedJobs: 0,
      latencies: [],
      lastProcessedAt: null,
    });
  });

  rows.forEach((r) => {
    const parsed = safeParseJson(r.resultJson ?? "{}");
    const workerType = parsed?.workerType;
    if (workerType !== "ping" && workerType !== "snmp") return;
    if (!summaryByType.has(workerType)) return;

    const bucket = summaryByType.get(workerType)!;
    bucket.totalFinishedJobs += 1;
    const successFlag = parsed?.success === true || r.status === "completed";
    const failureFlag = parsed?.success === false || r.status === "failed";
    if (successFlag) bucket.successfulJobs += 1;
    if (failureFlag) bucket.failedJobs += 1;

    if (Number.isFinite(parsed?.latencyMs)) {
      bucket.latencies.push(Number(parsed.latencyMs));
    }

    const processedAt = parsed?.processedAt ?? r.updatedAt;
    if (processedAt && (!bucket.lastProcessedAt || processedAt > bucket.lastProcessedAt)) {
      bucket.lastProcessedAt = processedAt;
    }
  });

  const typesArray = types.map((t) => {
    const b = summaryByType.get(t)!;
    const avgLatencyMs = b.latencies.length
      ? b.latencies.reduce((a, v) => a + v, 0) / b.latencies.length
      : null;
    return {
      workerType: t,
      totalFinishedJobs: b.totalFinishedJobs,
      successfulJobs: b.successfulJobs,
      failedJobs: b.failedJobs,
      avgLatencyMs: avgLatencyMs !== null ? Number(avgLatencyMs.toFixed(2)) : null,
      lastProcessedAt: b.lastProcessedAt,
    };
  });

  const totals = typesArray.reduce(
    (acc, t) => {
      acc.totalFinishedJobs += t.totalFinishedJobs;
      acc.successfulJobs += t.successfulJobs;
      acc.failedJobs += t.failedJobs;
      return acc;
    },
    { totalFinishedJobs: 0, successfulJobs: 0, failedJobs: 0 }
  );

  return {
    types: typesArray,
    totals,
    filters: {
      ...(input?.workerType ? { workerType: input.workerType } : {}),
      ...(input?.sinceSeconds ? { sinceSeconds: input.sinceSeconds } : {}),
    },
  };
}

// --- Topology discovery storage helpers ---

export function saveSnmpSystemSnapshot(db: DB, input: {
  deviceId: number;
  system: { sysName?: string | null; sysDescr?: string | null; sysObjectId?: string | null; sysUpTime?: string | null };
  collectedAt: string;
}) {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO snmp_system_snapshots (device_id, sys_name, sys_descr, sys_object_id, sys_uptime, collected_at, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)`
  ).run(
    input.deviceId,
    input.system.sysName ?? null,
    input.system.sysDescr ?? null,
    input.system.sysObjectId ?? null,
    input.system.sysUpTime ?? null,
    input.collectedAt,
    now
  );
}

export function replaceInterfaceSnapshotsForDevice(
  db: DB,
  deviceId: number,
  interfaces: Array<{
    ifIndex?: number | null;
    ifName?: string | null;
    ifDescr?: string | null;
    ifAlias?: string | null;
    adminStatus?: string | null;
    operStatus?: string | null;
    speed?: number | null;
    mtu?: number | null;
    mac?: string | null;
    bpsIn?: number | null;
    bpsOut?: number | null;
    utilIn?: number | null;
    utilOut?: number | null;
    utilAvg?: number | null;
    rateCollectedAt?: string | null;
    ifType?: number | null;
  }>,
  collectedAt: string
) {
  const now = new Date().toISOString();
  const tx = db.transaction(() => {
    db.prepare(`DELETE FROM interface_snapshots WHERE device_id = ?`).run(deviceId);
    const stmt = db.prepare(
      `INSERT INTO interface_snapshots (device_id, if_index, if_type, if_name, if_descr, if_alias, admin_status, oper_status, speed, mtu, mac, bps_in, bps_out, util_in, util_out, util_avg, rate_collected_at, collected_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    );
    for (const intf of interfaces) {
      stmt.run(
        deviceId,
        intf.ifIndex ?? null,
        intf.ifType ?? null,
        intf.ifName ?? null,
        intf.ifDescr ?? null,
        intf.ifAlias ?? null,
        intf.adminStatus ?? null,
        intf.operStatus ?? null,
        intf.speed ?? null,
        intf.mtu ?? null,
        intf.mac ?? null,
        intf.bpsIn ?? null,
        intf.bpsOut ?? null,
        intf.utilIn ?? null,
        intf.utilOut ?? null,
        intf.utilAvg ?? null,
        intf.rateCollectedAt ?? null,
        collectedAt,
        now
      );
    }
  });
  tx();
}

export function replaceLldpNeighborsForDevice(
  db: DB,
  deviceId: number,
  neighbors: Array<{
    localPort?: string | null;
    remoteSysName?: string | null;
    remotePortId?: string | null;
    remoteChassisId?: string | null;
    remoteMgmtIp?: string | null;
  }>,
  collectedAt: string
) {
  const now = new Date().toISOString();
  const tx = db.transaction(() => {
    db.prepare(`DELETE FROM lldp_neighbors WHERE device_id = ?`).run(deviceId);
    const stmt = db.prepare(
      `INSERT INTO lldp_neighbors (device_id, local_port, remote_sys_name, remote_port_id, remote_chassis_id, remote_mgmt_ip, collected_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    );
    for (const n of neighbors) {
      stmt.run(
        deviceId,
        n.localPort ?? null,
        n.remoteSysName ?? null,
        n.remotePortId ?? null,
        n.remoteChassisId ?? null,
        n.remoteMgmtIp ?? null,
        collectedAt,
        now
      );
    }
  });
  tx();
}

export function upsertDiscoveredDeviceCandidatesFromLldp(
  db: DB,
  sourceDeviceId: number,
  neighbors: Array<{
    remoteSysName?: string | null;
    remotePortId?: string | null;
    remoteChassisId?: string | null;
    remoteMgmtIp?: string | null;
  }>,
  collectedAt: string
) {
  const now = new Date().toISOString();
  const tx = db.transaction(() => {
    const selectStmt = db.prepare(
      `SELECT id, status FROM discovered_device_candidates
       WHERE source_device_id = ?
         AND (
               (remote_chassis_id IS NOT NULL AND remote_chassis_id = ?)
            OR (remote_chassis_id IS NULL AND remote_mgmt_ip IS NOT NULL AND remote_mgmt_ip = ?)
            OR (remote_chassis_id IS NULL AND remote_mgmt_ip IS NULL AND remote_sys_name IS NOT NULL AND remote_sys_name = ? AND remote_port_id = ?)
         )
       LIMIT 1`
    );
    const insertStmt = db.prepare(
      `INSERT INTO discovered_device_candidates
         (source_device_id, remote_sys_name, remote_chassis_id, remote_mgmt_ip, first_seen_at, last_seen_at, status, notes, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)`
    );
    const updateStmt = db.prepare(
      `UPDATE discovered_device_candidates
       SET remote_sys_name = ?, remote_chassis_id = ?, remote_mgmt_ip = ?, last_seen_at = ?, updated_at = ?
       WHERE id = ?`
    );

    for (const n of neighbors) {
      const remoteChassisId = n.remoteChassisId ?? null;
      const remoteMgmtIp = n.remoteMgmtIp ?? null;
      const remoteSysName = n.remoteSysName ?? null;
      const remotePortId = n.remotePortId ?? null;
      const found = selectStmt.get(
        sourceDeviceId,
        remoteChassisId,
        remoteMgmtIp,
        remoteSysName,
        remotePortId
      ) as { id: number; status: string } | undefined;
      if (found) {
        updateStmt.run(remoteSysName, remoteChassisId, remoteMgmtIp, collectedAt, now, found.id);
      } else {
        insertStmt.run(
          sourceDeviceId,
          remoteSysName,
          remoteChassisId,
          remoteMgmtIp,
          collectedAt,
          collectedAt,
          "candidate",
          now,
          now
        );
      }
    }
  });
  tx();
}

export function getSystemSnapshotsForDevice(db: DB, deviceId: number) {
  return db
    .prepare(
      `SELECT id, device_id as deviceId, sys_name as sysName, sys_descr as sysDescr, sys_object_id as sysObjectId,
              sys_uptime as sysUpTime, collected_at as collectedAt, created_at as createdAt
       FROM snmp_system_snapshots
       WHERE device_id = ?
       ORDER BY collected_at DESC, id DESC`
    )
    .all(deviceId) as any[];
}

export function listInterfaceSnapshotsForDevice(db: DB, deviceId: number) {
  return db
    .prepare(
      `SELECT id, device_id as deviceId, if_index as ifIndex, if_type as ifType, if_name as ifName, if_descr as ifDescr, if_alias as ifAlias,
              admin_status as adminStatus, oper_status as operStatus, speed, mtu, mac,
              bps_in as bpsIn, bps_out as bpsOut, util_in as utilIn, util_out as utilOut, util_avg as utilAvg,
              rate_collected_at as rateCollectedAt,
              collected_at as collectedAt, created_at as createdAt
       FROM interface_snapshots
       WHERE device_id = ?
       ORDER BY if_index ASC, id ASC`
    )
    .all(deviceId) as any[];
}

export function getLastInterfaceCounters(db: DB, deviceId: number) {
  return db
    .prepare(
      `SELECT if_index as ifIndex, hc_in_octets as hcIn, hc_out_octets as hcOut,
              in_octets as inOctets, out_octets as outOctets, collected_at as collectedAt
       FROM interface_counters_last
       WHERE device_id = ?`
    )
    .all(deviceId) as Array<{
      ifIndex: number;
      hcIn: number | null;
      hcOut: number | null;
      inOctets: number | null;
      outOctets: number | null;
      collectedAt: string;
    }>;
}

export function upsertLastInterfaceCounters(
  db: DB,
  deviceId: number,
  items: Array<{
    ifIndex: number;
    hcIn?: number | null;
    hcOut?: number | null;
    inOctets?: number | null;
    outOctets?: number | null;
    collectedAt: string;
  }>
) {
  const now = new Date().toISOString();
  const tx = db.transaction(() => {
    const delStmt = db.prepare(`DELETE FROM interface_counters_last WHERE device_id = ? AND if_index = ?`);
    const insStmt = db.prepare(
      `INSERT INTO interface_counters_last (device_id, if_index, hc_in_octets, hc_out_octets, in_octets, out_octets, collected_at, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
    );
    for (const item of items) {
      delStmt.run(deviceId, item.ifIndex);
      insStmt.run(
        deviceId,
        item.ifIndex,
        item.hcIn ?? null,
        item.hcOut ?? null,
        item.inOctets ?? null,
        item.outOctets ?? null,
        item.collectedAt,
        now
      );
    }
  });
  tx();
}

export function listLldpNeighborsForDevice(db: DB, deviceId: number) {
  return db
    .prepare(
      `SELECT id, device_id as deviceId, local_port as localPort, remote_sys_name as remoteSysName,
              remote_port_id as remotePortId, remote_chassis_id as remoteChassisId, remote_mgmt_ip as remoteMgmtIp,
              collected_at as collectedAt, created_at as createdAt
       FROM lldp_neighbors
       WHERE device_id = ?
       ORDER BY id ASC`
    )
    .all(deviceId) as any[];
}

export function listLldpNeighbors(db: DB, input?: { deviceId?: number }) {
  return db
    .prepare(
      `SELECT id, device_id as deviceId, local_port as localPort, remote_sys_name as remoteSysName,
              remote_port_id as remotePortId, remote_chassis_id as remoteChassisId, remote_mgmt_ip as remoteMgmtIp,
              collected_at as collectedAt, created_at as createdAt
       FROM lldp_neighbors
       ${input?.deviceId ? "WHERE device_id = ?" : ""}
       ORDER BY device_id ASC, id ASC`
    )
    .all(input?.deviceId ? input.deviceId : undefined) as any[];
}

export function getLldpNeighborById(db: DB, id: number) {
  return db
    .prepare(
      `SELECT id, device_id as deviceId, local_port as localPort, remote_sys_name as remoteSysName,
              remote_port_id as remotePortId, remote_chassis_id as remoteChassisId, remote_mgmt_ip as remoteMgmtIp,
              collected_at as collectedAt, created_at as createdAt
       FROM lldp_neighbors
       WHERE id = ?
       LIMIT 1`
    )
    .get(id) as any;
}

export function listNeighborsWithReview(db: DB, input?: { deviceId?: number; status?: string }) {
  const params: any[] = [];
  let where = "";
  if (input?.deviceId) {
    where += (where ? " AND " : "WHERE ") + "n.device_id = ?";
    params.push(input.deviceId);
  }
  if (input?.status) {
    where += (where ? " AND " : "WHERE ") + "(r.status = ?)";
    params.push(input.status);
  }
  const rows = db
    .prepare(
      `SELECT n.id, n.device_id as deviceId, n.local_port as localPort, n.remote_sys_name as remoteSysName,
              n.remote_port_id as remotePortId, n.remote_chassis_id as remoteChassisId, n.remote_mgmt_ip as remoteMgmtIp,
              n.collected_at as collectedAt, n.created_at as createdAt,
              r.status as reviewStatus, r.linked_device_id as linkedDeviceId, r.promoted_device_id as promotedDeviceId,
              r.note as reviewNote, r.updated_at as reviewUpdatedAt
       FROM lldp_neighbors n
       LEFT JOIN neighbor_reviews r ON r.neighbor_id = n.id
       ${where}
       ORDER BY n.device_id ASC, n.id ASC`
    )
    .all(...params) as any[];
  return rows.map((r) => ({
    ...r,
    reviewStatus: r.reviewStatus ?? "new",
  }));
}

export function setNeighborReview(
  db: DB,
  input: { neighborId: number; status: string; linkedDeviceId?: number | null; promotedDeviceId?: number | null; note?: string | null }
) {
  const now = new Date().toISOString();
  const existing = db
    .prepare(
      `SELECT id FROM neighbor_reviews WHERE neighbor_id = ?
       LIMIT 1`
    )
    .get(input.neighborId) as { id: number } | undefined;
  if (existing) {
    db.prepare(
      `UPDATE neighbor_reviews
       SET status = ?, linked_device_id = ?, promoted_device_id = ?, note = ?, updated_at = ?
       WHERE neighbor_id = ?`
    ).run(
      input.status,
      input.linkedDeviceId ?? null,
      input.promotedDeviceId ?? null,
      input.note ?? null,
      now,
      input.neighborId
    );
  } else {
    db.prepare(
      `INSERT INTO neighbor_reviews (neighbor_id, status, linked_device_id, promoted_device_id, note, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    ).run(
      input.neighborId,
      input.status,
      input.linkedDeviceId ?? null,
      input.promotedDeviceId ?? null,
      input.note ?? null,
      now,
      now
    );
  }
}

export function logNeighborReviewEvent(
  db: DB,
  input: { neighborId: number; action: string; actor?: string | null; deviceId?: number | null; note?: string | null }
) {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO neighbor_review_events (neighbor_id, action, actor, device_id, note, created_at)
     VALUES (?, ?, ?, ?, ?, ?)`
  ).run(input.neighborId, input.action, input.actor ?? null, input.deviceId ?? null, input.note ?? null, now);
}

export function listNeighborReviewEvents(db: DB, input?: { neighborId?: number; deviceId?: number; limit?: number }) {
  const params: any[] = [];
  let where = "";
  if (input?.neighborId) {
    where += (where ? " AND " : "WHERE ") + "neighbor_id = ?";
    params.push(input.neighborId);
  }
  if (input?.deviceId) {
    where += (where ? " AND " : "WHERE ") + "(device_id = ? OR neighbor_id IN (SELECT id FROM lldp_neighbors WHERE device_id = ?))";
    params.push(input.deviceId, input.deviceId);
  }
  const limit = input?.limit ?? 200;
  return db
    .prepare(
      `SELECT id, neighbor_id as neighborId, action, actor, device_id as deviceId, note, created_at as createdAt
       FROM neighbor_review_events
       ${where}
       ORDER BY created_at DESC
       LIMIT ${limit}`
    )
    .all(...params) as any[];
}

export function logAdminAuditEvent(
  db: DB,
  input: {
    entityType: string;
    entityId?: number | null;
    action: string;
    actor?: string | null;
    note?: string | null;
    before?: any;
    after?: any;
  }
) {
  const now = new Date().toISOString();
  db.prepare(
    `INSERT INTO admin_audit_events (entity_type, entity_id, action, actor, note, before_json, after_json, created_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  ).run(
    input.entityType,
    input.entityId ?? null,
    input.action,
    input.actor ?? null,
    input.note ?? null,
    input.before !== undefined ? JSON.stringify(input.before) : null,
    input.after !== undefined ? JSON.stringify(input.after) : null,
    now
  );
}

export function listAdminAuditEvents(
  db: DB,
  input?: { entityType?: string; entityId?: number; action?: string; limit?: number }
) {
  const params: any[] = [];
  let where = "";
  if (input?.entityType) {
    where += (where ? " AND " : "WHERE ") + "entity_type = ?";
    params.push(input.entityType);
  }
  if (input?.entityId) {
    where += (where ? " AND " : "WHERE ") + "entity_id = ?";
    params.push(input.entityId);
  }
  if (input?.action) {
    where += (where ? " AND " : "WHERE ") + "action = ?";
    params.push(input.action);
  }
  const limit = input?.limit ?? 200;
  return db
    .prepare(
      `SELECT id, entity_type as entityType, entity_id as entityId, action, actor, note,
              before_json as beforeJson, after_json as afterJson, created_at as createdAt
       FROM admin_audit_events
       ${where}
       ORDER BY created_at DESC
       LIMIT ${limit}`
    )
    .all(...params) as any[];
}

export type LicenseState = {
  status: string;
  subscriptionStatus: string;
  licenseKey: string | null;
  customer: string | null;
  entitlementStatus?: string | null;
  activatedAt?: string | null;
  validatedAt: string | null;
  expiresAt: string | null;
  graceUntil?: string | null;
  lastValidationSource?: string | null;
  lastError: string | null;
  lastErrorCode?: string | null;
  nextValidationDueAt?: string | null;
  updatedAt: string;
};

export function getLicenseState(db: DB): LicenseState {
  const row =
    (db
      .prepare(
        `SELECT status, subscription_status as subscriptionStatus, license_key as licenseKey, customer,
                entitlement_status as entitlementStatus, activated_at as activatedAt,
                validated_at as validatedAt, expires_at as expiresAt, grace_until as graceUntil,
                last_validation_source as lastValidationSource, last_error as lastError, last_error_code as lastErrorCode,
                next_validation_due_at as nextValidationDueAt, updated_at as updatedAt
         FROM license_state WHERE id = 1`
      )
      .get() as LicenseState | undefined) ??
    ({
      status: "unlicensed",
      subscriptionStatus: "inactive",
      licenseKey: null,
      customer: null,
      entitlementStatus: null,
      activatedAt: null,
      validatedAt: null,
      expiresAt: null,
      graceUntil: null,
      lastValidationSource: null,
      lastError: null,
      lastErrorCode: null,
      nextValidationDueAt: null,
      updatedAt: new Date().toISOString(),
    } as LicenseState);
  return row;
}

export function setLicenseState(
  db: DB,
  input: Partial<Omit<LicenseState, "updatedAt">> & { status: string; subscriptionStatus: string }
): LicenseState {
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE license_state
     SET status = ?, subscription_status = ?, license_key = COALESCE(?, license_key),
         customer = COALESCE(?, customer), entitlement_status = COALESCE(?, entitlement_status),
         activated_at = COALESCE(?, activated_at),
         validated_at = COALESCE(?, validated_at),
         expires_at = COALESCE(?, expires_at), grace_until = COALESCE(?, grace_until),
         last_validation_source = COALESCE(?, last_validation_source),
         last_error = COALESCE(?, last_error), last_error_code = COALESCE(?, last_error_code),
         next_validation_due_at = COALESCE(?, next_validation_due_at),
         updated_at = ?
     WHERE id = 1`
  ).run(
    input.status,
    input.subscriptionStatus,
    input.licenseKey ?? null,
    input.customer ?? null,
    input.entitlementStatus ?? null,
    input.activatedAt ?? null,
    input.validatedAt ?? null,
    input.expiresAt ?? null,
    input.graceUntil ?? null,
    input.lastValidationSource ?? null,
    input.lastError ?? null,
    input.lastErrorCode ?? null,
    input.nextValidationDueAt ?? null,
    now
  );
  return getLicenseState(db);
}

export type EffectiveLicense = {
  allowed: boolean;
  effectiveStatus: string;
  reason?: string;
  expiresAt?: string | null;
  graceUntil?: string | null;
  state: LicenseState;
};

export function evaluateLicenseState(db: DB): EffectiveLicense {
  const state = getLicenseState(db);
  const now = Date.now();
  const expires = state.expiresAt ? Date.parse(state.expiresAt) : null;
  const grace = state.graceUntil ? Date.parse(state.graceUntil) : null;

  const isActive = state.status === "active" && state.subscriptionStatus === "active";
  const inGrace = !isActive && grace !== null && grace > now;
  let allowed = isActive || inGrace;
  let reason = allowed ? undefined : "license_required";
  let effectiveStatus = "restricted";

  if (isActive) {
    effectiveStatus = "active";
  } else if (inGrace) {
    effectiveStatus = "grace";
    reason = "license_grace_active";
  } else if (state.status === "revoked") {
    reason = "license_revoked";
    effectiveStatus = "revoked";
  } else if (state.status === "invalid") {
    reason = "license_invalid";
    effectiveStatus = "invalid";
  } else if (state.status === "expired") {
    reason = "license_expired";
    effectiveStatus = "expired";
  } else if (state.subscriptionStatus === "inactive") {
    reason = "paid_subscription_required";
    effectiveStatus = "inactive_subscription";
  } else if (state.status === "unlicensed") {
    reason = "license_required";
    effectiveStatus = "unlicensed";
  }

  return {
    allowed,
    effectiveStatus,
    reason,
    expiresAt: state.expiresAt,
    graceUntil: state.graceUntil,
    state,
  };
}

export function licenseAllowsCollection(db: DB): EffectiveLicense {
  return evaluateLicenseState(db);
}

export function listDiscoveredCandidatesForSourceDevice(db: DB, sourceDeviceId: number) {
  return db
    .prepare(
      `SELECT id, source_device_id as sourceDeviceId, remote_sys_name as remoteSysName, remote_chassis_id as remoteChassisId,
              remote_mgmt_ip as remoteMgmtIp, first_seen_at as firstSeenAt, last_seen_at as lastSeenAt,
              status, notes, created_at as createdAt, updated_at as updatedAt
       FROM discovered_device_candidates
       WHERE source_device_id = ?
       ORDER BY last_seen_at DESC, id DESC`
    )
    .all(sourceDeviceId) as any[];
}

export function upsertWorkerRegistration(db: DB, input: {
  workerType: string;
  workerName: string;
  capabilities?: any;
  enabled?: boolean;
}): WorkerRegistrationRow {
  const now = new Date().toISOString();
  const enabled = input.enabled ?? true;
  const capabilitiesJson = JSON.stringify(input.capabilities ?? {});
  const existing = db
    .prepare(
      `SELECT id FROM worker_registrations WHERE worker_name = ? LIMIT 1`
    )
    .get(input.workerName) as { id: number } | undefined;

  if (existing) {
    db.prepare(
      `UPDATE worker_registrations
       SET worker_type = ?, capabilities_json = ?, last_heartbeat_at = ?, enabled = ?, updated_at = ?
       WHERE worker_name = ?`
    ).run(input.workerType, capabilitiesJson, now, enabled ? 1 : 0, now, input.workerName);
    const row = db
      .prepare(
        `SELECT id, worker_type as workerType, worker_name as workerName, capabilities_json as capabilitiesJson,
                last_heartbeat_at as lastHeartbeatAt, enabled, created_at as createdAt, updated_at as updatedAt
         FROM worker_registrations WHERE worker_name = ? LIMIT 1`
      )
      .get(input.workerName) as any;
    return {
      id: row.id,
      workerType: row.workerType,
      workerName: row.workerName,
      capabilities: safeParseJson(row.capabilitiesJson),
      lastHeartbeatAt: row.lastHeartbeatAt,
      enabled: Boolean(row.enabled),
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    };
  }

  const result = db
    .prepare(
      `INSERT INTO worker_registrations (worker_type, worker_name, capabilities_json, last_heartbeat_at, enabled, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .run(input.workerType, input.workerName, capabilitiesJson, now, enabled ? 1 : 0, now, now);

  return {
    id: Number(result.lastInsertRowid),
    workerType: input.workerType,
    workerName: input.workerName,
    capabilities: input.capabilities ?? {},
    lastHeartbeatAt: now,
    enabled,
    createdAt: now,
    updatedAt: now,
  };
}

export function heartbeatWorkerRegistration(db: DB, workerName: string): WorkerRegistrationRow | null {
  const existing = db
    .prepare(
      `SELECT id, worker_type as workerType, worker_name as workerName, capabilities_json as capabilitiesJson,
              last_heartbeat_at as lastHeartbeatAt, enabled, created_at as createdAt, updated_at as updatedAt
       FROM worker_registrations WHERE worker_name = ? LIMIT 1`
    )
    .get(workerName) as any;
  if (!existing) return null;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE worker_registrations SET last_heartbeat_at = ?, updated_at = ? WHERE worker_name = ?`
  ).run(now, now, workerName);
  return {
    id: existing.id,
    workerType: existing.workerType,
    workerName: existing.workerName,
    capabilities: safeParseJson(existing.capabilitiesJson),
    lastHeartbeatAt: now,
    enabled: Boolean(existing.enabled),
    createdAt: existing.createdAt,
    updatedAt: now,
  };
}

function safeParseJson(str: any): any {
  try {
    return JSON.parse(str ?? "{}");
  } catch {
    return {};
  }
}

export type PollTargetRow = {
  id: number;
  deviceId: number;
  profileId: number;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
};

export function listPollTargets(db: DB, input?: { deviceId?: number; profileId?: number }): PollTargetRow[] {
  const conditions: string[] = [];
  const params: any[] = [];
  if (input?.deviceId !== undefined) {
    conditions.push("device_id = ?");
    params.push(input.deviceId);
  }
  if (input?.profileId !== undefined) {
    conditions.push("profile_id = ?");
    params.push(input.profileId);
  }
  const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";
  const rows = db
    .prepare(
      `SELECT id, device_id as deviceId, profile_id as profileId, enabled,
              created_at as createdAt, updated_at as updatedAt
       FROM poll_targets ${where} ORDER BY id`
    )
    .all(...params) as Array<{
    id: number;
    deviceId: number;
    profileId: number;
    enabled: number;
    createdAt: string;
    updatedAt: string;
  }>;
  return rows.map((r) => ({ ...r, enabled: Boolean(r.enabled) }));
}

export function listEnabledPollTargets(db: DB): PollTargetRow[] {
  if (!licenseAllowsCollection(db).allowed) return [];
  const rows = db
    .prepare(
      `SELECT id, device_id as deviceId, profile_id as profileId, enabled,
              created_at as createdAt, updated_at as updatedAt
       FROM poll_targets WHERE enabled = 1 ORDER BY id`
    )
    .all() as Array<{
    id: number;
    deviceId: number;
    profileId: number;
    enabled: number;
    createdAt: string;
    updatedAt: string;
  }>;
  return rows.map((r) => ({ ...r, enabled: Boolean(r.enabled) }));
}

export function getPollTargetById(db: DB, id: number): PollTargetRow | null {
  const row = db
    .prepare(
      `SELECT id, device_id as deviceId, profile_id as profileId, enabled,
              created_at as createdAt, updated_at as updatedAt
       FROM poll_targets WHERE id = ? LIMIT 1`
    )
    .get(id) as
    | {
        id: number;
        deviceId: number;
        profileId: number;
        enabled: number;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) return null;
  return { ...row, enabled: Boolean(row.enabled) };
}

export function createPollTarget(db: DB, input: { deviceId: number; profileId: number; enabled?: boolean }): PollTargetRow {
  const now = new Date().toISOString();
  const enabled = input.enabled ?? true;
  const result = db
    .prepare(
      `INSERT INTO poll_targets (device_id, profile_id, enabled, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?)`
    )
    .run(input.deviceId, input.profileId, enabled ? 1 : 0, now, now);
  return {
    id: Number(result.lastInsertRowid),
    deviceId: input.deviceId,
    profileId: input.profileId,
    enabled,
    createdAt: now,
    updatedAt: now,
  };
}

export function findPollTargetByDeviceProfile(db: DB, deviceId: number, profileId: number): PollTargetRow | null {
  const row = db
    .prepare(
      `SELECT id, device_id as deviceId, profile_id as profileId, enabled, created_at as createdAt, updated_at as updatedAt
       FROM poll_targets
       WHERE device_id = ? AND profile_id = ?
       LIMIT 1`
    )
    .get(deviceId, profileId) as
    | {
        id: number;
        deviceId: number;
        profileId: number;
        enabled: number;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  return row ? { ...row, enabled: Boolean(row.enabled) } : null;
}

function assertLicense(db: DB) {
  const lic = licenseAllowsCollection(db);
  if (!lic.allowed) {
    const err = new Error(lic.reason ?? "license_required");
    throw err;
  }
}

export function updatePollTarget(db: DB, input: { id: number; enabled?: boolean }): PollTargetRow | null {
  const existing = getPollTargetById(db, input.id);
  if (!existing) return null;
  const now = new Date().toISOString();
  const enabled = input.enabled ?? existing.enabled;
  db.prepare(`UPDATE poll_targets SET enabled = ?, updated_at = ? WHERE id = ?`).run(enabled ? 1 : 0, now, input.id);
  return getPollTargetById(db, input.id);
}

export function enqueuePollJobForTarget(db: DB, targetId: number): PollJobRow {
  assertLicense(db);
  const target = getPollTargetById(db, targetId);
  if (!target) throw new Error("target_not_found");
  return createPollJob(db, { targetId });
}

export function enqueuePollJobsForTargets(db: DB, targetIds: number[]): PollJobRow[] {
  assertLicense(db);
  const tx = db.transaction((ids: number[]) => ids.map((id) => enqueuePollJobForTarget(db, id)));
  return tx(targetIds);
}

export type PollJobRow = {
  id: number;
  targetId: number;
  status: string;
  scheduledAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  leaseOwner: string | null;
  attemptCount: number;
  result: any | null;
  createdAt: string;
  updatedAt: string;
};

type PollJobRowRaw = {
  id: number;
  targetId: number;
  status: string;
  scheduledAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  leaseOwner: string | null;
  attemptCount: number;
  resultJson: string | null;
  createdAt: string;
  updatedAt: string;
};

function mapPollJobRow(row: PollJobRowRaw): PollJobRow {
  return {
    id: row.id,
    targetId: row.targetId,
    status: row.status,
    scheduledAt: row.scheduledAt,
    startedAt: row.startedAt,
    finishedAt: row.finishedAt,
    leaseOwner: row.leaseOwner ?? null,
    attemptCount: row.attemptCount,
    result: safeParseJson(row.resultJson ?? null),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

export function listPollJobs(db: DB, status?: "pending" | "running" | "completed" | "failed", leaseOwner?: string | null): PollJobRow[] {
  const clauses: string[] = [];
  const params: any[] = [];

  // limit result size to keep Jobs page responsive
  const LIMIT = 500;

  if (status) {
    clauses.push("status = ?");
    params.push(status);
  }

  if (leaseOwner !== undefined) {
    clauses.push("lease_owner = ?");
    params.push(leaseOwner);
  }

  const where = clauses.length ? ` WHERE ${clauses.join(" AND ")}` : "";
  const rows = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs${where} ORDER BY id DESC LIMIT ${LIMIT}`
    )
    .all(...params) as PollJobRowRaw[];
  return rows.map(mapPollJobRow);
}

export function getPollJobById(db: DB, id: number): PollJobRow | null {
  const row = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs WHERE id = ? LIMIT 1`
    )
    .get(id) as PollJobRowRaw | undefined;
  if (!row) return null;
  return mapPollJobRow(row);
}

export function claimNextPendingPollJob(db: DB, leaseOwner: string): PollJobRow | null {
  if (!licenseAllowsCollection(db).allowed) return null;
  const now = new Date().toISOString();

  const tx = db.transaction(() => {
    const row = db
      .prepare(
        `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
                started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
                attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
         FROM poll_jobs
         WHERE status = 'pending'
         ORDER BY id
         LIMIT 1`
      )
      .get() as any;

    if (!row) return null;

    db.prepare(
      `UPDATE poll_jobs
       SET status = ?, lease_owner = ?, started_at = ?, attempt_count = attempt_count + 1, updated_at = ?
       WHERE id = ? AND status = 'pending'`
    ).run("running", leaseOwner, now, now, row.id);

    const claimed = db
      .prepare(
        `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
                started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
                attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
         FROM poll_jobs
         WHERE id = ? LIMIT 1`
      )
      .get(row.id) as any;

    return claimed ? { ...claimed, result: safeParseJson(claimed.resultJson ?? null), leaseOwner: claimed.leaseOwner ?? null } as PollJobRow : null;
  });

  return tx();
}

export function claimPollJobById(db: DB, input: { jobId: number; leaseOwner: string }): PollJobRow | null {
  const existing = getPollJobById(db, input.jobId);
  if (!existing || existing.status !== "pending") return null;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE poll_jobs
     SET status = 'running', lease_owner = ?, started_at = ?, attempt_count = attempt_count + 1, updated_at = ?
     WHERE id = ? AND status = 'pending'`
  ).run(input.leaseOwner, now, now, input.jobId);
  return getPollJobById(db, input.jobId);
}

export function claimPendingPollJobsBatch(
  db: DB,
  input: {
    workerName: string;
    supportedKinds: string[];
    limit: number;
    shardHint?: number;
  }
): PollJobRow[] {
  if (!licenseAllowsCollection(db).allowed) return [];
  if (!input.supportedKinds.length || input.limit <= 0) return [];
  const now = new Date().toISOString();
  const kindsPlaceholders = input.supportedKinds.map(() => "?").join(",");
  const tx = db.transaction(() => {
    const rows = db
      .prepare(
        `SELECT pj.id
         FROM poll_jobs pj
         JOIN poll_targets pt ON pj.target_id = pt.id
         JOIN poll_profiles pp ON pt.profile_id = pp.id
         WHERE pj.status = 'pending' AND pp.kind IN (${kindsPlaceholders})
         ORDER BY pj.id
         LIMIT ?`
      )
      .all(...input.supportedKinds, input.limit) as Array<{ id: number }>;

    if (!rows.length) return [] as PollJobRow[];

    const ids = rows.map((r) => r.id);
    const placeholders = ids.map(() => "?").join(",");

    db.prepare(
      `UPDATE poll_jobs
       SET status = 'running', lease_owner = ?, started_at = ?, attempt_count = attempt_count + 1, updated_at = ?
       WHERE id IN (${placeholders}) AND status = 'pending'`
    ).run(input.workerName, now, now, ...ids);

    const claimed = db
      .prepare(
        `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
                started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
                attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
         FROM poll_jobs
         WHERE id IN (${placeholders})
         ORDER BY id`
      )
      .all(...ids) as PollJobRowRaw[];

    return claimed.map(mapPollJobRow);
  });

  return tx();
}

export function retryPollJob(db: DB, jobId: number): PollJobRow | null {
  const existing = getPollJobById(db, jobId);
  if (!existing || existing.status !== "failed") return null;
  return createPollJob(db, { targetId: existing.targetId });
}

export function finishPollJob(db: DB, input: { jobId: number; status: "completed" | "failed"; result?: any | null }): PollJobRow | null {
  const existing = getPollJobById(db, input.jobId);
  if (!existing || existing.status !== "running") return null;
  const now = new Date().toISOString();
  const resultJson = JSON.stringify(input.result ?? null);
  db.prepare(
    `UPDATE poll_jobs
     SET status = ?, finished_at = ?, result_json = ?, updated_at = ?
     WHERE id = ? AND status = 'running'`
  ).run(input.status, now, resultJson, now, input.jobId);
  return getPollJobById(db, input.jobId);
}

export function releasePollJob(db: DB, jobId: number): PollJobRow | null {
  const existing = getPollJobById(db, jobId);
  if (!existing || existing.status !== "running") return null;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE poll_jobs
     SET status = 'pending', lease_owner = NULL, started_at = NULL, finished_at = NULL, result_json = NULL, updated_at = ?
     WHERE id = ? AND status = 'running'`
  ).run(now, jobId);
  return getPollJobById(db, jobId);
}

export function heartbeatPollJob(db: DB, input: { jobId: number; leaseOwner: string }): PollJobRow | null {
  const existing = getPollJobById(db, input.jobId);
  if (!existing || existing.status !== "running") return null;
  if ((existing.leaseOwner ?? null) !== input.leaseOwner) return null;
  const now = new Date().toISOString();
  db.prepare(`UPDATE poll_jobs SET updated_at = ? WHERE id = ?`).run(now, input.jobId);
  return getPollJobById(db, input.jobId);
}

export function abandonPollJob(db: DB, input: { jobId: number; leaseOwner: string; result?: any | null }): PollJobRow | null {
  const existing = getPollJobById(db, input.jobId);
  if (!existing || existing.status !== "running") return null;
  if ((existing.leaseOwner ?? null) !== input.leaseOwner) return null;
  const now = new Date().toISOString();
  const resultJson = JSON.stringify(input.result ?? null);
  db.prepare(
    `UPDATE poll_jobs
     SET status = 'failed', finished_at = ?, result_json = ?, updated_at = ?
     WHERE id = ? AND status = 'running'`
  ).run(now, resultJson, now, input.jobId);
  return getPollJobById(db, input.jobId);
}

export function unclaimPollJobById(db: DB, input: { jobId: number; leaseOwner: string }): PollJobRow | null {
  const existing = getPollJobById(db, input.jobId);
  if (!existing || existing.status !== "running") return null;
  if ((existing.leaseOwner ?? null) !== input.leaseOwner) return null;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE poll_jobs
     SET status = 'pending', lease_owner = NULL, started_at = NULL, finished_at = NULL, result_json = NULL, updated_at = ?
     WHERE id = ? AND status = 'running'`
  ).run(now, input.jobId);
  return getPollJobById(db, input.jobId);
}

export function getRunningPollJobForLeaseOwner(db: DB, leaseOwner: string): PollJobRow | null {
  const row = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs
       WHERE status = 'running' AND lease_owner = ?
       ORDER BY id ASC
       LIMIT 1`
    )
    .get(leaseOwner) as PollJobRowRaw | undefined;
  return row ? mapPollJobRow(row) : null;
}

export function getPollJobDetail(
  db: DB,
  jobId: number
): {
  job: PollJobRow;
  target: PollTargetRow;
  device: DeviceRow;
  profile: PollProfileRow;
} | null {
  const row = db
    .prepare(
      `SELECT
         pj.id as jobId, pj.target_id as jobTargetId, pj.status as jobStatus, pj.scheduled_at as jobScheduledAt,
         pj.started_at as jobStartedAt, pj.finished_at as jobFinishedAt, pj.lease_owner as jobLeaseOwner,
         pj.result_json as jobResultJson, pj.attempt_count as jobAttemptCount, pj.created_at as jobCreatedAt, pj.updated_at as jobUpdatedAt,
         pt.id as targetId, pt.device_id as targetDeviceId, pt.profile_id as targetProfileId, pt.enabled as targetEnabled,
         pt.created_at as targetCreatedAt, pt.updated_at as targetUpdatedAt,
         d.id as deviceId, d.hostname as deviceHostname, d.ip_address as deviceIpAddress, d.enabled as deviceEnabled,
         d.site as deviceSite, d.type as deviceType, d.org as deviceOrg, d.created_at as deviceCreatedAt, d.updated_at as deviceUpdatedAt,
         pp.id as profileId, pp.kind as profileKind, pp.name as profileName, pp.interval_sec as profileIntervalSec,
         pp.timeout_ms as profileTimeoutMs, pp.retries as profileRetries, pp.enabled as profileEnabled,
         pp.config_json as profileConfigJson, pp.created_at as profileCreatedAt, pp.updated_at as profileUpdatedAt
       FROM poll_jobs pj
       JOIN poll_targets pt ON pj.target_id = pt.id
       JOIN devices d ON pt.device_id = d.id
       JOIN poll_profiles pp ON pt.profile_id = pp.id
       WHERE pj.id = ?
       LIMIT 1`
    )
    .get(jobId) as
    | {
        jobId: number;
        jobTargetId: number;
        jobStatus: string;
        jobScheduledAt: string | null;
        jobStartedAt: string | null;
        jobFinishedAt: string | null;
        jobLeaseOwner: string | null;
        jobResultJson: string | null;
        jobAttemptCount: number;
        jobCreatedAt: string;
        jobUpdatedAt: string;
        targetId: number;
        targetDeviceId: number;
        targetProfileId: number;
        targetEnabled: number;
        targetCreatedAt: string;
        targetUpdatedAt: string;
        deviceId: number;
        deviceHostname: string;
        deviceIpAddress: string;
        deviceEnabled: number;
        deviceSite: string | null;
        deviceType: string | null;
        deviceOrg: string | null;
        deviceCreatedAt: string;
        deviceUpdatedAt: string;
        profileId: number;
        profileKind: string;
        profileName: string;
        profileIntervalSec: number;
        profileTimeoutMs: number;
        profileRetries: number;
        profileEnabled: number;
        profileConfigJson: string;
        profileCreatedAt: string;
        profileUpdatedAt: string;
      }
    | undefined;

  if (!row) return null;

  const job: PollJobRow = mapPollJobRow({
    id: row.jobId,
    targetId: row.jobTargetId,
    status: row.jobStatus,
    scheduledAt: row.jobScheduledAt,
    startedAt: row.jobStartedAt,
    finishedAt: row.jobFinishedAt,
    leaseOwner: row.jobLeaseOwner ?? null,
    resultJson: row.jobResultJson ?? null,
    attemptCount: row.jobAttemptCount,
    createdAt: row.jobCreatedAt,
    updatedAt: row.jobUpdatedAt,
  });

  const target: PollTargetRow = {
    id: row.targetId,
    deviceId: row.targetDeviceId,
    profileId: row.targetProfileId,
    enabled: Boolean(row.targetEnabled),
    createdAt: row.targetCreatedAt,
    updatedAt: row.targetUpdatedAt,
  };

  const device: DeviceRow = {
    id: row.deviceId,
    hostname: row.deviceHostname,
    ipAddress: row.deviceIpAddress,
    enabled: Boolean(row.deviceEnabled),
    site: row.deviceSite ?? null,
    type: row.deviceType ?? null,
    org: row.deviceOrg ?? null,
    createdAt: row.deviceCreatedAt,
    updatedAt: row.deviceUpdatedAt,
  };

  const profile: PollProfileRow = {
    id: row.profileId,
    kind: row.profileKind,
    name: row.profileName,
    intervalSec: row.profileIntervalSec,
    timeoutMs: row.profileTimeoutMs,
    retries: row.profileRetries,
    enabled: Boolean(row.profileEnabled),
    config: safeParseJson(row.profileConfigJson ?? "{}"),
    createdAt: row.profileCreatedAt,
    updatedAt: row.profileUpdatedAt,
  };

  return { job, target, device, profile };
}

export function getRunningPollJobDetailForLeaseOwner(
  db: DB,
  leaseOwner: string
): {
  job: PollJobRow;
  target: PollTargetRow;
  device: DeviceRow;
  profile: PollProfileRow;
} | null {
  const row = db
    .prepare(
      `SELECT id FROM poll_jobs
       WHERE status = 'running' AND lease_owner = ?
       ORDER BY id ASC
       LIMIT 1`
    )
    .get(leaseOwner) as { id: number } | undefined;
  if (!row) return null;
  return getPollJobDetail(db, row.id);
}

export function listStaleRunningPollJobs(db: DB, olderThanSeconds: number): PollJobRow[] {
  const threshold = new Date(Date.now() - olderThanSeconds * 1000).toISOString();
  const rows = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs
       WHERE status = 'running' AND updated_at < ?
       ORDER BY id`
    )
    .all(threshold) as any[];
  return rows.map((r) => ({ ...r, result: safeParseJson(r.resultJson ?? null), leaseOwner: r.leaseOwner ?? null }));
}

export function requeueStaleRunningPollJobs(db: DB, olderThanSeconds: number): PollJobRow[] {
  const threshold = new Date(Date.now() - olderThanSeconds * 1000).toISOString();
  const now = new Date().toISOString();

  const rows = db
    .prepare(
      `SELECT id FROM poll_jobs WHERE status = 'running' AND updated_at < ? ORDER BY id`
    )
    .all(threshold) as Array<{ id: number }>;

  const tx = db.transaction((ids: Array<{ id: number }>) => {
    ids.forEach((r) => {
      db.prepare(
        `UPDATE poll_jobs
         SET status = 'pending', lease_owner = NULL, started_at = NULL, finished_at = NULL, result_json = NULL, updated_at = ?
         WHERE id = ? AND status = 'running'`
      ).run(now, r.id);
    });
    return ids.map((r) => getPollJobById(db, r.id)).filter(Boolean) as PollJobRow[];
  });

  return rows.length ? tx(rows) : [];
}

export function claimNextPendingPollJobForWorker(db: DB, input: { workerName: string; workerType: string }): PollJobRow | null {
  if (!licenseAllowsCollection(db).allowed) return null;
  const now = new Date().toISOString();
  const row = db
    .prepare(
      `SELECT pj.id
       FROM poll_jobs pj
       JOIN poll_targets pt ON pj.target_id = pt.id
       JOIN poll_profiles pp ON pt.profile_id = pp.id
       WHERE pj.status = 'pending' AND pp.kind = ?
       ORDER BY pj.id
       LIMIT 1`
    )
    .get(input.workerType) as { id: number } | undefined;

  if (!row) return null;

  db.prepare(
    `UPDATE poll_jobs
     SET status = 'running', lease_owner = ?, started_at = ?, attempt_count = attempt_count + 1, updated_at = ?
     WHERE id = ? AND status = 'pending'`
  ).run(input.workerName, now, now, row.id);

  return getPollJobById(db, row.id);
}

export function listRunningPollJobsForWorker(db: DB, workerName: string): PollJobRow[] {
  const rows = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs
       WHERE status = 'running' AND lease_owner = ?
       ORDER BY id`
    )
    .all(workerName) as any[];
  return rows.map((r) => ({ ...r, result: safeParseJson(r.resultJson ?? null), leaseOwner: r.leaseOwner ?? null }));
}

export function requeueStaleRunningPollJobsForWorker(db: DB, input: { workerName: string; olderThanSeconds: number }): PollJobRow[] {
  const threshold = new Date(Date.now() - input.olderThanSeconds * 1000).toISOString();
  const now = new Date().toISOString();

  const rows = db
    .prepare(
      `SELECT id FROM poll_jobs WHERE status = 'running' AND lease_owner = ? AND updated_at < ? ORDER BY id`
    )
    .all(input.workerName, threshold) as Array<{ id: number }>;

  const tx = db.transaction((ids: Array<{ id: number }>) => {
    ids.forEach((r) => {
      db.prepare(
        `UPDATE poll_jobs
         SET status = 'pending', lease_owner = NULL, started_at = NULL, finished_at = NULL, result_json = NULL, updated_at = ?
         WHERE id = ? AND status = 'running'`
      ).run(now, r.id);
    });
    return ids.map((r) => getPollJobById(db, r.id)).filter(Boolean) as PollJobRow[];
  });

  return rows.length ? tx(rows) : [];
}

export function claimNextPendingPollJobForWorkerCapabilities(db: DB, input: { workerName: string; supportedKinds: string[] }): PollJobRow | null {
  if (!licenseAllowsCollection(db).allowed) return null;
  if (!input.supportedKinds.length) return null;
  const now = new Date().toISOString();
  const placeholders = input.supportedKinds.map(() => "?").join(",");
  const row = db
    .prepare(
      `SELECT pj.id
       FROM poll_jobs pj
       JOIN poll_targets pt ON pj.target_id = pt.id
       JOIN poll_profiles pp ON pt.profile_id = pp.id
       WHERE pj.status = 'pending' AND pp.kind IN (${placeholders})
       ORDER BY pj.id
       LIMIT 1`
    )
    .get(...input.supportedKinds) as { id: number } | undefined;

  if (!row) return null;

  db.prepare(
    `UPDATE poll_jobs
     SET status = 'running', lease_owner = ?, started_at = ?, attempt_count = attempt_count + 1, updated_at = ?
     WHERE id = ? AND status = 'pending'`
  ).run(input.workerName, now, now, row.id);

  return getPollJobById(db, row.id);
}

export function abandonStaleRunningPollJobsForWorker(db: DB, input: { workerName: string; olderThanSeconds: number; result?: any | null }): PollJobRow[] {
  const threshold = new Date(Date.now() - input.olderThanSeconds * 1000).toISOString();
  const now = new Date().toISOString();
  const resultJson = JSON.stringify(input.result ?? null);

  const rows = db
    .prepare(
      `SELECT id FROM poll_jobs WHERE status = 'running' AND lease_owner = ? AND updated_at < ? ORDER BY id`
    )
    .all(input.workerName, threshold) as Array<{ id: number }>;

  const tx = db.transaction((ids: Array<{ id: number }>) => {
    ids.forEach((r) => {
      db.prepare(
        `UPDATE poll_jobs
         SET status = 'failed', finished_at = ?, result_json = ?, updated_at = ?
         WHERE id = ? AND status = 'running'`
      ).run(now, resultJson, now, r.id);
    });
    return ids.map((r) => getPollJobById(db, r.id)).filter(Boolean) as PollJobRow[];
  });

  return rows.length ? tx(rows) : [];
}

export function retryFailedPollJobsForWorker(db: DB, input: { workerName: string; olderThanSeconds?: number }): PollJobRow[] {
  const nowIso = new Date();
  const threshold = input.olderThanSeconds !== undefined ? new Date(Date.now() - input.olderThanSeconds * 1000).toISOString() : null;

  const rows = db
    .prepare(
      `SELECT id, target_id as targetId FROM poll_jobs
       WHERE status = 'failed' AND lease_owner = ?
       ${threshold ? "AND updated_at < ?" : ""}
       ORDER BY id`
    )
    .all(threshold ? [input.workerName, threshold] : [input.workerName]) as Array<{ id: number; targetId: number }>;

  const tx = db.transaction((ids: Array<{ id: number; targetId: number }>) => {
    return ids.map((r) => createPollJob(db, { targetId: r.targetId }));
  });

  return rows.length ? tx(rows) : [];
}

export function listFailedPollJobsForWorker(db: DB, input: { workerName: string; olderThanSeconds?: number }): PollJobRow[] {
  const threshold =
    input.olderThanSeconds !== undefined ? new Date(Date.now() - input.olderThanSeconds * 1000).toISOString() : null;
  const rows = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs
       WHERE status = 'failed' AND lease_owner = ? ${threshold ? "AND updated_at < ?" : ""}
       ORDER BY id`
    )
    .all(threshold ? [input.workerName, threshold] : [input.workerName]) as any[];
  return rows.map((r) => ({ ...r, result: safeParseJson(r.resultJson ?? null), leaseOwner: r.leaseOwner ?? null }));
}

export function listCompletedPollJobsForWorker(db: DB, input: { workerName: string; olderThanSeconds?: number }): PollJobRow[] {
  const threshold =
    input.olderThanSeconds !== undefined ? new Date(Date.now() - input.olderThanSeconds * 1000).toISOString() : null;
  const rows = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs
       WHERE status = 'completed' AND lease_owner = ? ${threshold ? "AND updated_at < ?" : ""}
       ORDER BY id`
    )
    .all(threshold ? [input.workerName, threshold] : [input.workerName]) as any[];
  return rows.map((r) => ({ ...r, result: safeParseJson(r.resultJson ?? null), leaseOwner: r.leaseOwner ?? null }));
}

export function getPollJobSummaryForWorker(db: DB, workerName: string): {
  workerName: string;
  total: number;
  pending: number;
  running: number;
  completed: number;
  failed: number;
} {
  const counts = db
    .prepare(
      `SELECT status, COUNT(*) as cnt
       FROM poll_jobs
       WHERE lease_owner = ?
       GROUP BY status`
    )
    .all(workerName) as Array<{ status: string; cnt: number }>;

  let running = 0;
  let completed = 0;
  let failed = 0;
  counts.forEach((row) => {
    if (row.status === "running") running = row.cnt;
    else if (row.status === "completed") completed = row.cnt;
    else if (row.status === "failed") failed = row.cnt;
  });

  const total = running + completed + failed;

  return {
    workerName,
    total,
    pending: 0,
    running,
    completed,
    failed,
  };
}

export function getPollJobSummary(db: DB, input?: { workerName?: string; workerType?: "ping" | "snmp" }): {
  total: number;
  pending: number;
  running: number;
  completed: number;
  failed: number;
  filters: { workerName?: string; workerType?: "ping" | "snmp" };
} {
  const workerName = input?.workerName?.trim();
  const workerType = input?.workerType;

  const joins = workerType
    ? ` JOIN poll_targets pt ON pj.target_id = pt.id JOIN poll_profiles pp ON pt.profile_id = pp.id `
    : "";

  const whereParts: string[] = ["status IN ('running','completed','failed')"];
  const params: any[] = [];

  if (workerName) {
    whereParts.push("pj.lease_owner = ?");
    params.push(workerName);
  }
  if (workerType) {
    whereParts.push("pp.kind = ?");
    params.push(workerType);
  }
  const where = `WHERE ${whereParts.join(" AND ")}`;

  // running/completed/failed counts
  const rows = db
    .prepare(
      `SELECT status, COUNT(*) as cnt
       FROM poll_jobs pj
       ${joins}
       ${where}
       GROUP BY status`
    )
    .all(...params) as Array<{ status: string; cnt: number }>;

  let running = 0;
  let completed = 0;
  let failed = 0;
  rows.forEach((row) => {
    if (row.status === "running") running = row.cnt;
    else if (row.status === "completed") completed = row.cnt;
    else if (row.status === "failed") failed = row.cnt;
  });

  // pending count (pending jobs have no lease_owner; if workerName filter supplied, pending is 0)
  let pending = 0;
  if (!workerName) {
    const pendingParams: any[] = [];
    const pendingWhereParts: string[] = ["status = 'pending'"];
    if (workerType) {
      pendingWhereParts.push("pp.kind = ?");
      pendingParams.push(workerType);
    }
    const pendingWhere = pendingWhereParts.length ? `WHERE ${pendingWhereParts.join(" AND ")}` : "";
    const pendingRow = db
      .prepare(
        `SELECT COUNT(*) as cnt
         FROM poll_jobs pj
         ${joins}
         ${pendingWhere}`
      )
      .get(...pendingParams) as { cnt: number };
    pending = pendingRow?.cnt ?? 0;
  }

  const total = pending + running + completed + failed;

  return {
    total,
    pending,
    running,
    completed,
    failed,
    filters: {
      ...(workerName ? { workerName } : {}),
      ...(workerType ? { workerType } : {}),
    },
  };
}

export function getStalePollJobSummary(db: DB, input: { olderThanSeconds: number; workerName?: string; workerType?: "ping" | "snmp" }): {
  olderThanSeconds: number;
  total: number;
  staleRunning: number;
  staleFailed: number;
  staleCompleted: number;
  filters: { workerName?: string; workerType?: "ping" | "snmp" };
} {
  const threshold = new Date(Date.now() - input.olderThanSeconds * 1000).toISOString();
  const workerName = input.workerName?.trim();
  const workerType = input.workerType;

  const joins = workerType
    ? ` JOIN poll_targets pt ON pj.target_id = pt.id JOIN poll_profiles pp ON pt.profile_id = pp.id `
    : "";

  const whereParts: string[] = ["pj.status IN ('running','completed','failed')", "pj.updated_at < ?"];
  const params: any[] = [threshold];

  if (workerName) {
    whereParts.push("pj.lease_owner = ?");
    params.push(workerName);
  }
  if (workerType) {
    whereParts.push("pp.kind = ?");
    params.push(workerType);
  }
  const where = `WHERE ${whereParts.join(" AND ")}`;

  const rows = db
    .prepare(
      `SELECT status, COUNT(*) as cnt
       FROM poll_jobs pj
       ${joins}
       ${where}
       GROUP BY status`
    )
    .all(...params) as Array<{ status: string; cnt: number }>;

  let staleRunning = 0;
  let staleCompleted = 0;
  let staleFailed = 0;
  rows.forEach((row) => {
    if (row.status === "running") staleRunning = row.cnt;
    else if (row.status === "completed") staleCompleted = row.cnt;
    else if (row.status === "failed") staleFailed = row.cnt;
  });

  const total = staleRunning + staleCompleted + staleFailed;

  return {
    olderThanSeconds: input.olderThanSeconds,
    total,
    staleRunning,
    staleFailed,
    staleCompleted,
    filters: {
      ...(workerName ? { workerName } : {}),
      ...(workerType ? { workerType } : {}),
    },
  };
}

export function getPendingPollJobAvailabilityForWorkerCapabilities(db: DB, input: { supportedKinds: string[] }): {
  totalPendingCompatible: number;
  byKind: Array<{ kind: string; count: number }>;
} {
  if (!input.supportedKinds.length) {
    return { totalPendingCompatible: 0, byKind: [] };
  }
  const placeholders = input.supportedKinds.map(() => "?").join(",");
  const rows = db
    .prepare(
      `SELECT pp.kind as kind, COUNT(*) as count
       FROM poll_jobs pj
       JOIN poll_targets pt ON pj.target_id = pt.id
       JOIN poll_profiles pp ON pt.profile_id = pp.id
       WHERE pj.status = 'pending' AND pp.kind IN (${placeholders})
       GROUP BY pp.kind
       ORDER BY pp.kind ASC`
    )
    .all(...input.supportedKinds) as Array<{ kind: string; count: number }>;

  const totalPendingCompatible = rows.reduce((acc, r) => acc + (r.count ?? 0), 0);
  return { totalPendingCompatible, byKind: rows };
}

export function listStaleRunningPollJobsForWorker(db: DB, input: { workerName: string; olderThanSeconds: number }): PollJobRow[] {
  const threshold = new Date(Date.now() - input.olderThanSeconds * 1000).toISOString();
  const rows = db
    .prepare(
      `SELECT id, target_id as targetId, status, scheduled_at as scheduledAt,
              started_at as startedAt, finished_at as finishedAt, lease_owner as leaseOwner,
              attempt_count as attemptCount, result_json as resultJson, created_at as createdAt, updated_at as updatedAt
       FROM poll_jobs
       WHERE status = 'running' AND lease_owner = ? AND updated_at < ?
       ORDER BY id`
    )
    .all(input.workerName, threshold) as any[];
  return rows.map((r) => ({ ...r, result: safeParseJson(r.resultJson ?? null), leaseOwner: r.leaseOwner ?? null }));
}

export function createPollJob(db: DB, input: { targetId: number; scheduledAt?: string; status?: string }): PollJobRow {
  assertLicense(db);
  const now = new Date().toISOString();
  const scheduledAt = input.scheduledAt ?? now;
  const status = input.status ?? "pending";
  const result = db
    .prepare(
      `INSERT INTO poll_jobs (target_id, status, scheduled_at, started_at, finished_at, lease_owner, result_json, attempt_count, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(input.targetId, status, scheduledAt, null, null, null, null, 0, now, now);
  return {
    id: Number(result.lastInsertRowid),
    targetId: input.targetId,
    status,
    scheduledAt,
    startedAt: null,
    finishedAt: null,
    leaseOwner: null,
    attemptCount: 0,
    result: null,
    createdAt: now,
    updatedAt: now,
  };
}

export type AgentEnrollmentRow = {
  id: number;
  enrollmentId: string;
  agentType: string;
  token: string;
  status: string;
  companySlug: string | null;
  deploymentId: string | null;
  createdAt: string;
  expiresAt: string | null;
  lastUsedAt: string | null;
  changedBy: string | null;
  updatedAt: string;
};

export function listAgentEnrollments(db: DB, status?: "active" | "revoked"): AgentEnrollmentRow[] {
  const base =
    `SELECT id, enrollment_id as enrollmentId, agent_type as agentType, token, status,
            company_slug as companySlug, deployment_id as deploymentId,
            created_at as createdAt, expires_at as expiresAt, last_used_at as lastUsedAt, changed_by as changedBy, updated_at as updatedAt
     FROM agent_enrollments`;
  if (status === "active" || status === "revoked") {
    return db.prepare(`${base} WHERE status = ? ORDER BY id`).all(status) as AgentEnrollmentRow[];
  }
  return db.prepare(`${base} ORDER BY id`).all() as AgentEnrollmentRow[];
}

export function createAgentEnrollment(db: DB, input: {
  enrollmentId: string;
  agentType: string;
  token: string;
  status?: string;
  companySlug?: string | null;
  deploymentId?: string | null;
  expiresAt?: string | null;
  changedBy?: string | null;
}): AgentEnrollmentRow {
  const now = new Date().toISOString();
  const status = input.status ?? "active";
  const result = db
    .prepare(
      `INSERT INTO agent_enrollments (enrollment_id, agent_type, token, status, company_slug, deployment_id, created_at, expires_at, last_used_at, changed_by, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
    )
    .run(
      input.enrollmentId,
      input.agentType,
      input.token,
      status,
      input.companySlug ?? null,
      input.deploymentId ?? null,
      now,
      input.expiresAt ?? null,
      null,
      input.changedBy ?? null,
      now
    );
  return {
    id: Number(result.lastInsertRowid),
    enrollmentId: input.enrollmentId,
    agentType: input.agentType,
    token: input.token,
    status,
    companySlug: input.companySlug ?? null,
    deploymentId: input.deploymentId ?? null,
    createdAt: now,
    expiresAt: input.expiresAt ?? null,
    lastUsedAt: null,
    changedBy: input.changedBy ?? null,
    updatedAt: now,
  };
}

export function revokeAgentEnrollment(db: DB, enrollmentId: string, changedBy?: string | null): AgentEnrollmentRow | null {
  const existing = db
    .prepare(
      `SELECT id, enrollment_id as enrollmentId, agent_type as agentType, token, status,
              company_slug as companySlug, deployment_id as deploymentId,
              created_at as createdAt, expires_at as expiresAt, last_used_at as lastUsedAt, changed_by as changedBy, updated_at as updatedAt
       FROM agent_enrollments WHERE enrollment_id = ? LIMIT 1`
    )
    .get(enrollmentId) as AgentEnrollmentRow | undefined;
  if (!existing) return null;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE agent_enrollments SET status = ?, changed_by = ?, updated_at = ? WHERE enrollment_id = ?`
  ).run("revoked", changedBy ?? existing.changedBy ?? null, now, enrollmentId);
  return { ...existing, status: "revoked", changedBy: changedBy ?? existing.changedBy ?? null, updatedAt: now };
}

export function getAgentEnrollmentByEnrollmentId(db: DB, enrollmentId: string): AgentEnrollmentRow | null {
  const row = db
    .prepare(
      `SELECT id, enrollment_id as enrollmentId, agent_type as agentType, token, status,
              company_slug as companySlug, deployment_id as deploymentId,
              created_at as createdAt, expires_at as expiresAt, last_used_at as lastUsedAt, changed_by as changedBy, updated_at as updatedAt
       FROM agent_enrollments WHERE enrollment_id = ? LIMIT 1`
    )
    .get(enrollmentId) as AgentEnrollmentRow | undefined;
  return row ?? null;
}

export function getAgentEnrollmentByToken(db: DB, token: string): AgentEnrollmentRow | null {
  const row = db
    .prepare(
      `SELECT id, enrollment_id as enrollmentId, agent_type as agentType, token, status,
              company_slug as companySlug, deployment_id as deploymentId,
              created_at as createdAt, expires_at as expiresAt, last_used_at as lastUsedAt, changed_by as changedBy, updated_at as updatedAt
       FROM agent_enrollments WHERE token = ? LIMIT 1`
    )
    .get(token) as AgentEnrollmentRow | undefined;
  return row ?? null;
}

export function touchAgentEnrollmentLastUsedAt(db: DB, enrollmentId: string): void {
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE agent_enrollments SET last_used_at = ?, updated_at = ? WHERE enrollment_id = ?`
  ).run(now, now, enrollmentId);
}

export function updateAgentEnrollmentStatus(db: DB, enrollmentId: string, status: "active" | "revoked", changedBy?: string | null): AgentEnrollmentRow | null {
  const existing = getAgentEnrollmentByEnrollmentId(db, enrollmentId);
  if (!existing) return null;
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE agent_enrollments SET status = ?, changed_by = ?, updated_at = ? WHERE enrollment_id = ?`
  ).run(status, changedBy ?? existing.changedBy ?? null, now, enrollmentId);
  return { ...existing, status, changedBy: changedBy ?? existing.changedBy ?? null, updatedAt: now };
}

export function normalizeCompanySlug(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

export function isCompanySlugInUse(db: DB, slug: string, excludeId?: number): boolean {
  if (!slug) return false;
  const row = db
    .prepare(
      `SELECT id FROM companies WHERE company_slug = ? ${excludeId ? "AND id != ?" : ""} LIMIT 1`
    )
    .get(excludeId ? [slug, excludeId] : [slug]) as { id: number } | undefined;
  return Boolean(row);
}

export function generateDeploymentId(): string {
  return `dep-${randomUUID().replace(/-/g, "")}`;
}

export function generateEnrollmentToken(): string {
  return `enr-${randomUUID().replace(/-/g, "")}`;
}

export function generateEnrollmentId(): string {
  return `enr-id-${randomUUID().replace(/-/g, "")}`;
}

export type CompanyRow = {
  id: number;
  companyName: string;
  companySlug: string;
  orgId: string | null;
  createdAt: string;
  updatedAt: string;
};

export function getCompany(db: DB): CompanyRow | null {
  const row = db
    .prepare(
      `SELECT id, company_name as companyName, company_slug as companySlug, org_id as orgId,
              created_at as createdAt, updated_at as updatedAt
       FROM companies ORDER BY id LIMIT 1`
    )
    .get() as CompanyRow | undefined;
  return row ?? null;
}

export function upsertCompany(db: DB, input: { companyName: string; companySlug: string; orgId?: string | null }): CompanyRow {
  const existing = getCompany(db);
  const now = new Date().toISOString();
  const duplicate = isCompanySlugInUse(db, input.companySlug, existing?.id);
  if (duplicate) {
    const err: any = new Error("company_slug_in_use");
    err.code = "company_slug_in_use";
    throw err;
  }
  if (existing) {
    db.prepare(
      `UPDATE companies SET company_name = ?, company_slug = ?, org_id = ?, updated_at = ? WHERE id = ?`
    ).run(input.companyName, input.companySlug, input.orgId ?? null, now, existing.id);
    return { ...existing, companyName: input.companyName, companySlug: input.companySlug, orgId: input.orgId ?? null, updatedAt: now };
  }
  const result = db
    .prepare(
      `INSERT INTO companies (company_name, company_slug, org_id, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?)`
    )
    .run(input.companyName, input.companySlug, input.orgId ?? null, now, now);
  return {
    id: Number(result.lastInsertRowid),
    companyName: input.companyName,
    companySlug: input.companySlug,
    orgId: input.orgId ?? null,
    createdAt: now,
    updatedAt: now,
  };
}

export type DeploymentRow = {
  id: number;
  deploymentId: string;
  deploymentName: string;
  mode: string;
  registeredToCloud: boolean;
  buildChannel: string | null;
  createdAt: string;
  updatedAt: string;
};

export function getDeployment(db: DB): DeploymentRow | null {
  const row = db
    .prepare(
      `SELECT id, deployment_id as deploymentId, deployment_name as deploymentName, mode,
              registered_to_cloud as registeredToCloud, build_channel as buildChannel,
              created_at as createdAt, updated_at as updatedAt
       FROM deployments ORDER BY id LIMIT 1`
    )
    .get() as
    | {
        id: number;
        deploymentId: string;
        deploymentName: string;
        mode: string;
        registeredToCloud: number;
        buildChannel: string | null;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) return null;
  return { ...row, registeredToCloud: Boolean(row.registeredToCloud) };
}

export function upsertDeployment(db: DB, input: {
  deploymentId: string;
  deploymentName: string;
  mode: string;
  registeredToCloud?: boolean;
  buildChannel?: string | null;
}): DeploymentRow {
  const existing = getDeployment(db);
  const now = new Date().toISOString();
  const registered = input.registeredToCloud ?? false;
  if (existing) {
    db.prepare(
      `UPDATE deployments
       SET deployment_id = ?, deployment_name = ?, mode = ?, registered_to_cloud = ?, build_channel = ?, updated_at = ?
       WHERE id = ?`
    ).run(input.deploymentId, input.deploymentName, input.mode, registered ? 1 : 0, input.buildChannel ?? null, now, existing.id);
    return {
      ...existing,
      deploymentId: input.deploymentId,
      deploymentName: input.deploymentName,
      mode: input.mode,
      registeredToCloud: registered,
      buildChannel: input.buildChannel ?? null,
      updatedAt: now,
    };
  }
  const result = db
    .prepare(
      `INSERT INTO deployments (deployment_id, deployment_name, mode, registered_to_cloud, build_channel, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .run(input.deploymentId, input.deploymentName, input.mode, registered ? 1 : 0, input.buildChannel ?? null, now, now);
  return {
    id: Number(result.lastInsertRowid),
    deploymentId: input.deploymentId,
    deploymentName: input.deploymentName,
    mode: input.mode,
    registeredToCloud: registered,
    buildChannel: input.buildChannel ?? null,
    createdAt: now,
    updatedAt: now,
  };
}

export type CloudSyncStateRow = {
  enabled: boolean;
  status: string;
  lastSyncAt: string | null;
  cloudEndpoint: string | null;
  tenantKey: string | null;
  createdAt: string;
  updatedAt: string;
};

export function getCloudSyncState(db: DB): CloudSyncStateRow {
  const row = db
    .prepare(
      `SELECT enabled, status, last_sync_at as lastSyncAt, cloud_endpoint as cloudEndpoint, tenant_key as tenantKey,
              created_at as createdAt, updated_at as updatedAt
       FROM cloud_sync_state WHERE id = 1`
    )
    .get() as
    | {
        enabled: number;
        status: string;
        lastSyncAt: string | null;
        cloudEndpoint: string | null;
        tenantKey: string | null;
        createdAt: string;
        updatedAt: string;
      }
    | undefined;
  if (!row) {
    return {
      enabled: false,
      status: "not-configured",
      lastSyncAt: null,
      cloudEndpoint: null,
      tenantKey: null,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };
  }
  return { ...row, enabled: Boolean(row.enabled) };
}

export function updateCloudSyncState(db: DB, input: Partial<{
  enabled: boolean;
  status: string;
  lastSyncAt: string | null;
  cloudEndpoint: string | null;
  tenantKey: string | null;
}>): CloudSyncStateRow {
  const current = getCloudSyncState(db);
  const now = new Date().toISOString();
  db.prepare(
    `UPDATE cloud_sync_state
     SET enabled = ?, status = ?, last_sync_at = ?, cloud_endpoint = ?, tenant_key = ?, updated_at = ?
     WHERE id = 1`
  ).run(
    input.enabled !== undefined ? (input.enabled ? 1 : 0) : current.enabled ? 1 : 0,
    input.status ?? current.status,
    input.lastSyncAt !== undefined ? input.lastSyncAt : current.lastSyncAt,
    input.cloudEndpoint !== undefined ? input.cloudEndpoint : current.cloudEndpoint,
    input.tenantKey !== undefined ? input.tenantKey : current.tenantKey,
    now
  );
  return {
    enabled: input.enabled ?? current.enabled,
    status: input.status ?? current.status,
    lastSyncAt: input.lastSyncAt !== undefined ? input.lastSyncAt : current.lastSyncAt,
    cloudEndpoint: input.cloudEndpoint !== undefined ? input.cloudEndpoint : current.cloudEndpoint,
    tenantKey: input.tenantKey !== undefined ? input.tenantKey : current.tenantKey,
    createdAt: current.createdAt,
    updatedAt: now,
  };
}
