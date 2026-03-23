import Database from "better-sqlite3";
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
  `);

  const row = db
    .prepare(`SELECT id FROM bootstrap_state WHERE id = 1`)
    .get() as { id: number } | undefined;

  if (!row) {
    db.prepare(
      `INSERT INTO bootstrap_state (id, configured, status, updated_at) VALUES (1, ?, ?, ?)`
    ).run(0, "not-initialized", new Date().toISOString());
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
