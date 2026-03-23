import { randomBytes } from "node:crypto";
import type { DB, SessionRow } from "./db.js";
import { createSession, getSessionById, deleteSession } from "./db.js";

function generateSessionId(): string {
  return randomBytes(32).toString("hex");
}

export function issueSession(db: DB, userId: number, ttlSeconds: number): SessionRow {
  const now = new Date();
  const expires = new Date(now.getTime() + ttlSeconds * 1000);
  const session: SessionRow = {
    id: generateSessionId(),
    userId,
    createdAt: now.toISOString(),
    expiresAt: expires.toISOString(),
  };
  createSession(db, session);
  return session;
}

export function readValidSession(db: DB, sessionId: string): SessionRow | null {
  const session = getSessionById(db, sessionId);
  if (!session) return null;
  if (new Date(session.expiresAt).getTime() <= Date.now()) {
    deleteSession(db, sessionId);
    return null;
  }
  return session;
}

export function clearSession(db: DB, sessionId: string) {
  deleteSession(db, sessionId);
}
