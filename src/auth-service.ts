import type { DB, UserRow } from "./db.js";
import { getUserByUsername } from "./db.js";
import { verifyPassword } from "./passwords.js";

type AuthResult =
  | { ok: true; user: Pick<UserRow, "id" | "username" | "role" | "isActive"> }
  | { ok: false; reason: "invalid_credentials" | "inactive_user" };

export async function authenticateLocalUser(db: DB, username: string, password: string): Promise<AuthResult> {
  const user = getUserByUsername(db, username);
  if (!user) return { ok: false, reason: "invalid_credentials" };

  const valid = await verifyPassword(password, user.passwordHash);
  if (!valid) return { ok: false, reason: "invalid_credentials" };

  if (!user.isActive) return { ok: false, reason: "inactive_user" };

  return {
    ok: true,
    user: {
      id: user.id,
      username: user.username,
      role: user.role,
      isActive: user.isActive,
    },
  };
}
