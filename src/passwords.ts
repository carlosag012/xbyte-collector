import { randomBytes, scrypt as _scrypt, timingSafeEqual } from "node:crypto";

// Returns a string encoding algorithm parameters, salt, and hash for storage.
// Format: scrypt$N$r$p$keylen$base64salt$base64hash
export async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16);
  const N = 16384;
  const r = 8;
  const p = 1;
  const keylen = 64;
  const derivedKey = await scryptBuffer(password, salt, keylen, { N, r, p });

  return [
    "scrypt",
    N,
    r,
    p,
    keylen,
    salt.toString("base64"),
    derivedKey.toString("base64"),
  ].join("$");
}

export async function verifyPassword(password: string, stored: string): Promise<boolean> {
  try {
    const parts = stored.split("$");
    if (parts.length !== 7) return false;

    const [alg, nStr, rStr, pStr, keylenStr, saltB64, hashB64] = parts;
    if (alg !== "scrypt") return false;

    const N = Number(nStr);
    const r = Number(rStr);
    const p = Number(pStr);
    const keylen = Number(keylenStr);

    if (
      !Number.isFinite(N) ||
      !Number.isFinite(r) ||
      !Number.isFinite(p) ||
      !Number.isFinite(keylen)
    ) {
      return false;
    }

    const salt = Buffer.from(saltB64, "base64");
    const storedHash = Buffer.from(hashB64, "base64");
    const derived = await scryptBuffer(password, salt, keylen, { N, r, p });

    if (derived.length !== storedHash.length) return false;
    return timingSafeEqual(derived, storedHash);
  } catch {
    return false;
  }
}

function scryptBuffer(
  password: string,
  salt: Buffer,
  keylen: number,
  options: { N: number; r: number; p: number }
): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    _scrypt(password, salt, keylen, options, (err, derivedKey) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(derivedKey as Buffer);
    });
  });
}
