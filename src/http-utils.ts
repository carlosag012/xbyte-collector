// Tiny HTTP helpers for path parameter parsing.

export function parseNonNegativeInteger(value: string): number | null {
  if (!/^-?\d+$/.test(value)) return null;
  const num = Number(value);
  if (!Number.isFinite(num)) return null;
  if (!Number.isInteger(num)) return null;
  if (num < 0) return null;
  return num;
}
