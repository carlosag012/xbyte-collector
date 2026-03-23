import type { IncomingMessage } from "node:http";

export type ReadJsonOptions = {
  maxBytes?: number;
};

// Minimal JSON body reader for small POST requests (dependency-free).
export async function readJsonBody<T = any>(req: IncomingMessage, options: ReadJsonOptions = {}): Promise<T> {
  const maxBytes = options.maxBytes ?? 16 * 1024; // 16 KB default
  let received = 0;
  const chunks: Buffer[] = [];

  for await (const chunk of req) {
    const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    received += buf.length;
    if (received > maxBytes) {
      throw new Error("request_body_too_large");
    }
    chunks.push(buf);
  }

  const raw = Buffer.concat(chunks).toString("utf8");
  try {
    return raw ? (JSON.parse(raw) as T) : ({} as T);
  } catch {
    throw new Error("invalid_json");
  }
}
