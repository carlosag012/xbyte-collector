import pkg from "../package.json" with { type: "json" };

export const versionInfo = {
  service: "xbyte-collector" as const,
  phase: "phase-1-foundation" as const,
  version: pkg.version ?? "0.0.0",
};
