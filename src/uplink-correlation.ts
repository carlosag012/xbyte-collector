import {
  findDeviceByIpOrHostname,
  getDeviceById,
  getDevicePollHealth,
  listInterfaceSnapshotsForDevice,
  listNeighborsWithReview,
  listUplinkFiberConfigForDevice,
  type DB,
  type DevicePollHealth,
  type DeviceRow,
} from "./db.js";

type StatusNormalized = "up" | "down" | "unknown";

export type UplinkCorrelationCategory =
  | "healthy"
  | "admin_shutdown"
  | "likely_local_uplink_failure"
  | "likely_fiber_path_issue"
  | "likely_remote_side_issue"
  | "degraded_unknown";

export type UplinkCorrelationConfidence = "low" | "medium" | "high";

type InterfaceSnapshotRow = {
  ifIndex: number | null;
  ifName: string | null;
  ifDescr: string | null;
  ifAlias: string | null;
  adminStatus: string | null;
  operStatus: string | null;
  speed: number | null;
  bpsIn: number | null;
  bpsOut: number | null;
  utilAvg: number | null;
  rateCollectedAt: string | null;
  collectedAt: string;
};

type NeighborWithReviewRow = {
  localPort: string | null;
  remoteSysName: string | null;
  remotePortId: string | null;
  remoteChassisId: string | null;
  remoteMgmtIp: string | null;
  collectedAt: string;
  linkedDeviceId: number | null;
  reviewStatus: string | null;
};

type FiberConfigRow = {
  stableInterfaceKey: string;
  localIfIndex: number | null;
  localPortNormalized: string | null;
  localPortDisplay: string | null;
  cableCount: string | null;
  bufferColor: string | null;
  txStrandColor: string | null;
  rxStrandColor: string | null;
  jumperMode: string | null;
  connectorType: string | null;
  patchPanelPorts: string | null;
  sfpDetected: string | null;
  sfpPartNumber: string | null;
  rxLight: string | null;
  txLight: string | null;
  updatedAt: string;
};

export type CorrelationNeighborSummary = {
  remoteSysName: string | null;
  remotePortId: string | null;
  remoteChassisId: string | null;
  remoteMgmtIp: string | null;
  collectedAt: string;
  linkedDeviceId: number | null;
  linkedDeviceHostname: string | null;
  linkedDeviceHealthStatus: string | null;
};

export type CorrelationFiberSummary = {
  stableInterfaceKey: string;
  localPortDisplay: string | null;
  cableCount: string | null;
  bufferColor: string | null;
  txStrandColor: string | null;
  rxStrandColor: string | null;
  jumperMode: string | null;
  connectorType: string | null;
  patchPanelPorts: string | null;
  sfpDetected: string | null;
  sfpPartNumber: string | null;
  rxLight: string | null;
  txLight: string | null;
  updatedAt: string;
};

export type CorrelatedInterfaceRow = {
  stableInterfaceKey: string;
  localIfIndex: number | null;
  localPortLabel: string;
  adminStatus: StatusNormalized;
  operStatus: StatusNormalized;
  speed: number | null;
  bpsIn: number | null;
  bpsOut: number | null;
  utilAvg: number | null;
  rateCollectedAt: string | null;
  collectedAt: string;
  hasRelationshipEvidence: boolean;
  hasDocumentedFiber: boolean;
  matchedNeighbor: CorrelationNeighborSummary | null;
  matchedFiberConfig: CorrelationFiberSummary | null;
  likelyCauseCategory: UplinkCorrelationCategory;
  confidence: UplinkCorrelationConfidence;
  evidence: string[];
};

export type BlastRadiusCandidate = {
  localPort: string;
  remoteSysName: string | null;
  remoteMgmtIp: string | null;
  linkedDeviceId: number | null;
  reason: string;
};

export type UplinkCorrelationSnapshot = {
  deviceId: number;
  hostname: string | null;
  ipAddress: string | null;
  pollHealth: DevicePollHealth | null;
  generatedAt: string;
  likelyIssueCategory: UplinkCorrelationCategory;
  confidence: UplinkCorrelationConfidence;
  summary: string;
  affectedUplinkCount: number;
  likelyBlastRadiusCount: number;
  blastRadiusCandidates: BlastRadiusCandidate[];
  correlatedInterfaces: CorrelatedInterfaceRow[];
};

function normalizePortIdentity(value: unknown): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  return raw.replace(/\s+/g, "").replace(/["']/g, "");
}

function buildStableInterfaceKey(ifIndex: number | null, localPortLabel: string): string {
  if (Number.isInteger(ifIndex) && Number(ifIndex) > 0) {
    return `if:${Number(ifIndex)}`;
  }
  const normalizedPort = normalizePortIdentity(localPortLabel);
  return normalizedPort ? `port:${normalizedPort}` : `port:${localPortLabel.toLowerCase()}`;
}

function normalizeAdminStatus(value: unknown): StatusNormalized {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return "unknown";
  if (raw === "1" || raw === "up" || raw.includes("admin up")) return "up";
  if (raw === "2" || raw === "down" || raw.includes("admin down")) return "down";
  return "unknown";
}

function normalizeOperStatus(value: unknown): StatusNormalized {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return "unknown";
  if (raw === "1" || raw === "up" || raw.includes("oper up")) return "up";
  if (
    raw === "2" ||
    raw === "3" ||
    raw === "5" ||
    raw === "6" ||
    raw === "7" ||
    raw === "down" ||
    raw.includes("lowerlayerdown") ||
    raw.includes("lower layer down")
  ) {
    return "down";
  }
  return "unknown";
}

function hasAnyFiberValue(row: FiberConfigRow | null): boolean {
  if (!row) return false;
  return Boolean(
    row.cableCount ||
      row.bufferColor ||
      row.txStrandColor ||
      row.rxStrandColor ||
      row.jumperMode ||
      row.connectorType ||
      row.patchPanelPorts ||
      row.sfpDetected ||
      row.sfpPartNumber ||
      row.rxLight ||
      row.txLight,
  );
}

function classifyInterface(input: {
  adminStatus: StatusNormalized;
  operStatus: StatusNormalized;
  hasRelationshipEvidence: boolean;
  hasDocumentedFiber: boolean;
  remoteLikelyDown: boolean;
  lowTraffic: boolean;
}): { category: UplinkCorrelationCategory; confidence: UplinkCorrelationConfidence; evidence: string[] } {
  const evidence: string[] = [];
  if (input.hasRelationshipEvidence) evidence.push("relationship_evidence_present");
  if (input.hasDocumentedFiber) evidence.push("documented_uplink_fiber_present");
  if (input.lowTraffic) evidence.push("low_or_no_traffic_detected");
  if (input.remoteLikelyDown) evidence.push("linked_remote_device_recent_failure");

  if (input.adminStatus === "down") {
    evidence.unshift("admin_status_down");
    return { category: "admin_shutdown", confidence: "high", evidence };
  }

  if (input.adminStatus === "up" && input.operStatus === "up") {
    evidence.unshift("admin_up_oper_up");
    return { category: "healthy", confidence: "high", evidence };
  }

  if (input.adminStatus === "up" && input.operStatus === "down") {
    evidence.unshift("admin_up_oper_down");
    if (input.remoteLikelyDown && input.hasRelationshipEvidence) {
      return { category: "likely_remote_side_issue", confidence: "medium", evidence };
    }
    if (input.hasDocumentedFiber && input.hasRelationshipEvidence && input.lowTraffic) {
      return { category: "likely_fiber_path_issue", confidence: "high", evidence };
    }
    if (input.hasDocumentedFiber || input.hasRelationshipEvidence) {
      return { category: "likely_local_uplink_failure", confidence: "medium", evidence };
    }
  }

  evidence.unshift("insufficient_correlated_signal");
  return { category: "degraded_unknown", confidence: "low", evidence };
}

function categoryPriority(category: UplinkCorrelationCategory): number {
  switch (category) {
    case "likely_fiber_path_issue":
      return 90;
    case "likely_local_uplink_failure":
      return 85;
    case "likely_remote_side_issue":
      return 80;
    case "degraded_unknown":
      return 60;
    case "admin_shutdown":
      return 40;
    case "healthy":
    default:
      return 10;
  }
}

function confidenceScore(confidence: UplinkCorrelationConfidence): number {
  switch (confidence) {
    case "high":
      return 3;
    case "medium":
      return 2;
    case "low":
    default:
      return 1;
  }
}

function categorySummary(category: UplinkCorrelationCategory): string {
  switch (category) {
    case "healthy":
      return "healthy state";
    case "admin_shutdown":
      return "admin shutdown";
    case "likely_local_uplink_failure":
      return "likely local uplink failure";
    case "likely_fiber_path_issue":
      return "likely fiber-path issue";
    case "likely_remote_side_issue":
      return "likely remote-side issue";
    case "degraded_unknown":
    default:
      return "degraded unknown condition";
  }
}

function pickRelevantInterfaces(rows: CorrelatedInterfaceRow[]): CorrelatedInterfaceRow[] {
  const relevant = rows.filter(
    (row) =>
      row.hasRelationshipEvidence ||
      row.hasDocumentedFiber ||
      row.adminStatus !== "up" ||
      row.operStatus !== "up",
  );
  if (relevant.length) return relevant;
  return rows.slice(0, 10);
}

export function buildUplinkCorrelationSnapshot(db: DB, deviceId: number): UplinkCorrelationSnapshot | null {
  const device = getDeviceById(db, deviceId);
  if (!device) return null;

  const pollHealth = getDevicePollHealth(db, deviceId);
  const interfacesRaw = listInterfaceSnapshotsForDevice(db, deviceId) as InterfaceSnapshotRow[];
  const neighborsRaw = listNeighborsWithReview(db, { deviceId }) as NeighborWithReviewRow[];
  const fiberRaw = listUplinkFiberConfigForDevice(db, deviceId) as FiberConfigRow[];

  const neighborsByPort = new Map<string, NeighborWithReviewRow[]>();
  for (const neighbor of neighborsRaw) {
    const key = normalizePortIdentity(neighbor.localPort);
    if (!key) continue;
    const list = neighborsByPort.get(key) ?? [];
    list.push(neighbor);
    neighborsByPort.set(key, list);
  }

  const fiberByStableKey = new Map<string, FiberConfigRow>();
  const fiberByPort = new Map<string, FiberConfigRow>();
  for (const row of fiberRaw) {
    const stableKey = String(row.stableInterfaceKey ?? "").trim();
    if (stableKey) fiberByStableKey.set(stableKey, row);

    const portNormalized = normalizePortIdentity(row.localPortNormalized ?? row.localPortDisplay);
    if (portNormalized && !fiberByPort.has(portNormalized)) {
      fiberByPort.set(portNormalized, row);
    }

    if (stableKey.startsWith("port:")) {
      const stablePort = normalizePortIdentity(stableKey.slice(5));
      if (stablePort && !fiberByPort.has(stablePort)) {
        fiberByPort.set(stablePort, row);
      }
    }
  }

  const correlatedRows: CorrelatedInterfaceRow[] = interfacesRaw.map((iface) => {
    const localPortLabel = String(iface.ifName ?? iface.ifDescr ?? iface.ifAlias ?? (iface.ifIndex != null ? `ifIndex-${iface.ifIndex}` : "unknown")).trim();
    const stableInterfaceKey = buildStableInterfaceKey(iface.ifIndex, localPortLabel);
    const localPortKey = normalizePortIdentity(localPortLabel);

    const matchedNeighbors = localPortKey ? neighborsByPort.get(localPortKey) ?? [] : [];
    const matchedNeighbor = matchedNeighbors[0] ?? null;

    const matchedFiber =
      fiberByStableKey.get(stableInterfaceKey) ??
      (localPortKey ? fiberByPort.get(localPortKey) ?? null : null);

    let linkedDevice: DeviceRow | null = null;
    let linkedHealth: DevicePollHealth | null = null;
    let linkedDeviceId: number | null = null;

    if (matchedNeighbor) {
      if (Number.isInteger(matchedNeighbor.linkedDeviceId) && Number(matchedNeighbor.linkedDeviceId) > 0) {
        linkedDeviceId = Number(matchedNeighbor.linkedDeviceId);
        linkedDevice = getDeviceById(db, linkedDeviceId);
      }
      if (!linkedDevice) {
        linkedDevice = findDeviceByIpOrHostname(db, {
          ipAddress: matchedNeighbor.remoteMgmtIp ?? undefined,
          hostname: matchedNeighbor.remoteSysName ?? undefined,
        });
        if (linkedDevice) linkedDeviceId = linkedDevice.id;
      }
      if (linkedDeviceId) {
        linkedHealth = getDevicePollHealth(db, linkedDeviceId);
      }
    }

    const adminStatus = normalizeAdminStatus(iface.adminStatus);
    const operStatus = normalizeOperStatus(iface.operStatus);
    const lowTraffic =
      typeof iface.bpsIn === "number" &&
      typeof iface.bpsOut === "number" &&
      iface.bpsIn <= 1000 &&
      iface.bpsOut <= 1000;

    const remoteLikelyDown =
      linkedHealth?.currentStatus === "failed" ||
      (Boolean(linkedHealth?.lastFailureAt) &&
        (!linkedHealth?.lastSuccessAt || Date.parse(linkedHealth.lastFailureAt ?? "") > Date.parse(linkedHealth.lastSuccessAt ?? "")));

    const hasRelationshipEvidence = Boolean(matchedNeighbor);
    const hasDocumentedFiber = hasAnyFiberValue(matchedFiber ?? null);

    const classification = classifyInterface({
      adminStatus,
      operStatus,
      hasRelationshipEvidence,
      hasDocumentedFiber,
      remoteLikelyDown: Boolean(remoteLikelyDown),
      lowTraffic,
    });

    const matchedNeighborSummary: CorrelationNeighborSummary | null = matchedNeighbor
      ? {
          remoteSysName: matchedNeighbor.remoteSysName ?? null,
          remotePortId: matchedNeighbor.remotePortId ?? null,
          remoteChassisId: matchedNeighbor.remoteChassisId ?? null,
          remoteMgmtIp: matchedNeighbor.remoteMgmtIp ?? null,
          collectedAt: matchedNeighbor.collectedAt,
          linkedDeviceId,
          linkedDeviceHostname: linkedDevice?.hostname ?? null,
          linkedDeviceHealthStatus: linkedHealth?.currentStatus ?? null,
        }
      : null;

    const matchedFiberSummary: CorrelationFiberSummary | null = matchedFiber
      ? {
          stableInterfaceKey: matchedFiber.stableInterfaceKey,
          localPortDisplay: matchedFiber.localPortDisplay ?? null,
          cableCount: matchedFiber.cableCount ?? null,
          bufferColor: matchedFiber.bufferColor ?? null,
          txStrandColor: matchedFiber.txStrandColor ?? null,
          rxStrandColor: matchedFiber.rxStrandColor ?? null,
          jumperMode: matchedFiber.jumperMode ?? null,
          connectorType: matchedFiber.connectorType ?? null,
          patchPanelPorts: matchedFiber.patchPanelPorts ?? null,
          sfpDetected: matchedFiber.sfpDetected ?? null,
          sfpPartNumber: matchedFiber.sfpPartNumber ?? null,
          rxLight: matchedFiber.rxLight ?? null,
          txLight: matchedFiber.txLight ?? null,
          updatedAt: matchedFiber.updatedAt,
        }
      : null;

    return {
      stableInterfaceKey,
      localIfIndex: Number.isInteger(iface.ifIndex) ? Number(iface.ifIndex) : null,
      localPortLabel,
      adminStatus,
      operStatus,
      speed: typeof iface.speed === "number" ? iface.speed : null,
      bpsIn: typeof iface.bpsIn === "number" ? iface.bpsIn : null,
      bpsOut: typeof iface.bpsOut === "number" ? iface.bpsOut : null,
      utilAvg: typeof iface.utilAvg === "number" ? iface.utilAvg : null,
      rateCollectedAt: iface.rateCollectedAt ?? null,
      collectedAt: iface.collectedAt,
      hasRelationshipEvidence,
      hasDocumentedFiber,
      matchedNeighbor: matchedNeighborSummary,
      matchedFiberConfig: matchedFiberSummary,
      likelyCauseCategory: classification.category,
      confidence: classification.confidence,
      evidence: classification.evidence,
    };
  });

  const relevantRows = pickRelevantInterfaces(correlatedRows);

  const blastRadiusCandidates: BlastRadiusCandidate[] = [];
  const seenBlast = new Set<string>();
  for (const row of relevantRows) {
    if (!row.matchedNeighbor) continue;
    if (row.likelyCauseCategory === "healthy" || row.likelyCauseCategory === "admin_shutdown") continue;
    const key = `${row.localPortLabel}|${row.matchedNeighbor.remoteMgmtIp ?? ""}|${row.matchedNeighbor.remoteSysName ?? ""}`;
    if (seenBlast.has(key)) continue;
    seenBlast.add(key);
    blastRadiusCandidates.push({
      localPort: row.localPortLabel,
      remoteSysName: row.matchedNeighbor.remoteSysName,
      remoteMgmtIp: row.matchedNeighbor.remoteMgmtIp,
      linkedDeviceId: row.matchedNeighbor.linkedDeviceId,
      reason: categorySummary(row.likelyCauseCategory),
    });
  }

  const topRow =
    relevantRows
      .slice()
      .sort((a, b) => {
        const byPriority = categoryPriority(b.likelyCauseCategory) - categoryPriority(a.likelyCauseCategory);
        if (byPriority !== 0) return byPriority;
        return confidenceScore(b.confidence) - confidenceScore(a.confidence);
      })[0] ?? null;

  const likelyIssueCategory = topRow?.likelyCauseCategory ?? "healthy";
  const confidence = topRow?.confidence ?? "high";
  const issueRows = relevantRows.filter((row) => row.likelyCauseCategory !== "healthy");
  const affectedUplinkCount = issueRows.filter((row) => row.hasRelationshipEvidence || row.hasDocumentedFiber).length;

  const summary =
    likelyIssueCategory === "healthy"
      ? "No likely uplink or fiber-path issues detected from current local collector evidence."
      : `Likely ${categorySummary(likelyIssueCategory)} on ${issueRows.length} interface(s); ${blastRadiusCandidates.length} one-hop blast radius candidate(s).`;

  return {
    deviceId: device.id,
    hostname: device.hostname ?? null,
    ipAddress: device.ipAddress ?? null,
    pollHealth,
    generatedAt: new Date().toISOString(),
    likelyIssueCategory,
    confidence,
    summary,
    affectedUplinkCount,
    likelyBlastRadiusCount: blastRadiusCandidates.length,
    blastRadiusCandidates,
    correlatedInterfaces: relevantRows,
  };
}
