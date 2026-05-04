import {
  findDeviceByIpOrHostname,
  getDeviceById,
  getDevicePollHealth,
  listInterfaceSnapshotsForDevice,
  listDevices,
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
export type RemoteNeighborRole = "network_infrastructure" | "server_or_host" | "unknown";
export type PathCriticalityTier = "low" | "medium" | "high";

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
  deviceId: number | null;
  localPort: string | null;
  remoteSysName: string | null;
  remotePortId: string | null;
  remoteChassisId: string | null;
  remoteMgmtIp: string | null;
  collectedAt: string;
  linkedDeviceId: number | null;
  reviewStatus: string | null;
  promotedDeviceId: number | null;
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
  reviewStatus: string | null;
  promotedDeviceId: number | null;
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
  remoteNeighborRole: RemoteNeighborRole;
  remoteNeighborRoleConfidence: UplinkCorrelationConfidence;
  pathCriticalityScore: number;
  pathCriticalityTier: PathCriticalityTier;
  drivesTopLevel: boolean;
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
  totalInterfaceCount: number;
  candidateUplinkCount: number;
  drivingUplinkCount: number;
  likelyIssueCategory: UplinkCorrelationCategory;
  confidence: UplinkCorrelationConfidence;
  summary: string;
  affectedUplinkCount: number;
  likelyBlastRadiusCount: number;
  blastRadiusCandidates: BlastRadiusCandidate[];
  correlatedInterfaces: CorrelatedInterfaceRow[];
};

export type TopologyNode = {
  id: string;
  kind: "local_device" | "unresolved_remote";
  deviceId: number | null;
  hostname: string | null;
  ipAddress: string | null;
  type: string | null;
  site: string | null;
  org: string | null;
  pollHealth: {
    currentStatus: string | null;
    lastPollAt: string | null;
    lastSuccessAt: string | null;
    lastFailureAt: string | null;
    lastError: string | null;
  } | null;
  unresolvedFingerprint: {
    remoteSysName: string | null;
    remotePortId: string | null;
    remoteChassisId: string | null;
    remoteMgmtIp: string | null;
    fingerprint: string;
    sourceDeviceId: number;
    sourceStableInterfaceKey: string;
  } | null;
};

export type TopologyEdge = {
  id: string;
  sourceNodeId: string;
  targetNodeId: string;
  sourceDeviceId: number;
  stableInterfaceKey: string;
  localIfIndex: number | null;
  localPortLabel: string;
  remoteSysName: string | null;
  remotePortId: string | null;
  remoteChassisId: string | null;
  remoteMgmtIp: string | null;
  collectedAt: string | null;
  linkedDeviceId: number | null;
  reviewStatus: string | null;
  promotedDeviceId: number | null;
  hasDocumentedFiber: boolean;
  matchedFiberConfig: CorrelationFiberSummary | null;
  remoteNeighborRole: RemoteNeighborRole;
  remoteNeighborRoleConfidence: UplinkCorrelationConfidence;
  pathCriticalityScore: number;
  pathCriticalityTier: PathCriticalityTier;
  drivesTopLevel: boolean;
  likelyCauseCategory: UplinkCorrelationCategory;
  confidence: UplinkCorrelationConfidence;
};

export type TopologySnapshot = {
  generatedAt: string;
  scope: {
    kind: "single_device" | "all_devices";
    deviceId: number | null;
  };
  counts: {
    nodeCount: number;
    edgeCount: number;
    drivingEdgeCount: number;
    unresolvedEdgeCount: number;
  };
  nodes: TopologyNode[];
  edges: TopologyEdge[];
};

function normalizePortIdentity(value: unknown): string | null {
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return null;
  return raw.replace(/\s+/g, "").replace(/["']/g, "");
}

function normalizeEdgeToken(value: unknown): string {
  const normalized = String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[^a-z0-9._:-]/g, "");
  return normalized || "na";
}

function buildRemoteFingerprint(input: {
  remoteSysName: string | null | undefined;
  remotePortId: string | null | undefined;
  remoteChassisId: string | null | undefined;
  remoteMgmtIp: string | null | undefined;
}): string {
  const chassis = normalizeEdgeToken(input.remoteChassisId);
  const mgmt = normalizeEdgeToken(input.remoteMgmtIp);
  const sys = normalizeEdgeToken(input.remoteSysName);
  const port = normalizeEdgeToken(input.remotePortId);
  return `ch:${chassis}|ip:${mgmt}|sys:${sys}|rp:${port}`;
}

function buildUnresolvedNodeId(sourceDeviceId: number, sourceStableInterfaceKey: string, remoteFingerprint: string): string {
  return `unresolved:${sourceDeviceId}:${normalizeEdgeToken(sourceStableInterfaceKey)}:${normalizeEdgeToken(remoteFingerprint)}`;
}

function buildEdgeId(sourceDeviceId: number, sourceStableInterfaceKey: string, remoteFingerprint: string): string {
  return `edge:${sourceDeviceId}:${normalizeEdgeToken(sourceStableInterfaceKey)}:${normalizeEdgeToken(remoteFingerprint)}`;
}

function buildStableInterfaceKey(ifIndex: number | null, localPortLabel: string): string {
  if (Number.isInteger(ifIndex) && Number(ifIndex) > 0) {
    return `if:${Number(ifIndex)}`;
  }
  const normalizedPort = normalizePortIdentity(localPortLabel);
  return normalizedPort ? `port:${normalizedPort}` : `port:${localPortLabel.toLowerCase()}`;
}

function buildDeviceNode(device: DeviceRow, pollHealth: DevicePollHealth): TopologyNode {
  return {
    id: `device:${device.id}`,
    kind: "local_device",
    deviceId: device.id,
    hostname: device.hostname ?? null,
    ipAddress: device.ipAddress ?? null,
    type: device.type ?? null,
    site: device.site ?? null,
    org: device.org ?? null,
    pollHealth: {
      currentStatus: pollHealth.currentStatus ?? null,
      lastPollAt: pollHealth.lastPollAt ?? null,
      lastSuccessAt: pollHealth.lastSuccessAt ?? null,
      lastFailureAt: pollHealth.lastFailureAt ?? null,
      lastError: pollHealth.lastError ?? null,
    },
    unresolvedFingerprint: null,
  };
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

function normalizeHintText(value: unknown): string {
  return String(value ?? "").trim().toLowerCase();
}

function containsAnyToken(value: string, tokens: string[]): boolean {
  if (!value) return false;
  return tokens.some((token) => value.includes(token));
}

function classifyRemoteNeighborRole(input: {
  linkedDeviceType: string | null | undefined;
  linkedDeviceId: number | null;
  reviewStatus: string | null | undefined;
  remoteSysName: string | null | undefined;
  remotePortId: string | null | undefined;
}): { role: RemoteNeighborRole; confidence: UplinkCorrelationConfidence; evidence: string[] } {
  const evidence: string[] = [];
  const linkedType = normalizeHintText(input.linkedDeviceType);
  const reviewStatus = normalizeHintText(input.reviewStatus);
  const remoteSysName = normalizeHintText(input.remoteSysName);
  const remotePortId = normalizeHintText(input.remotePortId);

  const networkTypeHints = ["switch", "router", "firewall", "distribution", "core", "hub", "gateway", "access-point", "controller"];
  const hostTypeHints = ["server", "workstation", "desktop", "laptop", "hypervisor", "virtual-machine", "vm", "host", "windows-server", "linux-server"];

  if (containsAnyToken(linkedType, networkTypeHints)) {
    evidence.push("remote_role_linked_device_type_network");
    return { role: "network_infrastructure", confidence: "high", evidence };
  }
  if (containsAnyToken(linkedType, hostTypeHints)) {
    evidence.push("remote_role_linked_device_type_host");
    return { role: "server_or_host", confidence: "high", evidence };
  }

  const networkNameHint = /\b(core|distribution|dist|agg|aggregation|spine|leaf|switch|router|firewall|gateway|transit|wan|mdf|idf)\b/.test(
    remoteSysName,
  );
  const hostNameHint = /\b(server|srv|host|hyperv|hyper-v|esxi|proxmox|kvm|vmware|workstation|desktop|laptop|node)\b/.test(remoteSysName);
  const networkPortHint =
    /^(gi|te|fo|fa|et|ge|xe)\b/.test(remotePortId) ||
    /(gigabitethernet|fastethernet|tengig|twentyfivegig|fortygig|hundredgig|port-?channel|bundle-ether|ethernet\d*\/)/.test(remotePortId);
  const hostPortHint = /^(eth\d+|eno\d+|ens\d+|enp\d+s?\d*|bond\d+|team\d+|veth\d*|docker\d*|tap\d*|vmnic\d+|lo\d*|wlan\d+|wlp\d*)$/.test(
    remotePortId,
  );

  if (networkNameHint || networkPortHint) {
    evidence.push("remote_role_name_or_port_network_hint");
    if (input.linkedDeviceId && (reviewStatus === "linked" || reviewStatus === "promoted")) {
      evidence.push("remote_role_linked_review_hint");
      return { role: "network_infrastructure", confidence: "high", evidence };
    }
    return { role: "network_infrastructure", confidence: "medium", evidence };
  }
  if (hostNameHint || hostPortHint) {
    evidence.push("remote_role_name_or_port_host_hint");
    if (input.linkedDeviceId && (reviewStatus === "linked" || reviewStatus === "promoted")) {
      evidence.push("remote_role_linked_review_hint");
      return { role: "server_or_host", confidence: "high", evidence };
    }
    return { role: "server_or_host", confidence: "medium", evidence };
  }

  if (input.linkedDeviceId && reviewStatus === "ignored") {
    evidence.push("remote_role_review_ignored");
    return { role: "unknown", confidence: "medium", evidence };
  }

  evidence.push("remote_role_insufficient_signal");
  return { role: "unknown", confidence: "low", evidence };
}

function scorePathCriticality(input: {
  localPortLabel: string;
  adminStatus: StatusNormalized;
  operStatus: StatusNormalized;
  hasRelationshipEvidence: boolean;
  hasDocumentedFiber: boolean;
  linkedDeviceResolved: boolean;
  remoteNeighborRole: RemoteNeighborRole;
  remoteNeighborRoleConfidence: UplinkCorrelationConfidence;
}): { score: number; tier: PathCriticalityTier; evidence: string[] } {
  const evidence: string[] = [];
  let score = 0;

  if (input.hasDocumentedFiber) {
    score += 40;
    evidence.push("criticality_documented_fiber");
  }
  if (input.hasRelationshipEvidence) {
    score += 35;
    evidence.push("criticality_relationship_evidence");
  }
  if (input.remoteNeighborRole === "network_infrastructure") {
    score += 25;
    evidence.push("criticality_network_neighbor_role");
  } else if (input.remoteNeighborRole === "server_or_host") {
    score -= 20;
    evidence.push("criticality_host_neighbor_role");
  } else {
    evidence.push("criticality_unknown_neighbor_role");
  }
  if (input.linkedDeviceResolved) {
    score += 10;
    evidence.push("criticality_linked_device_resolved");
  }
  if (input.remoteNeighborRoleConfidence === "high") score += 8;
  else if (input.remoteNeighborRoleConfidence === "medium") score += 4;

  if (input.adminStatus === "up" && input.operStatus === "down") {
    score += 8;
    evidence.push("criticality_admin_up_oper_down");
  } else if (input.adminStatus === "down") {
    score -= 8;
    evidence.push("criticality_admin_down");
  } else if (input.adminStatus === "up" && input.operStatus === "up") {
    score += 2;
    evidence.push("criticality_admin_up_oper_up");
  }

  if (hasExplicitUplinkIntentLabel(input.localPortLabel)) {
    score += 5;
    evidence.push("criticality_explicit_uplink_intent");
  }

  if (score < 0) score = 0;
  if (score > 100) score = 100;

  const tier: PathCriticalityTier = score >= 70 ? "high" : score >= 45 ? "medium" : "low";
  return { score, tier, evidence };
}

function shouldDriveTopLevel(input: {
  hasRelationshipEvidence: boolean;
  hasDocumentedFiber: boolean;
  remoteNeighborRole: RemoteNeighborRole;
  pathCriticalityScore: number;
  pathCriticalityTier: PathCriticalityTier;
}): boolean {
  const hasCoreEvidence = input.hasRelationshipEvidence || input.hasDocumentedFiber;
  if (!hasCoreEvidence) return false;
  if (input.remoteNeighborRole === "server_or_host") return false;
  if (input.remoteNeighborRole === "network_infrastructure") {
    return input.pathCriticalityTier !== "low";
  }
  return input.pathCriticalityScore >= 80 && input.hasRelationshipEvidence && input.hasDocumentedFiber;
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

function isExcludedByDefaultInterfaceLabel(label: string): boolean {
  const normalized = String(label ?? "").trim().toLowerCase();
  if (!normalized) return true;
  if (/^(vl|vlan)\b/.test(normalized)) return true;
  if (/^loopback\b|^lo\d*$/.test(normalized)) return true;
  if (/^tunnel\b|^tu\d*$/.test(normalized)) return true;
  if (/^port-?channel\b|^po\d+/.test(normalized)) return true;
  if (/^null\d*$/.test(normalized)) return true;
  if (/^nvi\d*$/.test(normalized)) return true;
  if (/^bdi\d*$/.test(normalized)) return true;
  if (/^irb\d*$/.test(normalized)) return true;
  if (/^veth\b|^virtual\b|^docker\b|^tap\b|^tun\d*$/.test(normalized)) return true;
  if (/(^|[-_ .])(stack|internal|backplane|fabric|cpu)([-_ .]|$)/.test(normalized)) return true;
  return false;
}

function hasExplicitUplinkIntentLabel(label: string): boolean {
  const normalized = String(label ?? "").trim().toLowerCase();
  if (!normalized) return false;
  return /\b(uplink|trunk|core|backbone|wan|transit|peer)\b/.test(normalized);
}

function isCandidateUplinkInterface(row: CorrelatedInterfaceRow): boolean {
  if (row.hasDocumentedFiber || row.hasRelationshipEvidence) return true;
  if (isExcludedByDefaultInterfaceLabel(row.localPortLabel)) return false;
  return hasExplicitUplinkIntentLabel(row.localPortLabel);
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
    const hasRelationshipEvidence = Boolean(matchedNeighbor);
    const hasDocumentedFiber = hasAnyFiberValue(matchedFiber ?? null);

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

    const classification = classifyInterface({
      adminStatus,
      operStatus,
      hasRelationshipEvidence,
      hasDocumentedFiber,
      remoteLikelyDown: Boolean(remoteLikelyDown),
      lowTraffic,
    });
    const remoteRole = classifyRemoteNeighborRole({
      linkedDeviceType: linkedDevice?.type ?? null,
      linkedDeviceId,
      reviewStatus: matchedNeighbor?.reviewStatus ?? null,
      remoteSysName: matchedNeighbor?.remoteSysName ?? null,
      remotePortId: matchedNeighbor?.remotePortId ?? null,
    });
    const criticality = scorePathCriticality({
      localPortLabel,
      adminStatus,
      operStatus,
      hasRelationshipEvidence,
      hasDocumentedFiber,
      linkedDeviceResolved: Boolean(linkedDeviceId),
      remoteNeighborRole: remoteRole.role,
      remoteNeighborRoleConfidence: remoteRole.confidence,
    });
    const drivesTopLevel = shouldDriveTopLevel({
      hasRelationshipEvidence,
      hasDocumentedFiber,
      remoteNeighborRole: remoteRole.role,
      pathCriticalityScore: criticality.score,
      pathCriticalityTier: criticality.tier,
    });

    const matchedNeighborSummary: CorrelationNeighborSummary | null = matchedNeighbor
      ? {
          remoteSysName: matchedNeighbor.remoteSysName ?? null,
          remotePortId: matchedNeighbor.remotePortId ?? null,
          remoteChassisId: matchedNeighbor.remoteChassisId ?? null,
          remoteMgmtIp: matchedNeighbor.remoteMgmtIp ?? null,
          collectedAt: matchedNeighbor.collectedAt,
          linkedDeviceId,
          reviewStatus: matchedNeighbor.reviewStatus ?? null,
          promotedDeviceId: matchedNeighbor.promotedDeviceId ?? null,
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
      remoteNeighborRole: remoteRole.role,
      remoteNeighborRoleConfidence: remoteRole.confidence,
      pathCriticalityScore: criticality.score,
      pathCriticalityTier: criticality.tier,
      drivesTopLevel,
      likelyCauseCategory: classification.category,
      confidence: classification.confidence,
      evidence: [...classification.evidence, ...remoteRole.evidence, ...criticality.evidence],
    };
  });

  const candidateRows = correlatedRows.filter((row) => isCandidateUplinkInterface(row));
  const drivingRows = candidateRows.filter((row) => row.drivesTopLevel);

  const blastRadiusCandidates: BlastRadiusCandidate[] = [];
  const seenBlast = new Set<string>();
  for (const row of drivingRows) {
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
    drivingRows
      .slice()
      .sort((a, b) => {
        const byPriority = categoryPriority(b.likelyCauseCategory) - categoryPriority(a.likelyCauseCategory);
        if (byPriority !== 0) return byPriority;
        const byConfidence = confidenceScore(b.confidence) - confidenceScore(a.confidence);
        if (byConfidence !== 0) return byConfidence;
        return b.pathCriticalityScore - a.pathCriticalityScore;
      })[0] ?? null;

  const likelyIssueCategory = topRow?.likelyCauseCategory ?? "degraded_unknown";
  const confidence = topRow?.confidence ?? "low";
  const issueRows = drivingRows.filter((row) => row.likelyCauseCategory !== "healthy");
  const affectedUplinkCount = issueRows.length;

  const summary = drivingRows.length
    ? likelyIssueCategory === "healthy"
      ? "No likely uplink or fiber-path issues detected from high-confidence network-uplink evidence."
      : `Likely ${categorySummary(likelyIssueCategory)} on ${issueRows.length} high-confidence uplink interface(s); ${blastRadiusCandidates.length} one-hop blast radius candidate(s).`
    : candidateRows.length
      ? "Candidate uplink evidence is present, but no high-confidence network-uplink path currently qualifies to drive top-level conclusions."
      : "No candidate uplink interfaces matched documented fiber, relationship evidence, or explicit uplink intent labels.";

  return {
    deviceId: device.id,
    hostname: device.hostname ?? null,
    ipAddress: device.ipAddress ?? null,
    pollHealth,
    generatedAt: new Date().toISOString(),
    totalInterfaceCount: correlatedRows.length,
    candidateUplinkCount: candidateRows.length,
    drivingUplinkCount: drivingRows.length,
    likelyIssueCategory,
    confidence,
    summary,
    affectedUplinkCount,
    likelyBlastRadiusCount: blastRadiusCandidates.length,
    blastRadiusCandidates,
    correlatedInterfaces: candidateRows,
  };
}

export function buildTopologySnapshot(db: DB, input?: { deviceId?: number }): TopologySnapshot | null {
  const scopeDeviceId = input?.deviceId;
  if (scopeDeviceId !== undefined && scopeDeviceId !== null) {
    const exists = getDeviceById(db, scopeDeviceId);
    if (!exists) return null;
  }

  const scopedDevices = scopeDeviceId
    ? listDevices(db).filter((device) => device.id === scopeDeviceId)
    : listDevices(db);

  const nodeMap = new Map<string, TopologyNode>();
  const edgeMap = new Map<string, TopologyEdge>();

  const ensureLocalDeviceNode = (deviceId: number) => {
    const nodeId = `device:${deviceId}`;
    if (nodeMap.has(nodeId)) return;
    const device = getDeviceById(db, deviceId);
    if (!device) return;
    nodeMap.set(nodeId, buildDeviceNode(device, getDevicePollHealth(db, deviceId)));
  };

  const addUnresolvedNode = (edge: TopologyEdge, sourceStableInterfaceKey: string, remoteFingerprint: string) => {
    const unresolvedId = buildUnresolvedNodeId(edge.sourceDeviceId, sourceStableInterfaceKey, remoteFingerprint);
    if (!nodeMap.has(unresolvedId)) {
      nodeMap.set(unresolvedId, {
        id: unresolvedId,
        kind: "unresolved_remote",
        deviceId: null,
        hostname: edge.remoteSysName,
        ipAddress: edge.remoteMgmtIp,
        type: null,
        site: null,
        org: null,
        pollHealth: null,
        unresolvedFingerprint: {
          remoteSysName: edge.remoteSysName,
          remotePortId: edge.remotePortId,
          remoteChassisId: edge.remoteChassisId,
          remoteMgmtIp: edge.remoteMgmtIp,
          fingerprint: remoteFingerprint,
          sourceDeviceId: edge.sourceDeviceId,
          sourceStableInterfaceKey,
        },
      });
    }
    return unresolvedId;
  };

  const addEdge = (edge: TopologyEdge) => {
    const remoteFingerprint = buildRemoteFingerprint({
      remoteSysName: edge.remoteSysName,
      remotePortId: edge.remotePortId,
      remoteChassisId: edge.remoteChassisId,
      remoteMgmtIp: edge.remoteMgmtIp,
    });
    const edgeId = buildEdgeId(edge.sourceDeviceId, edge.stableInterfaceKey, remoteFingerprint);
    if (edgeMap.has(edgeId)) return;
    const resolvedTargetId =
      Number.isInteger(edge.linkedDeviceId) && Number(edge.linkedDeviceId) > 0
        ? `device:${Number(edge.linkedDeviceId)}`
        : null;
    const targetNodeId = resolvedTargetId ?? addUnresolvedNode(edge, edge.stableInterfaceKey, remoteFingerprint);
    if (resolvedTargetId) {
      ensureLocalDeviceNode(Number(edge.linkedDeviceId));
    }
    edgeMap.set(edgeId, {
      ...edge,
      id: edgeId,
      sourceNodeId: `device:${edge.sourceDeviceId}`,
      targetNodeId,
    });
  };

  for (const device of scopedDevices) {
    ensureLocalDeviceNode(device.id);
    const correlation = buildUplinkCorrelationSnapshot(db, device.id);
    if (!correlation) continue;

    for (const row of correlation.correlatedInterfaces) {
      if (!row.matchedNeighbor) continue;
      addEdge({
        id: "",
        sourceNodeId: "",
        targetNodeId: "",
        sourceDeviceId: device.id,
        stableInterfaceKey: row.stableInterfaceKey,
        localIfIndex: row.localIfIndex,
        localPortLabel: row.localPortLabel,
        remoteSysName: row.matchedNeighbor.remoteSysName,
        remotePortId: row.matchedNeighbor.remotePortId,
        remoteChassisId: row.matchedNeighbor.remoteChassisId,
        remoteMgmtIp: row.matchedNeighbor.remoteMgmtIp,
        collectedAt: row.matchedNeighbor.collectedAt,
        linkedDeviceId: row.matchedNeighbor.linkedDeviceId,
        reviewStatus: row.matchedNeighbor.reviewStatus,
        promotedDeviceId: row.matchedNeighbor.promotedDeviceId,
        hasDocumentedFiber: row.hasDocumentedFiber,
        matchedFiberConfig: row.matchedFiberConfig ?? null,
        remoteNeighborRole: row.remoteNeighborRole,
        remoteNeighborRoleConfidence: row.remoteNeighborRoleConfidence,
        pathCriticalityScore: row.pathCriticalityScore,
        pathCriticalityTier: row.pathCriticalityTier,
        drivesTopLevel: row.drivesTopLevel,
        likelyCauseCategory: row.likelyCauseCategory,
        confidence: row.confidence,
      });
    }

    const interfaces = listInterfaceSnapshotsForDevice(db, device.id) as Array<{
      ifIndex: number | null;
      ifName: string | null;
      ifDescr: string | null;
      ifAlias: string | null;
    }>;
    const interfaceByPort = new Map<string, { stableInterfaceKey: string; localIfIndex: number | null; localPortLabel: string }>();
    for (const iface of interfaces) {
      const label = String(iface.ifName ?? iface.ifDescr ?? iface.ifAlias ?? (iface.ifIndex != null ? `ifIndex-${iface.ifIndex}` : "unknown")).trim();
      const stable = buildStableInterfaceKey(Number.isInteger(iface.ifIndex) ? Number(iface.ifIndex) : null, label);
      const candidateValues = [label, iface.ifName, iface.ifDescr, iface.ifAlias];
      for (const candidate of candidateValues) {
        const key = normalizePortIdentity(candidate);
        if (!key || interfaceByPort.has(key)) continue;
        interfaceByPort.set(key, {
          stableInterfaceKey: stable,
          localIfIndex: Number.isInteger(iface.ifIndex) ? Number(iface.ifIndex) : null,
          localPortLabel: label,
        });
      }
    }

    const neighbors = listNeighborsWithReview(db, { deviceId: device.id }) as NeighborWithReviewRow[];
    for (const neighbor of neighbors) {
      if (Number.isInteger(neighbor.linkedDeviceId) && Number(neighbor.linkedDeviceId) > 0) continue;
      const localPortLabel = String(neighbor.localPort ?? "").trim();
      if (!localPortLabel) continue;
      const portKey = normalizePortIdentity(localPortLabel);
      const matchedInterface = portKey ? interfaceByPort.get(portKey) ?? null : null;
      const stableInterfaceKey = matchedInterface?.stableInterfaceKey ?? buildStableInterfaceKey(null, localPortLabel);
      const localIfIndex = matchedInterface?.localIfIndex ?? null;
      const resolvedPortLabel = matchedInterface?.localPortLabel ?? localPortLabel;
      addEdge({
        id: "",
        sourceNodeId: "",
        targetNodeId: "",
        sourceDeviceId: device.id,
        stableInterfaceKey,
        localIfIndex,
        localPortLabel: resolvedPortLabel,
        remoteSysName: neighbor.remoteSysName ?? null,
        remotePortId: neighbor.remotePortId ?? null,
        remoteChassisId: neighbor.remoteChassisId ?? null,
        remoteMgmtIp: neighbor.remoteMgmtIp ?? null,
        collectedAt: neighbor.collectedAt ?? null,
        linkedDeviceId: null,
        reviewStatus: neighbor.reviewStatus ?? null,
        promotedDeviceId: Number.isInteger(neighbor.promotedDeviceId) ? Number(neighbor.promotedDeviceId) : null,
        hasDocumentedFiber: false,
        matchedFiberConfig: null,
        remoteNeighborRole: "unknown",
        remoteNeighborRoleConfidence: "low",
        pathCriticalityScore: 0,
        pathCriticalityTier: "low",
        drivesTopLevel: false,
        likelyCauseCategory: "degraded_unknown",
        confidence: "low",
      });
    }
  }

  const edges = Array.from(edgeMap.values()).sort((a, b) => a.id.localeCompare(b.id));
  const nodes = Array.from(nodeMap.values()).sort((a, b) => a.id.localeCompare(b.id));
  const drivingEdgeCount = edges.filter((edge) => edge.drivesTopLevel).length;
  const unresolvedEdgeCount = edges.filter((edge) => edge.targetNodeId.startsWith("unresolved:")).length;

  return {
    generatedAt: new Date().toISOString(),
    scope: {
      kind: scopeDeviceId ? "single_device" : "all_devices",
      deviceId: scopeDeviceId ?? null,
    },
    counts: {
      nodeCount: nodes.length,
      edgeCount: edges.length,
      drivingEdgeCount,
      unresolvedEdgeCount,
    },
    nodes,
    edges,
  };
}
