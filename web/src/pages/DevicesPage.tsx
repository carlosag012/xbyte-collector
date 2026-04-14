import { useEffect, useMemo, useRef, useState } from "react";
import { Card, PageHeader, Pill, Table, useToast } from "../components/UI";
import { Modal } from "../components/Modal";

type DevicePollHealth = {
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

type Device = {
  id: number;
  hostname: string;
  ipAddress: string;
  enabled: boolean;
  type?: string | null;
  site?: string | null;
  org?: string | null;
  pollHealth?: DevicePollHealth;
};

type Profile = { id: number; name: string; kind: "ping" | "snmp"; enabled: boolean };
type Target = { id: number; deviceId: number; profileId: number; enabled: boolean };
type SystemSnapshot = { sys_name?: string; sys_descr?: string; sys_object_id?: string; sys_uptime?: string };
type InterfaceRow = {
  ifIndex?: number;
  ifName?: string;
  ifDescr?: string;
  ifAlias?: string;
  adminStatus?: string;
  operStatus?: string;
  speed?: number | null;
  mtu?: number | null;
  mac?: string | null;
};
type NeighborRow = {
  localPort?: string | null;
  remoteSysName?: string | null;
  remotePortId?: string | null;
  remoteChassisId?: string | null;
  remoteMgmtIp?: string | null;
};

function hasActiveSnmpError(health?: DevicePollHealth) {
  if (!health?.lastSnmpError) return false;
  if (!health.lastSnmpSuccessAt) return true;
  if (!health.lastSnmpFailureAt) return false;
  return health.lastSnmpFailureAt >= health.lastSnmpSuccessAt;
}

function getOnboardingNextAction(health?: DevicePollHealth) {
  if (!health?.hasSnmpBinding) return "Add an SNMP polling binding to collect interfaces.";
  if (!health?.hasPingBinding) return "Add a ping polling binding for reachability.";
  if (!health?.cloudSyncConfigured) return "Configure cloud sync in Licensing to send data upstream.";
  if (hasActiveSnmpError(health)) return `Last SNMP attempt failed: ${health?.lastSnmpError ?? "check credentials/timeouts."}`;
  if (!health?.lastSnmpSuccessAt) return "Polling binding is active; waiting for first SNMP success.";
  return `SNMP healthy. Last success: ${health.lastSnmpSuccessAt}`;
}

export default function DevicesPage() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [targets, setTargets] = useState<Target[]>([]);
  const [selected, setSelected] = useState<number | null>(null);
  const [search, setSearch] = useState("");
  const [form, setForm] = useState({ hostname: "", ipAddress: "", site: "", org: "", type: "" });
  const [editing, setEditing] = useState<Device | null>(null);
  const [snapshot, setSnapshot] = useState<SystemSnapshot | null>(null);
  const [interfaces, setInterfaces] = useState<InterfaceRow[]>([]);
  const [neighbors, setNeighbors] = useState<NeighborRow[]>([]);
  const [detailTargets, setDetailTargets] = useState<Target[]>([]);
  const [enqueueTargetId, setEnqueueTargetId] = useState<number | null>(null);
  const [attachProfileId, setAttachProfileId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [promotionNote, setPromotionNote] = useState<string | null>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [historyModal, setHistoryModal] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const refreshInFlight = useRef(false);
  const toast = useToast();

  const targetsByDevice = useMemo(() => {
    const map = new Map<number, Target[]>();
    targets.forEach((t) => {
      if (!map.has(t.deviceId)) map.set(t.deviceId, []);
      map.get(t.deviceId)!.push(t);
    });
    return map;
  }, [targets]);

  const profileKind = useMemo(() => {
    const m = new Map<number, "ping" | "snmp">();
    profiles.forEach((p) => m.set(p.id, p.kind));
    return m;
  }, [profiles]);

  useEffect(() => {
    load();
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      if (refreshInFlight.current) return;
      refreshInFlight.current = true;
      void (async () => {
        try {
          await Promise.all([loadDevices(), loadTargets()]);
          if (selected) await loadDeviceDetail(selected);
        } finally {
          refreshInFlight.current = false;
        }
      })();
    }, 7000);
    return () => clearInterval(interval);
  }, [selected]);

  async function load() {
    setLoading(true);
    await Promise.all([loadDevices(), loadProfiles(), loadTargets()]);
    setLoading(false);
  }

  async function loadDevices() {
    const res = await fetch("/api/devices", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setDevices(data.devices ?? []);
    }
  }

  async function loadProfiles() {
    const res = await fetch("/api/poll-profiles", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setProfiles(data.profiles ?? []);
    }
  }

  async function loadTargets() {
    const res = await fetch("/api/poll-targets", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setTargets(data.targets ?? []);
    }
  }

  const filtered = useMemo(() => {
    const term = search.toLowerCase().trim();
    if (!term) return devices;
    return devices.filter((d) => d.hostname.toLowerCase().includes(term) || d.ipAddress.toLowerCase().includes(term));
  }, [devices, search]);

  async function submitDevice(e: React.FormEvent) {
    e.preventDefault();
    if (!form.hostname || !form.ipAddress) {
      setFormError("Hostname and IP address are required.");
      return;
    }
    setFormError(null);
    const payload: any = {
      hostname: form.hostname,
      ipAddress: form.ipAddress,
      site: form.site || undefined,
      org: form.org || undefined,
      type: form.type || undefined,
      enabled: true,
    };
    let url = "/api/devices";
    if (editing) {
      payload.id = editing.id;
      payload.enabled = editing.enabled;
      url = "/api/devices/update";
    }
    const res = await fetch(url, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.ok) {
      await loadDevices();
      setForm({ hostname: "", ipAddress: "", site: "", org: "", type: "" });
      setEditing(null);
    }
  }

  function startEdit(d: Device) {
    setEditing(d);
    setForm({ hostname: d.hostname, ipAddress: d.ipAddress, site: d.site ?? "", org: d.org ?? "", type: d.type ?? "" });
  }

  async function toggleEnabled(d: Device) {
    const res = await fetch("/api/devices/update", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: d.id, enabled: !d.enabled }),
    });
    if (res.ok) await loadDevices();
  }

  async function loadDeviceDetail(id: number) {
    const [sysRes, ifRes, neighRes, targRes, revRes, histRes] = await Promise.all([
      fetch(`/api/tdt/system-snapshots?deviceId=${id}`, { credentials: "include" }),
      fetch(`/api/tdt/interfaces?deviceId=${id}`, { credentials: "include" }),
      fetch(`/api/tdt/lldp-neighbors?deviceId=${id}`, { credentials: "include" }),
      fetch(`/api/poll-targets?deviceId=${id}`, { credentials: "include" }),
      fetch(`/api/neighbors?deviceId=${id}`, { credentials: "include" }),
      fetch(`/api/neighbors/history?deviceId=${id}`, { credentials: "include" }),
    ]);
    if (sysRes.ok) {
      const d = await sysRes.json();
      setSnapshot(d.snapshots?.[0] ?? null);
    }
    if (ifRes.ok) {
      const d = await ifRes.json();
      setInterfaces(d.interfaces ?? []);
    }
    if (neighRes.ok) {
      const d = await neighRes.json();
      setNeighbors(d.neighbors ?? []);
    }
    if (targRes.ok) {
      const d = await targRes.json();
      setDetailTargets(d.targets ?? []);
      const firstTarget = d.targets?.[0];
      setEnqueueTargetId(firstTarget ? firstTarget.id : null);
    }
    if (revRes.ok) {
      const d = await revRes.json();
      const promotedFrom = (d.neighbors ?? []).find((n: any) => n.promotedDeviceId === id);
      const linkedFrom = (d.neighbors ?? []).find((n: any) => n.linkedDeviceId === id);
      if (promotedFrom) setPromotionNote("Promoted from LLDP neighbor");
      else if (linkedFrom) setPromotionNote("Linked from LLDP neighbor");
      else setPromotionNote(null);
    }
    if (histRes.ok) {
      const d = await histRes.json();
      setHistory(d.events ?? []);
    }
  }

  async function selectDevice(id: number) {
    setSelected(id);
    await loadDeviceDetail(id);
    const firstProfile = profiles.find((p) => p.enabled);
    setAttachProfileId(firstProfile ? firstProfile.id : null);
  }

  async function attachTarget() {
    if (!selected || !attachProfileId) return;
    const res = await fetch("/api/poll-targets", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ deviceId: selected, profileId: attachProfileId, enabled: true }),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok) {
      await loadTargets();
      await loadDevices();
      await loadDeviceDetail(selected);
      if (data?.duplicate) {
        toast({ message: data?.message || "Polling binding already exists for this device/profile.", tone: "warning" });
        return;
      }
      if (data?.fastEnqueue?.attempted && data?.fastEnqueue?.ok) {
        toast({ message: data?.fastEnqueue?.message || "Polling binding attached and first SNMP poll queued.", tone: "success" });
        return;
      }
      if (data?.fastEnqueue?.attempted && !data?.fastEnqueue?.ok) {
        toast({
          message:
            data?.fastEnqueue?.message ||
            "Polling binding attached, but first SNMP poll was not queued immediately. Scheduler will retry shortly.",
          tone: "warning",
        });
        return;
      }
      toast({ message: "Polling binding attached.", tone: "success" });
      return;
    }
    toast({ message: data?.message || data?.error || "Failed to attach polling binding.", tone: "error" });
  }

  async function enqueueJob() {
    if (!enqueueTargetId) return;
    await fetch("/api/poll-jobs/enqueue", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ targetId: enqueueTargetId }),
    });
  }

  const detailDevice = devices.find((d) => d.id === selected) || null;

  return (
    <div>
      <PageHeader
        title="Devices"
        subtitle="Manage monitored devices, view discovery data, and launch polls."
        action={<input placeholder="Search hostname or IP" value={search} onChange={(e) => setSearch(e.target.value)} style={{ width: 220 }} />}
      />

      <div className="cards cards-full" style={{ marginBottom: 16 }}>
        <Card title={editing ? "Edit Device" : "Add Device"}>
          <div className="form-row">
            <form onSubmit={submitDevice} className="form-panel">
              <div className="form-columns">
                <div className="form-col">
                  <label>
                    <span>Hostname</span>
                    <input value={form.hostname} onChange={(e) => setForm({ ...form, hostname: e.target.value })} required />
                  </label>
                </div>
                <div className="form-col">
                  <label>
                    <span>Site</span>
                    <input value={form.site} onChange={(e) => setForm({ ...form, site: e.target.value })} />
                  </label>
                </div>
                <div className="form-col">
                  <label>
                    <span>Type</span>
                    <select value={form.type} onChange={(e) => setForm({ ...form, type: e.target.value })}>
                      <option value="">Select type</option>
                      {[
                        "server",
                        "workstation",
                        "switch",
                        "router",
                        "firewall",
                        "camera",
                        "iot-device",
                        "sensor",
                        "ups",
                        "printer",
                        "access-point",
                        "controller",
                        "storage",
                        "other",
                      ].map((t) => (
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
              </div>
              <div className="form-columns">
                <div className="form-col">
                  <label>
                    <span>IP Address</span>
                    <input value={form.ipAddress} onChange={(e) => setForm({ ...form, ipAddress: e.target.value })} required />
                  </label>
                </div>
                <div className="form-col">
                  <label>
                    <span>Org</span>
                    <input value={form.org} onChange={(e) => setForm({ ...form, org: e.target.value })} />
                  </label>
                </div>
              </div>
              <div className="form-actions" style={{ display: "flex", gap: 8, flexDirection: "column", maxWidth: 420 }}>
                <button type="submit" className="btn-collector">
                  <span className="btn-collector-label">{editing ? "Update" : "Create Device"}</span>
                </button>
                {editing && (
                  <button type="button" className="btn-secondary" onClick={() => setEditing(null)}>
                    Cancel
                  </button>
                )}
              </div>
              {formError && <p style={{ color: "#f87171", margin: "6px 0 0 0" }}>{formError}</p>}
            </form>
            <div className="about-panel">
              <strong>About devices</strong>
              <p style={{ marginTop: 6 }}>
                Devices are the managed assets you want to monitor. Add hostname and IP, then attach a polling binding to begin collection. Each device can
                have multiple bindings if you want to test different profiles.
              </p>
              <p style={{ marginTop: 6 }}>
                After saving, attach a profile on the Polling Bindings page and enqueue a poll (or use Jobs → Manual enqueue) to verify connectivity before
                rollout.
              </p>
              <p style={{ marginTop: 6, color: "var(--muted)" }}>
                Minimum for interfaces: SNMP profile + enabled polling binding + cloud sync configured. Without an SNMP binding, interfaces will not
                leave the collector.
              </p>
            </div>
          </div>
        </Card>
      </div>

      <Table
        columns={[
          { key: "hostname", header: "Hostname" },
          { key: "ipAddress", header: "IP" },
          { key: "enabled", header: "Status", render: (d: Device) => <Pill status={d.enabled ? "enabled" : "disabled"} /> },
          {
            key: "poll-status",
            header: "Current Poll Status",
            render: (d: Device) => <Pill status={d.pollHealth?.currentStatus ?? "idle"} />,
            minWidth: 150,
          },
          {
            key: "last-poll",
            header: "Last Poll",
            render: (d: Device) => d.pollHealth?.lastPollAt ?? "—",
            minWidth: 160,
          },
          {
            key: "last-success",
            header: "Last Success",
            render: (d: Device) => d.pollHealth?.lastSuccessAt ?? "—",
            minWidth: 160,
          },
          {
            key: "last-failure",
            header: "Last Failure",
            render: (d: Device) => d.pollHealth?.lastFailureAt ?? "—",
            minWidth: 160,
          },
          {
            key: "active-profile",
            header: "Active SNMP Profile",
            render: (d: Device) => d.pollHealth?.activeSnmpProfile ?? "—",
            minWidth: 160,
          },
          {
            key: "last-error",
            header: "Last Error",
            render: (d: Device) => (d.pollHealth?.lastError ? String(d.pollHealth.lastError).slice(0, 80) : "—"),
            minWidth: 200,
          },
          {
            key: "next-action",
            header: "Next Action",
            render: (d: Device) => getOnboardingNextAction(d.pollHealth),
            minWidth: 280,
          },
          { key: "site", header: "Site" },
          { key: "type", header: "Type", render: (d: Device) => d.type || "—", minWidth: 120 },
          { key: "org", header: "Org" },
          {
            key: "ready",
            header: "Readiness",
            render: (d: Device) => {
              const h = d.pollHealth;
              if (!h?.hasSnmpBinding) return <Pill status="needs target" label="No SNMP polling binding" />;
              if (!h?.hasPingBinding) return <Pill status="needs target" label="No ping binding" />;
              if (!h?.cloudSyncConfigured) return <Pill status="needs target" label="Cloud sync disabled" />;
              if (hasActiveSnmpError(h))
                return <Pill status="warning" label="Last SNMP failed" />;
              return <Pill status="ready" label="SNMP OK" />;
            },
          },
          {
            key: "actions",
            header: "Actions",
            render: (d: Device) => (
              <div style={{ display: "flex", gap: 8 }}>
                <button className="btn-secondary" onClick={() => selectDevice(d.id)}>
                  View
                </button>
                <button className="btn-secondary" onClick={() => startEdit(d)}>
                  Edit
                </button>
                <button className="btn-secondary" onClick={() => toggleEnabled(d)}>
                  {d.enabled ? "Disable" : "Enable"}
                </button>
              </div>
            ),
          },
        ]}
        data={filtered}
        empty={loading ? "Loading..." : "No devices"}
      />

      {detailDevice && (
        <div style={{ marginTop: 20, display: "grid", gap: 16 }}>
          <Card title={`Device Detail — ${detailDevice.hostname}`}>
            <p style={{ margin: 0, color: "var(--muted)" }}>IP: {detailDevice.ipAddress}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Site: {detailDevice.site ?? "—"}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Org: {detailDevice.org ?? "—"}</p>
            {promotionNote && <p style={{ margin: "6px 0 0 0", color: "#a855f7" }}>{promotionNote}</p>}
            <p style={{ margin: "8px 0 0 0" }}>
              System: {snapshot?.sys_name ?? "—"} | {snapshot?.sys_descr ?? ""} | {snapshot?.sys_object_id ?? ""} | {snapshot?.sys_uptime ?? ""}
            </p>
            <div className="detail-grid" style={{ marginTop: 10, display: "grid", gap: 8, gridTemplateColumns: "repeat(auto-fit, minmax(220px,1fr))" }}>
              <div>
                <div className="muted">Current Poll Status</div>
                <Pill status={detailDevice.pollHealth?.currentStatus ?? "idle"} />
              </div>
              <div>
                <div className="muted">Last polled</div>
                <div>{detailDevice.pollHealth?.lastPollAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Last success</div>
                <div>{detailDevice.pollHealth?.lastSuccessAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Last failure</div>
                <div>{detailDevice.pollHealth?.lastFailureAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Success / Failure count</div>
                <div>
                  {detailDevice.pollHealth?.successCount ?? 0} / {detailDevice.pollHealth?.failureCount ?? 0}
                </div>
              </div>
              <div>
                <div className="muted">Active profile / poller</div>
                <div>
                  {detailDevice.pollHealth?.activeSnmpProfile ?? "—"}
                  {detailDevice.pollHealth?.activeSnmpPollerId ? ` (${detailDevice.pollHealth.activeSnmpPollerId})` : ""}
                </div>
              </div>
              <div>
                <div className="muted">Last SNMP poll</div>
                <div>{detailDevice.pollHealth?.lastSnmpPollAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Last SNMP success</div>
                <div>{detailDevice.pollHealth?.lastSnmpSuccessAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Last SNMP failure</div>
                <div>{detailDevice.pollHealth?.lastSnmpFailureAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Last ping success</div>
                <div>{detailDevice.pollHealth?.lastPingSuccessAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Last ping failure</div>
                <div>{detailDevice.pollHealth?.lastPingFailureAt ?? "—"}</div>
              </div>
              <div>
                <div className="muted">Cloud sync</div>
                <div>{detailDevice.pollHealth?.cloudSyncConfigured ? "Configured" : "Not configured"}</div>
              </div>
              <div style={{ gridColumn: "1 / -1" }}>
                <div className="muted">Last error</div>
                <div style={{ color: "#f87171" }}>{detailDevice.pollHealth?.lastError ?? "—"}</div>
                {detailDevice.pollHealth?.lastSnmpError && (
                  <div style={{ color: "#f97316", marginTop: 4 }}>SNMP error: {detailDevice.pollHealth.lastSnmpError}</div>
                )}
              </div>
            </div>
            <p style={{ margin: "8px 0 0 0" }}>
              Readiness: <Pill status={detailTargets.length > 0 ? "ready" : promotionNote ? "needs target" : "needs profile"} />{" "}
              {detailTargets.length === 0 ? "Attach a polling binding to begin polling." : "Polling bindings attached; enqueue a poll to collect."}
            </p>
            {detailTargets.filter((t) => profileKind.get(t.profileId) === "snmp" && t.enabled).length === 0 && (
              <p style={{ margin: "4px 0 0 0", color: "#b45309" }}>
                No enabled SNMP polling binding. Create/enable one to collect interfaces and system data.
              </p>
            )}
            <div style={{ marginTop: 10, border: "1px solid var(--panel-border)", borderRadius: 10, padding: 10 }}>
              <div className="muted" style={{ marginBottom: 6 }}>
                Setup Checklist
              </div>
              <ul style={{ margin: 0, paddingLeft: 18, display: "grid", gap: 4 }}>
                <li>Device exists: {detailDevice.hostname}</li>
                <li>
                  {detailDevice.pollHealth?.hasSnmpBinding
                    ? "SNMP polling binding configured"
                    : "Add an SNMP polling binding to collect interfaces"}
                </li>
                <li>{detailDevice.pollHealth?.hasPingBinding ? "Ping polling binding configured" : "Add a ping polling binding for reachability"}</li>
                <li>
                  {detailDevice.pollHealth?.cloudSyncConfigured ? (
                    "Cloud sync configured"
                  ) : (
                    <>
                      Cloud sync missing. Configure credentials in <a href="/licensing">Licensing</a>.
                    </>
                  )}
                </li>
                <li>Last SNMP success: {detailDevice.pollHealth?.lastSnmpSuccessAt ?? "—"}</li>
                {hasActiveSnmpError(detailDevice.pollHealth) && (
                  <li style={{ color: "#f97316" }}>Last SNMP attempt failed: {detailDevice.pollHealth?.lastSnmpError ?? "unknown_error"}</li>
                )}
              </ul>
              <p style={{ margin: "8px 0 0 0" }}>
                <strong>Next action:</strong> {getOnboardingNextAction(detailDevice.pollHealth)}
              </p>
            </div>
          </Card>
          {history.length > 0 && (
            <Card title="History">
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <button className="btn-secondary" onClick={() => setHistoryModal(true)}>
                  View change history
                </button>
              </div>
            </Card>
          )}

          <div className="split">
            <Card title="Interfaces">
              <Table
                columns={[
                  { key: "ifName", header: "Name" },
                  { key: "ifDescr", header: "Descr" },
                  { key: "ifAlias", header: "Alias" },
                  { key: "adminStatus", header: "Admin" },
                  { key: "operStatus", header: "Oper" },
                ]}
                data={interfaces}
                empty="No interfaces yet"
              />
            </Card>
            <Card title="LLDP Neighbors">
              <Table
                columns={[
                  { key: "localPort", header: "Local Port" },
                  { key: "remoteSysName", header: "Remote Sys" },
                  { key: "remotePortId", header: "Remote Port" },
                  { key: "remoteMgmtIp", header: "Mgmt IP" },
                ]}
                data={neighbors}
                empty="No neighbors discovered"
              />
            </Card>
          </div>

          <Card title="Polling Bindings & Polling">
            <div className="form-grid" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(220px,1fr))" }}>
              <label>
                <span>Attach profile as polling binding</span>
                <select value={attachProfileId ?? ""} onChange={(e) => setAttachProfileId(Number(e.target.value))}>
                  <option value="">Select profile</option>
                  {profiles
                    .filter((p) => p.enabled)
                    .map((p) => (
                      <option key={p.id} value={p.id}>
                        {p.name} ({p.kind})
                      </option>
                    ))}
                </select>
              </label>
              <button type="button" className="btn-secondary" onClick={attachTarget} disabled={!attachProfileId}>
                Attach Polling Binding
              </button>
              <label>
                <span>Enqueue polling binding</span>
                <select value={enqueueTargetId ?? ""} onChange={(e) => setEnqueueTargetId(Number(e.target.value))}>
                  <option value="">Select polling binding</option>
                  {detailTargets.map((t) => (
                    <option key={t.id} value={t.id}>
                      Polling Binding #{t.id} — profile {t.profileId} ({t.enabled ? "enabled" : "disabled"})
                    </option>
                  ))}
                </select>
              </label>
              <button type="button" className="btn-secondary" onClick={enqueueJob} disabled={!enqueueTargetId}>
                Enqueue Poll
              </button>
            </div>
          </Card>
        </div>
      )}

      {historyModal && (
        <Modal title={`History — ${detailDevice?.hostname ?? ""}`} onClose={() => setHistoryModal(false)}>
          <ul style={{ margin: 0, paddingLeft: 18, color: "var(--muted)" }}>
            {history.map((h) => (
              <li key={h.id}>
                {h.createdAt} — {h.action} by {h.actor ?? "local-user"}
              </li>
            ))}
          </ul>
        </Modal>
      )}
    </div>
  );
}
