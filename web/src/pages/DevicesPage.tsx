import { useEffect, useMemo, useState } from "react";
import { Card, Empty, PageHeader, Pill, Table } from "../components/UI";
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

  useEffect(() => {
    load();
  }, []);

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

  async function selectDevice(id: number) {
    setSelected(id);
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
    if (res.ok) {
      await selectDevice(selected);
      await loadTargets();
    }
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
                Devices are the managed assets you want to monitor. Add hostname and IP, then attach a poll profile/target to begin collection. Each device can
                have multiple targets if you want to test different profiles.
              </p>
              <p style={{ marginTop: 6 }}>
                After saving, attach a profile on the Targets page and enqueue a poll (or use Jobs → Manual enqueue) to verify connectivity before rollout.
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
          { key: "site", header: "Site" },
          { key: "type", header: "Type", render: (d: Device) => d.type || "—", minWidth: 120 },
          { key: "org", header: "Org" },
          {
            key: "ready",
            header: "Readiness",
            render: (d: Device) => {
              const targetCount = targets.filter((t) => t.deviceId === d.id).length;
              const status = targetCount > 0 ? "ready" : "needs target";
              return <Pill status={status} />;
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
              <div style={{ gridColumn: "1 / -1" }}>
                <div className="muted">Last error</div>
                <div style={{ color: "#f87171" }}>{detailDevice.pollHealth?.lastError ?? "—"}</div>
              </div>
            </div>
            <p style={{ margin: "8px 0 0 0" }}>
              Readiness: <Pill status={detailTargets.length > 0 ? "ready" : promotionNote ? "needs target" : "needs profile"} />{" "}
              {detailTargets.length === 0 ? "Attach a profile/target to begin polling." : "Targets attached; enqueue a poll to collect."}
            </p>
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

          <Card title="Targets & Polling">
            <div className="form-grid" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(220px,1fr))" }}>
              <label>
                <span>Attach profile</span>
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
                Attach Target
              </button>
              <label>
                <span>Enqueue target</span>
                <select value={enqueueTargetId ?? ""} onChange={(e) => setEnqueueTargetId(Number(e.target.value))}>
                  <option value="">Select target</option>
                  {detailTargets.map((t) => (
                    <option key={t.id} value={t.id}>
                      Target #{t.id} — profile {t.profileId} ({t.enabled ? "enabled" : "disabled"})
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
