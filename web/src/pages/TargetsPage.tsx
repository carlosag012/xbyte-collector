import { useEffect, useMemo, useState } from "react";
import { Card, PageHeader, Pill, Table } from "../components/UI";
import { Modal } from "../components/Modal";

type Device = { id: number; hostname: string };
type Profile = { id: number; name: string; kind: "ping" | "snmp" };
type Target = { id: number; deviceId: number; profileId: number; enabled: boolean };

export default function TargetsPage() {
  const [devices, setDevices] = useState<Device[]>([]);
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [targets, setTargets] = useState<Target[]>([]);
  const [deviceFilter, setDeviceFilter] = useState<number | null>(null);
  const [profileFilter, setProfileFilter] = useState<number | null>(null);
  const [form, setForm] = useState<{ deviceId: number | ""; profileId: number | "" }>({ deviceId: "", profileId: "" });
  const [relations, setRelations] = useState<Record<number, string>>({});
  const [history, setHistory] = useState<any[]>([]);
  const [historyFor, setHistoryFor] = useState<Target | null>(null);

  useEffect(() => {
    loadAll();
  }, []);

  async function loadAll() {
    const [d, p, t, n] = await Promise.all([
      fetch("/api/devices", { credentials: "include" }),
      fetch("/api/poll-profiles", { credentials: "include" }),
      fetch("/api/poll-targets", { credentials: "include" }),
      fetch("/api/neighbors", { credentials: "include" }),
    ]);
    if (d.ok) setDevices((await d.json()).devices ?? []);
    if (p.ok) setProfiles((await p.json()).profiles ?? []);
    if (t.ok) setTargets((await t.json()).targets ?? []);
    if (n.ok) {
      const data = await n.json();
      const map: Record<number, string> = {};
      (data.neighbors ?? []).forEach((neigh: any) => {
        if (neigh.promotedDeviceId) map[neigh.promotedDeviceId] = "promoted";
        if (neigh.linkedDeviceId) map[neigh.linkedDeviceId] = "linked";
      });
      setRelations(map);
    }
  }

  const filtered = useMemo(
    () =>
      targets.filter(
        (t) => (deviceFilter ? t.deviceId === deviceFilter : true) && (profileFilter ? t.profileId === profileFilter : true)
      ),
    [targets, deviceFilter, profileFilter]
  );

  async function createTarget(e: React.FormEvent) {
    e.preventDefault();
    if (!form.deviceId || !form.profileId) return;
    const res = await fetch("/api/poll-targets", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ deviceId: form.deviceId, profileId: form.profileId, enabled: true }),
    });
    if (res.ok) {
      setForm({ deviceId: "", profileId: "" });
      await loadAll();
    }
  }

  async function openHistory(t: Target) {
    const res = await fetch(`/api/audit?entityType=target&entityId=${t.id}`, { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setHistory(data.events ?? []);
      setHistoryFor(t);
    }
  }

  async function toggle(target: Target) {
    await fetch("/api/poll-targets/update", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: target.id, enabled: !target.enabled }),
    });
    await loadAll();
  }

  async function enqueue(target: Target) {
    await fetch("/api/poll-jobs/enqueue", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ targetId: target.id }),
    });
  }

  return (
    <div>
      <PageHeader
        title="Poll Targets"
        subtitle="Link devices to profiles, then enqueue first polls."
        action={
          <div style={{ display: "flex", gap: 8 }}>
            <select value={deviceFilter ?? ""} onChange={(e) => setDeviceFilter(e.target.value ? Number(e.target.value) : null)}>
              <option value="">All devices</option>
              {devices.map((d) => (
                <option key={d.id} value={d.id}>
                  {d.hostname}
                </option>
              ))}
            </select>
            <select value={profileFilter ?? ""} onChange={(e) => setProfileFilter(e.target.value ? Number(e.target.value) : null)}>
              <option value="">All profiles</option>
              {profiles.map((p) => (
                <option key={p.id} value={p.id}>
                  {p.name}
                </option>
              ))}
            </select>
          </div>
        }
      />

      <div className="cards" style={{ marginBottom: 16 }}>
        <div style={{ maxWidth: 720, width: "100%" }}>
          <Card title="Create Target">
            <form onSubmit={createTarget} className="form-grid" style={{ gridTemplateColumns: "1fr", gap: 12, minWidth: 0 }}>
            <label>
              <span>Device</span>
              <select value={form.deviceId} onChange={(e) => setForm((f) => ({ ...f, deviceId: e.target.value ? Number(e.target.value) : "" }))}>
                <option value="">Select device</option>
                {devices.map((d) => (
                  <option key={d.id} value={d.id}>
                    {d.hostname}
                  </option>
                ))}
              </select>
              <small style={{ color: "var(--muted)", display: "block", marginTop: 4 }}>Choose the managed device that should be polled.</small>
            </label>
            <label>
              <span>Profile</span>
              <select
                value={form.profileId}
                onChange={(e) => setForm((f) => ({ ...f, profileId: e.target.value ? Number(e.target.value) : "" }))}
              >
                <option value="">Select profile</option>
                {profiles.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.name} ({p.kind})
                  </option>
                ))}
              </select>
              <small style={{ color: "var(--muted)", display: "block", marginTop: 4 }}>Pick a ping or SNMP profile appropriate for this device.</small>
              </label>
              <button type="submit" className="btn-collector" style={{ width: "100%" }}><span className="btn-collector-label">Create Target</span></button>
              <small style={{ color: "var(--muted)", display: "block", marginTop: 4 }}>
                After creation: enqueue a poll to verify connectivity. Use Jobs/Logs for troubleshooting.
              </small>
            </form>
          </Card>
        </div>
      </div>

      <Table
        columns={[
          { key: "id", header: "ID" },
          { key: "deviceId", header: "Device", render: (t: Target) => devices.find((d) => d.id === t.deviceId)?.hostname ?? t.deviceId },
          { key: "profileId", header: "Profile", render: (t: Target) => profiles.find((p) => p.id === t.profileId)?.name ?? t.profileId },
          {
            key: "relation",
            header: "Neighbor",
            render: (t: Target) =>
              relations[t.deviceId] ? <Pill status={relations[t.deviceId]} /> : <span style={{ color: "var(--muted)" }}>—</span>,
          },
          { key: "enabled", header: "Status", render: (t: Target) => <Pill status={t.enabled ? "enabled" : "disabled"} /> },
          {
            key: "actions",
            header: "Actions",
            render: (t: Target) => (
              <div style={{ display: "flex", gap: 8 }}>
                <button className="btn-secondary" onClick={() => toggle(t)}>{t.enabled ? "Disable" : "Enable"}</button>
                <button className="btn-secondary" onClick={() => enqueue(t)}>Enqueue</button>
                <button className="btn-secondary" onClick={() => openHistory(t)}>History</button>
              </div>
            ),
          },
        ]}
        data={filtered}
        empty="No targets yet"
      />

      {historyFor && (
        <Modal title={`History — Target #${historyFor.id}`} onClose={() => setHistoryFor(null)}>
          <ul style={{ margin: 0, paddingLeft: 18, color: "var(--muted)" }}>
            {history.map((h: any) => (
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
