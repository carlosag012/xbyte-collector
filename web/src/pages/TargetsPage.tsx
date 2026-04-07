import { useEffect, useMemo, useState } from "react";
import { Card, PageHeader, Pill, Table, useToast } from "../components/UI";
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
  const [suppressedInfo, setSuppressedInfo] = useState<Record<number, { note?: string | null; createdAt?: string }>>({});
  const toast = useToast();

  useEffect(() => {
    loadAll();
  }, []);

  async function loadAll() {
    const [d, p, t, n, a] = await Promise.all([
      fetch("/api/devices", { credentials: "include" }),
      fetch("/api/poll-profiles", { credentials: "include" }),
      fetch("/api/poll-targets", { credentials: "include" }),
      fetch("/api/neighbors", { credentials: "include" }),
      fetch("/api/audit?entityType=target&action=auto_suppress&limit=500", { credentials: "include" }),
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
    if (a.ok) {
      const data = await a.json();
      const map: Record<number, { note?: string | null; createdAt?: string }> = {};
      (data.events ?? []).forEach((ev: any) => {
        const id = ev?.entityId;
        if (!id) return;
        if (!map[id]) map[id] = { note: ev.note ?? null, createdAt: ev.createdAt };
      });
      setSuppressedInfo(map);
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
      toast({ message: "Target created", tone: "success" });
    } else {
      const data = await res.json().catch(() => ({}));
      toast({ message: data.message || data.error || "Create target failed", tone: "error" });
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
    const res = await fetch("/api/poll-jobs/enqueue", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ targetId: target.id }),
    });
    if (res.ok) {
      toast({ message: "Enqueued poll job", tone: "success" });
    } else {
      const data = await res.json().catch(() => ({}));
      const code = data.code || data.error;
      if (code && typeof code === "string" && code.toLowerCase().includes("license")) {
        toast({ message: "Enqueue blocked: active xByte license and paid subscription required.", tone: "error" });
      } else {
        toast({ message: data.message || data.error || "Failed to enqueue", tone: "error" });
      }
    }
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

      <div className="cards cards-full" style={{ marginBottom: 16 }}>
        <Card title="Create Target">
          <div className="form-row">
            <form onSubmit={createTarget} className="form-panel">
              <div className="form-fields form-fields-single">
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
                </label>
                <div className="form-actions" style={{ display: "flex", gap: 8, flexDirection: "column", maxWidth: 420 }}>
                  <button type="submit" className="btn-collector">
                    <span className="btn-collector-label">Create Target</span>
                  </button>
                </div>
              </div>
            </form>
            <div className="about-panel">
              <strong>About targets</strong>
              <p style={{ marginTop: 6 }}>
                Targets connect a device to a poll profile. Enable a target when you are ready to collect, then enqueue a poll to validate credentials and
                reachability. You can keep multiple targets per device to test different profiles.
              </p>
              <p style={{ marginTop: 6 }}>
                If a poll fails, check Jobs and Logs for auth/timeout details. Adjust the profile or device IP, then retry enqueue to confirm recovery.
              </p>
            </div>
          </div>
        </Card>
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
          {
            key: "enabled",
            header: "Status",
            render: (t: Target) => (
              <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <Pill status={t.enabled ? "enabled" : "disabled"} label={t.enabled ? "Enabled" : "Suppressed"} />
                {!t.enabled && suppressedInfo[t.id] && (
                  <div style={{ color: "var(--muted)", fontSize: 12, lineHeight: 1.3 }}>
                    {suppressedInfo[t.id].note || "Auto-suppressed"} · {suppressedInfo[t.id].createdAt || ""}
                  </div>
                )}
              </div>
            ),
          },
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
