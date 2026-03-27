import { useEffect, useMemo, useState } from "react";
import { PageHeader, Table, Pill } from "../components/UI";
import { Modal } from "../components/Modal";
import { useToast } from "../components/UI";

type Device = { id: number; hostname: string };
type Neighbor = {
  id: number;
  deviceId: number;
  localPort?: string | null;
  remoteSysName?: string | null;
  remotePortId?: string | null;
  remoteChassisId?: string | null;
  remoteMgmtIp?: string | null;
  reviewStatus?: string;
  linkedDeviceId?: number | null;
  promotedDeviceId?: number | null;
  reviewNote?: string | null;
};

export default function NeighborsPage() {
  const [neighbors, setNeighbors] = useState<Neighbor[]>([]);
  const [devices, setDevices] = useState<Device[]>([]);
  const [deviceFilter, setDeviceFilter] = useState<number | null>(null);
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [search, setSearch] = useState("");
  const [promoteNeighbor, setPromoteNeighbor] = useState<Neighbor | null>(null);
  const [promoteForm, setPromoteForm] = useState({ hostname: "", ipAddress: "", note: "" });
  const [linkNeighbor, setLinkNeighbor] = useState<Neighbor | null>(null);
  const [linkDeviceId, setLinkDeviceId] = useState<number | "">("");
  const [ignoreNeighbor, setIgnoreNeighbor] = useState<Neighbor | null>(null);
  const [ignoreNote, setIgnoreNote] = useState("");
  const toast = useToast();

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const [nRes, dRes] = await Promise.all([
      fetch("/api/neighbors", { credentials: "include" }),
      fetch("/api/devices", { credentials: "include" }),
    ]);
    if (nRes.ok) setNeighbors((await nRes.json()).neighbors ?? []);
    if (dRes.ok) setDevices((await dRes.json()).devices ?? []);
  }

  const filtered = useMemo(() => {
    return neighbors.filter((n) => {
      if (deviceFilter && n.deviceId !== deviceFilter) return false;
      if (statusFilter && (n.reviewStatus ?? "new") !== statusFilter) return false;
      const term = search.toLowerCase().trim();
      if (!term) return true;
      return (
        (n.remoteSysName ?? "").toLowerCase().includes(term) ||
        (n.remotePortId ?? "").toLowerCase().includes(term) ||
        (n.remoteMgmtIp ?? "").toLowerCase().includes(term) ||
        (n.remoteChassisId ?? "").toLowerCase().includes(term)
      );
    });
  }, [neighbors, deviceFilter, statusFilter, search]);

  function statusChip(s?: string) {
    const state = s ?? "new";
    return <Pill status={state} />;
  }

  function openPromote(n: Neighbor) {
    setPromoteNeighbor(n);
    setPromoteForm({
      hostname: n.remoteSysName ?? "",
      ipAddress: n.remoteMgmtIp ?? "",
      note: "",
    });
  }

  async function submitPromote(e: React.FormEvent) {
    e.preventDefault();
    if (!promoteNeighbor) return;
    const res = await fetch("/api/neighbors/promote", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        neighborId: promoteNeighbor.id,
        hostname: promoteForm.hostname,
        ipAddress: promoteForm.ipAddress,
        note: promoteForm.note || undefined,
      }),
    });
    if (res.ok) {
      toast({ message: "Neighbor promoted to device", tone: "success" });
      setPromoteNeighbor(null);
      await load();
    } else {
      const data = await res.json().catch(() => ({}));
      toast({ message: data.message || data.error || "Promote failed", tone: "error" });
    }
  }

  async function submitLink(e: React.FormEvent) {
    e.preventDefault();
    if (!linkNeighbor || !linkDeviceId) return;
    const res = await fetch("/api/neighbors/link", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ neighborId: linkNeighbor.id, deviceId: linkDeviceId }),
    });
    if (res.ok) {
      toast({ message: "Neighbor linked to device", tone: "success" });
      setLinkNeighbor(null);
      setLinkDeviceId("");
      await load();
    } else {
      const data = await res.json().catch(() => ({}));
      toast({ message: data.message || data.error || "Link failed", tone: "error" });
    }
  }

  async function submitIgnore(e: React.FormEvent) {
    e.preventDefault();
    if (!ignoreNeighbor) return;
    const res = await fetch("/api/neighbors/ignore", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ neighborId: ignoreNeighbor.id, note: ignoreNote || undefined }),
    });
    if (res.ok) {
      toast({ message: "Neighbor ignored", tone: "warning" });
      setIgnoreNeighbor(null);
      setIgnoreNote("");
      await load();
    } else {
      const data = await res.json().catch(() => ({}));
      toast({ message: data.message || data.error || "Ignore failed", tone: "error" });
    }
  }

  async function unignore(n: Neighbor) {
    const res = await fetch("/api/neighbors/unignore", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ neighborId: n.id }),
    });
    if (res.ok) {
      toast({ message: "Neighbor unignored", tone: "success" });
      await load();
    } else {
      const data = await res.json().catch(() => ({}));
      toast({ message: data.message || data.error || "Unignore failed", tone: "error" });
    }
  }

  return (
    <div>
      <PageHeader
        title="Neighbors"
        subtitle="LLDP discovery across managed devices. Promote, link, or ignore."
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
            <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
              <option value="">All states</option>
              <option value="new">Unreviewed</option>
              <option value="promoted">Promoted</option>
              <option value="linked">Linked</option>
              <option value="ignored">Ignored</option>
            </select>
            <input placeholder="Search neighbors" value={search} onChange={(e) => setSearch(e.target.value)} />
          </div>
        }
      />

      <Table
        columns={[
          { key: "device", header: "Local Device", render: (n: Neighbor) => devices.find((d) => d.id === n.deviceId)?.hostname ?? n.deviceId },
          { key: "localPort", header: "Local Port" },
          { key: "remoteSysName", header: "Remote Sys" },
          { key: "remotePortId", header: "Remote Port" },
          { key: "remoteMgmtIp", header: "Mgmt IP" },
          { key: "remoteChassisId", header: "Chassis" },
          { key: "reviewStatus", header: "State", render: (n: Neighbor) => statusChip(n.reviewStatus) },
          {
            key: "actions",
            header: "Actions",
            render: (n: Neighbor) => (
              <div style={{ display: "flex", gap: 6, flexWrap: "nowrap", whiteSpace: "nowrap", alignItems: "center", overflowX: "auto" }}>
                <button className="btn-secondary" onClick={() => openPromote(n)}>Promote</button>
                <button className="btn-secondary" onClick={() => setLinkNeighbor(n)}>Link</button>
                {(n.promotedDeviceId || n.linkedDeviceId) && (
                  <span style={{ color: "var(--muted)", fontSize: 12 }}>
                    {n.promotedDeviceId ? `→ device ${n.promotedDeviceId}` : `→ device ${n.linkedDeviceId}`}
                  </span>
                )}
                {n.reviewStatus === "ignored" ? (
                  <button className="btn-secondary" onClick={() => unignore(n)}>Unignore</button>
                ) : (
                  <button className="btn-secondary" onClick={() => setIgnoreNeighbor(n)}>Ignore</button>
                )}
              </div>
            ),
          },
        ]}
        data={filtered}
        empty="No neighbors discovered yet"
      />

      {promoteNeighbor && (
        <Modal title="Promote neighbor to device" onClose={() => setPromoteNeighbor(null)}>
          <form onSubmit={submitPromote} className="form-grid">
            <label>
              <span>Hostname</span>
              <input value={promoteForm.hostname} onChange={(e) => setPromoteForm({ ...promoteForm, hostname: e.target.value })} required />
            </label>
            <label>
              <span>IP Address</span>
              <input value={promoteForm.ipAddress} onChange={(e) => setPromoteForm({ ...promoteForm, ipAddress: e.target.value })} required />
            </label>
            <label>
              <span>Note</span>
              <input value={promoteForm.note} onChange={(e) => setPromoteForm({ ...promoteForm, note: e.target.value })} />
            </label>
            <button type="submit">Create device</button>
          </form>
        </Modal>
      )}

      {linkNeighbor && (
        <Modal title="Link neighbor to existing device" onClose={() => setLinkNeighbor(null)}>
          <form onSubmit={submitLink} className="form-grid">
            <label>
              <span>Device</span>
              <select value={linkDeviceId} onChange={(e) => setLinkDeviceId(e.target.value ? Number(e.target.value) : "")} required>
                <option value="">Select device</option>
                {devices.map((d) => (
                  <option key={d.id} value={d.id}>
                    {d.hostname}
                  </option>
                ))}
              </select>
            </label>
            <button type="submit" disabled={!linkDeviceId}>
              Link device
            </button>
          </form>
        </Modal>
      )}

      {ignoreNeighbor && (
        <Modal title="Ignore neighbor" onClose={() => setIgnoreNeighbor(null)}>
          <form onSubmit={submitIgnore} className="form-grid">
            <p style={{ margin: 0, color: "var(--muted)" }}>This neighbor will be marked ignored but can be unignored later.</p>
            <label>
              <span>Reason (optional)</span>
              <input value={ignoreNote} onChange={(e) => setIgnoreNote(e.target.value)} />
            </label>
            <button type="submit">Ignore</button>
          </form>
        </Modal>
      )}
    </div>
  );
}
