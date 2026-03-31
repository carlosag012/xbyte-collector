import { useEffect, useState } from "react";
import { Card, PageHeader, Pill, Table } from "../components/UI";
import { Modal } from "../components/Modal";

type Profile = {
  id: number;
  kind: "ping" | "snmp";
  name: string;
  intervalSec: number;
  timeoutMs: number;
  retries: number;
  enabled: boolean;
  config: any;
};

export default function ProfilesPage() {
  const [profiles, setProfiles] = useState<Profile[]>([]);
  const [form, setForm] = useState<Partial<Profile>>({ kind: "ping", intervalSec: 300, timeoutMs: 2000, retries: 1, enabled: true });
  const [editing, setEditing] = useState<Profile | null>(null);
  const [history, setHistory] = useState<any[]>([]);
  const [historyFor, setHistoryFor] = useState<Profile | null>(null);
  const [formError, setFormError] = useState<string | null>(null);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const res = await fetch("/api/poll-profiles", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setProfiles(data.profiles ?? []);
    }
  }

  function updateForm(field: string, value: any) {
    setForm((f) => ({ ...f, [field]: value }));
  }

  async function openHistory(p: Profile) {
    const res = await fetch(`/api/audit?entityType=profile&entityId=${p.id}`, { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setHistory(data.events ?? []);
      setHistoryFor(p);
    }
  }

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    if (!form.name) {
      setFormError("Name is required.");
      return;
    }
    if (form.kind === "snmp") {
      const version = form.config?.version ?? "2c";
      if (version === "2c" && !(form.config?.community ?? "").trim()) {
        setFormError("SNMP v2c requires community.");
        return;
      }
      if (version === "3" && !(form.config?.user ?? "").trim()) {
        setFormError("SNMP v3 requires username.");
        return;
      }
    }
    setFormError(null);
    const payload: any = {
      kind: form.kind,
      name: form.name,
      intervalSec: Number(form.intervalSec),
      timeoutMs: Number(form.timeoutMs),
      retries: Number(form.retries),
      enabled: form.enabled,
      config: form.config ?? {},
    };
    let url = "/api/poll-profiles";
    if (editing) {
      payload.id = editing.id;
      url = "/api/poll-profiles/update";
    }
    const res = await fetch(url, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (res.ok) {
      setEditing(null);
      setForm({ kind: "ping", intervalSec: 300, timeoutMs: 2000, retries: 1, enabled: true });
      await load();
    }
  }

  async function toggle(p: Profile) {
    await fetch("/api/poll-profiles/update", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: p.id, enabled: !p.enabled }),
    });
    await load();
  }

  return (
    <div>
      <PageHeader title="Poll Profiles" subtitle="Configure ping and SNMP profiles used by targets." />

      <div className="cards cards-full" style={{ marginBottom: 16 }}>
        <Card title={editing ? "Edit Profile" : "Create Profile"}>
          <div className="form-row">
            <form onSubmit={submit} className="form-panel">
              <div className="form-columns">
                <div className="form-col">
                  <label>
                    <span>Kind</span>
                    <select value={form.kind} onChange={(e) => updateForm("kind", e.target.value as any)}>
                      <option value="ping">Ping</option>
                      <option value="snmp">SNMP</option>
                    </select>
                  </label>
                  <label>
                    <span>Timeout (ms)</span>
                    <input type="number" value={form.timeoutMs ?? 2000} onChange={(e) => updateForm("timeoutMs", Number(e.target.value))} />
                  </label>
                </div>
                <div className="form-col">
                  <label>
                    <span>Name</span>
                    <input value={form.name ?? ""} onChange={(e) => updateForm("name", e.target.value)} required />
                  </label>
                  <label>
                    <span>Retries</span>
                    <input type="number" value={form.retries ?? 1} onChange={(e) => updateForm("retries", Number(e.target.value))} />
                  </label>
                </div>
                <div className="form-col" style={{ gridColumn: "1 / -1" }}>
                  <label>
                    <span>Interval (sec)</span>
                    <input type="number" value={form.intervalSec ?? 300} onChange={(e) => updateForm("intervalSec", Number(e.target.value))} />
                  </label>
                </div>
              </div>
            {form.kind === "snmp" && (
              <>
                <label>
                  <span>Version</span>
                  <select value={form.config?.version ?? "2c"} onChange={(e) => updateForm("config", { ...(form.config ?? {}), version: e.target.value })}>
                    <option value="2c">2c</option>
                    <option value="3">3</option>
                  </select>
                </label>
                {(form.config?.version ?? "2c") === "2c" ? (
                  <label>
                    <span>Community</span>
                    <input
                      value={form.config?.community ?? ""}
                      onChange={(e) => updateForm("config", { ...(form.config ?? {}), community: e.target.value })}
                    />
                    <small style={{ color: "var(--muted)" }}>Required for SNMP v2c</small>
                  </label>
                ) : (
                  <>
                    <label>
                      <span>Username</span>
                      <input
                        value={form.config?.user ?? ""}
                        onChange={(e) => updateForm("config", { ...(form.config ?? {}), user: e.target.value })}
                      />
                      <small style={{ color: "var(--muted)" }}>Required for SNMP v3</small>
                    </label>
                    <label>
                      <span>Auth Protocol</span>
                      <input
                        value={form.config?.authProtocol ?? ""}
                        onChange={(e) => updateForm("config", { ...(form.config ?? {}), authProtocol: e.target.value })}
                      />
                    </label>
                    <label>
                      <span>Auth Password</span>
                      <input
                        type="password"
                        value={form.config?.authPassword ?? ""}
                        onChange={(e) => updateForm("config", { ...(form.config ?? {}), authPassword: e.target.value })}
                      />
                    </label>
                    <label>
                      <span>Priv Protocol</span>
                      <input
                        value={form.config?.privProtocol ?? ""}
                        onChange={(e) => updateForm("config", { ...(form.config ?? {}), privProtocol: e.target.value })}
                      />
                    </label>
                    <label>
                      <span>Priv Password</span>
                      <input
                        type="password"
                        value={form.config?.privPassword ?? ""}
                        onChange={(e) => updateForm("config", { ...(form.config ?? {}), privPassword: e.target.value })}
                      />
                    </label>
                  </>
                )}
              </>
            )}
              <div className="form-actions" style={{ display: "flex", gap: 8, flexDirection: "column", maxWidth: 420 }}>
                <button type="submit" className="btn-collector">
                  <span className="btn-collector-label">{editing ? "Update" : "Create Profile"}</span>
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
              <strong>About profiles</strong>
              <p style={{ marginTop: 6 }}>
                Profiles define how a device is polled. Use Ping for basic reachability. Use SNMP when you need system/interface/LLDP data; provide the proper
                community or v3 credentials and security level your devices expect.
              </p>
              <p style={{ marginTop: 6 }}>
                After creating a profile, attach it to a device as a Target and enqueue a poll. If you see auth errors, recheck communities/users/auth/priv
                settings here.
              </p>
            </div>
          </div>
        </Card>
      </div>

      <Table
        columns={[
          { key: "name", header: "Name" },
          { key: "kind", header: "Kind" },
          { key: "intervalSec", header: "Interval" },
          { key: "timeoutMs", header: "Timeout" },
          { key: "enabled", header: "Status", render: (p: Profile) => <Pill status={p.enabled ? "enabled" : "disabled"} /> },
          {
            key: "actions",
            header: "Actions",
            render: (p: Profile) => (
              <div style={{ display: "flex", gap: 8 }}>
                <button className="btn-secondary" onClick={() => setEditing(p)}>Edit</button>
                <button className="btn-secondary" onClick={() => toggle(p)}>{p.enabled ? "Disable" : "Enable"}</button>
                <button className="btn-secondary" onClick={() => openHistory(p)}>History</button>
              </div>
            ),
          },
        ]}
        data={profiles}
        empty="No profiles yet"
      />

      {historyFor && (
        <Modal title={`History — ${historyFor.name}`} onClose={() => setHistoryFor(null)}>
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
