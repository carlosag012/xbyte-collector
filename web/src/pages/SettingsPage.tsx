import { useEffect, useState } from "react";
import { Card, PageHeader } from "../components/UI";

type Status = {
  bootstrap?: { configured: boolean; status: string };
  cloud?: { enabled?: boolean; status?: string };
};
type Config = {
  applianceName?: string;
  companyName?: string;
  orgId?: string;
  cloudEnabled?: boolean;
};

export default function SettingsPage({ status: initialStatus, config: initialConfig, onReload }: { status: Status | null; config: Config | null; onReload: () => Promise<void> }) {
  const [config, setConfig] = useState<Config | null>(initialConfig);
  const [status, setStatus] = useState<Status | null>(initialStatus);
  const [draft, setDraft] = useState<Config>(initialConfig ?? {});
  const [message, setMessage] = useState<string | null>(null);

  useEffect(() => {
    setConfig(initialConfig);
    setStatus(initialStatus);
    setDraft(initialConfig ?? {});
  }, [initialConfig, initialStatus]);

  async function saveConfig(e: React.FormEvent) {
    e.preventDefault();
    setMessage(null);
    await fetch("/api/config", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(draft),
    });
    await onReload();
    setMessage("Settings saved");
  }

  async function markBootstrap() {
    setMessage(null);
    await fetch("/api/bootstrap/mark-configured", { method: "POST", credentials: "include" });
    await onReload();
    setMessage("Bootstrap marked configured");
  }

  async function reloadStatus() {
    const res = await fetch("/api/status", { credentials: "include" });
    if (res.ok) setStatus((await res.json()) ?? null);
  }

  return (
    <div>
      <PageHeader title="Settings" subtitle="Appliance identity, bootstrap, and cloud sync." action={<button onClick={onReload}>Refresh</button>} />

      <div className="cards" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))" }}>
        <Card title="Appliance Identity">
          <form onSubmit={saveConfig} className="form-grid">
            <label>
              <span>Appliance Name</span>
              <input value={draft.applianceName ?? ""} onChange={(e) => setDraft({ ...draft, applianceName: e.target.value })} />
            </label>
            <label>
              <span>Company</span>
              <input value={draft.companyName ?? ""} onChange={(e) => setDraft({ ...draft, companyName: e.target.value })} />
            </label>
            <label>
              <span>Org ID</span>
              <input value={draft.orgId ?? ""} onChange={(e) => setDraft({ ...draft, orgId: e.target.value })} />
            </label>
            <label>
              <span>Cloud Enabled</span>
              <select value={draft.cloudEnabled ? "true" : "false"} onChange={(e) => setDraft({ ...draft, cloudEnabled: e.target.value === "true" })}>
                <option value="false">Disabled</option>
                <option value="true">Enabled</option>
              </select>
            </label>
            <button type="submit">Save</button>
          </form>
        </Card>
        <Card title="Collector Behavior">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Session TTL: managed server-side. Worker counts and timings are environment-driven.
          </p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Ping workers: env PING_WORKER_COUNT, SNMP workers: env SNMP_WORKER_COUNT.</p>
        </Card>
        <Card title="Cloud Sync">
          <p style={{ margin: 0, color: "var(--muted)" }}>Status: {status?.cloud?.status ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Enabled: {String(status?.cloud?.enabled ?? false)}</p>
          <button onClick={reloadStatus} style={{ marginTop: 10 }}>
            Reload status
          </button>
        </Card>
        <Card title="Bootstrap">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Configured: {String(status?.bootstrap?.configured ?? false)} (state: {status?.bootstrap?.status ?? "unknown"})
          </p>
          <button onClick={markBootstrap} style={{ marginTop: 10 }}>
            Mark Configured
          </button>
        </Card>
      </div>
      {message && <p style={{ color: "#a3e635", marginTop: 10 }}>{message}</p>}
    </div>
  );
}
