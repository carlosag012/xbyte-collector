import { useEffect, useMemo, useState } from "react";
import { Card, PageHeader, Pill, Table } from "../components/UI";

export default function ServicesPage() {
  const [workers, setWorkers] = useState<any[]>([]);
  const [summary, setSummary] = useState<any[]>([]);
  const [exec, setExec] = useState<any[]>([]);
  const [restartMsg, setRestartMsg] = useState<string | null>(null);
  const [services, setServices] = useState<any | null>(null);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const res = await fetch("/api/system/services", { credentials: "include" });
    if (!res.ok) return;
    const data = await res.json();
    setWorkers(data.workers ?? []);
    setSummary(data.summary ?? []);
    setExec(data.execution ?? []);
    setServices(data.service ?? null);
  }

  async function requestRestart() {
    const res = await fetch("/api/system/service/restart", { method: "POST", credentials: "include" });
    const data = await res.json().catch(() => ({}));
    if (res.ok && data.ok) {
      setRestartMsg("Restart triggered.");
    } else {
      setRestartMsg(data.error ?? "Restart not available from UI.");
    }
  }

  const workersWithStatus = useMemo(() => {
    const now = Date.now();
    return workers.map((w) => {
      const hb = w.lastHeartbeatAt ? Date.parse(w.lastHeartbeatAt) : 0;
      const age = hb ? (now - hb) / 1000 : Infinity;
      const status = age < 60 ? "healthy" : age < 180 ? "stale" : "down";
      return { ...w, status };
    });
  }, [workers]);

  return (
    <div>
      <PageHeader title="Services" subtitle="Supervisor and worker service status." action={<button onClick={load}>Refresh</button>} />

      <div className="cards" style={{ marginBottom: 16 }}>
        {services && (
          <Card title="Backend Service">
            <p style={{ margin: 0, color: "var(--muted)" }}>
              Status: <Pill status={services.status ?? "unknown"} />
            </p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Pid: {services.pid ?? "—"}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Uptime: {services.uptimeSeconds ?? "—"}s</p>
          </Card>
        )}
        {summary.map((s: any) => (
          <Card key={s.workerType} title={`${s.workerType.toUpperCase()} services`}>
            <p style={{ margin: 0, color: "var(--muted)" }}>Workers: {s.workers}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Enabled: {s.enabledWorkers}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Running jobs: {s.runningJobs}</p>
          </Card>
        ))}
        {exec.map((s: any) => (
          <Card key={`exec-${s.workerType}`} title={`${s.workerType.toUpperCase()} execution`}>
            <p style={{ margin: 0, color: "var(--muted)" }}>Finished: {s.totalFinishedJobs}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>
              Success: {s.successfulJobs} | Failed: {s.failedJobs}
            </p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Last: {s.lastProcessedAt ?? "—"}</p>
          </Card>
        ))}
      </div>

      <Table
        columns={[
          { key: "workerName", header: "Name" },
          { key: "workerType", header: "Type" },
          { key: "status", header: "Status", render: (w: any) => <Pill status={w.status} /> },
          { key: "lastHeartbeatAt", header: "Last Heartbeat" },
          { key: "enabled", header: "Enabled", render: (w: any) => (w.enabled ? "Yes" : "No") },
        ]}
        data={workersWithStatus}
        empty="No services registered"
      />

      <Card title="Restart">
        <p style={{ margin: 0, color: "var(--muted)" }}>
          Restarting services from UI may be unavailable on this appliance build. Use system tools (systemctl/service) if required. Status and logs remain available here.
        </p>
        <button style={{ marginTop: 10, opacity: 0.7 }} onClick={requestRestart}>
          Attempt restart
        </button>
        {restartMsg && <p style={{ marginTop: 6, color: "var(--muted)" }}>{restartMsg}</p>}
        <p style={{ marginTop: 8, color: "var(--muted)" }}>
          Need more detail? Check <a href="/logs" style={{ color: "var(--accent)" }}>Logs</a> or <a href="/system" style={{ color: "var(--accent)" }}>System</a>.
        </p>
      </Card>
    </div>
  );
}
