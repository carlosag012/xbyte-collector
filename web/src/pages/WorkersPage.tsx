import { useEffect, useMemo, useState } from "react";
import { Card, PageHeader, Pill, Table } from "../components/UI";

type Worker = {
  id: number;
  workerType: string;
  workerName: string;
  enabled: boolean;
  lastHeartbeatAt: string;
};

type SummaryByType = {
  workerType: "ping" | "snmp";
  workers: number;
  enabledWorkers: number;
  disabledWorkers: number;
  runningJobs: number;
  completedJobs: number;
  failedJobs: number;
};

export default function WorkersPage() {
  const [workers, setWorkers] = useState<Worker[]>([]);
  const [summary, setSummary] = useState<SummaryByType[]>([]);
  const [execSummary, setExecSummary] = useState<any[]>([]);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const [w, s, e] = await Promise.all([
      fetch("/api/workers", { credentials: "include" }),
      fetch("/api/workers/metrics-summary-by-type", { credentials: "include" }),
      fetch("/api/workers/execution-summary", { credentials: "include" }),
    ]);
    if (w.ok) setWorkers((await w.json()).workers ?? []);
    if (s.ok) setSummary((await s.json()).summary?.types ?? []);
    if (e.ok) setExecSummary((await e.json()).summary?.types ?? []);
  }

  const now = Date.now();
  const workersWithStatus = useMemo(
    () =>
      workers.map((w) => {
        const hb = w.lastHeartbeatAt ? Date.parse(w.lastHeartbeatAt) : 0;
        const age = hb ? (now - hb) / 1000 : Infinity;
        const status = age < 60 ? "healthy" : age < 180 ? "stale" : "down";
        return { ...w, status, age };
      }),
    [workers, now]
  );

  return (
    <div>
      <PageHeader title="Workers & Services" subtitle="Monitor poller processes and recent execution outcomes." action={<button onClick={load}>Refresh</button>} />

      <div className="cards" style={{ marginBottom: 16 }}>
        {summary.map((s) => (
          <Card key={s.workerType} title={`${s.workerType.toUpperCase()} workers`}>
            <p style={{ margin: 0, color: "var(--muted)" }}>Total: {s.workers}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Enabled: {s.enabledWorkers}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Running jobs: {s.runningJobs}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Completed: {s.completedJobs} / Failed: {s.failedJobs}</p>
          </Card>
        ))}
        {execSummary.map((s) => (
          <Card key={`exec-${s.workerType}`} title={`${s.workerType.toUpperCase()} execution`}>
            <p style={{ margin: 0, color: "var(--muted)" }}>Finished: {s.totalFinishedJobs}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Success: {s.successfulJobs} | Failed: {s.failedJobs}</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Avg latency: {s.avgLatencyMs ?? "—"} ms</p>
            <p style={{ margin: 0, color: "var(--muted)" }}>Last processed: {s.lastProcessedAt ?? "—"}</p>
          </Card>
        ))}
      </div>

      <Table
        columns={[
          { key: "workerName", header: "Name" },
          { key: "workerType", header: "Type" },
          { key: "status", header: "Status", render: (w: any) => <Pill status={w.status} /> },
          { key: "lastHeartbeatAt", header: "Last Heartbeat" },
          { key: "enabled", header: "Enabled", render: (w: Worker) => (w.enabled ? "Yes" : "No") },
        ]}
        data={workersWithStatus}
        empty="No workers registered"
      />
    </div>
  );
}
