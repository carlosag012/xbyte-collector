import { useEffect, useMemo, useState } from "react";
import { PageHeader, Table, Pill } from "../components/UI";

type LogEntry = { ts: string; line: string };

export default function LogsPage() {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [search, setSearch] = useState("");

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const res = await fetch("/api/system/logs", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setLogs(data.logs ?? []);
    }
  }

  const filtered = useMemo(() => {
    const term = search.toLowerCase().trim();
    if (!term) return logs;
    return logs.filter((l) => l.line.toLowerCase().includes(term) || (l.ts ?? "").toLowerCase().includes(term));
  }, [logs, search]);

  const parsed = useMemo(() => {
    return filtered.map((l) => {
      try {
        const obj = JSON.parse(l.line);
        const msg = obj.msg ?? obj.message ?? l.line;
        return {
          ...l,
          level: obj.level ?? obj.status ?? "info",
          component: obj.workerType ?? obj.component ?? obj.workerName ?? "system",
          message: typeof msg === "string" ? msg : l.line,
          jobId: obj.jobId ?? obj?.context?.jobId,
          targetId: obj?.context?.targetId,
          reason: obj.reason ?? obj.error ?? obj.code,
        };
      } catch {
        return { ...l, level: l.line.toLowerCase().includes("error") ? "error" : "info", component: "text", message: l.line };
      }
    });
  }, [filtered]);

  return (
    <div>
      <PageHeader
        title="Logs & Events"
        subtitle="Recent collector activity and poller output."
        action={
          <div style={{ display: "flex", gap: 8 }}>
            <input placeholder="Search" value={search} onChange={(e) => setSearch(e.target.value)} />
            <button onClick={load}>Refresh</button>
          </div>
        }
      />
      <Table
        columns={[
          { key: "ts", header: "Timestamp" },
          { key: "level", header: "Level", render: (r: any) => <Pill status={r.level} /> },
          { key: "component", header: "Source" },
          {
            key: "message",
            header: "Message",
            render: (r: any) => (
              <div>
                <div>{r.message}</div>
                <div style={{ fontSize: 12, color: "var(--muted)" }}>
                  {r.jobId && (
                    <a href="/jobs" style={{ color: "var(--accent)", marginRight: 8 }}>
                      Job #{r.jobId}
                    </a>
                  )}
                  {r.reason && r.reason.toString().includes("license") && (
                    <a href="/licensing" style={{ color: "var(--accent)" }}>
                      Licensing
                    </a>
                  )}
                </div>
              </div>
            ),
          },
        ]}
        data={parsed}
        empty="No logs available"
      />
    </div>
  );
}
