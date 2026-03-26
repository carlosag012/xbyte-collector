import { useEffect, useState } from "react";
import { Card, PageHeader, Pill, Table } from "../components/UI";

type Health = {
  db?: string;
  bootstrap?: any;
  cloud?: any;
  workers?: any;
};

type About = {
  version?: { version?: string; gitCommit?: string; builtAt?: string };
  serverTime?: string;
  db?: string;
};

export default function SystemPage() {
  const [health, setHealth] = useState<Health | null>(null);
  const [about, setAbout] = useState<About | null>(null);
  const [runtime, setRuntime] = useState<any | null>(null);
  const [storage, setStorage] = useState<any | null>(null);
  const [activity, setActivity] = useState<any | null>(null);
  const [license, setLicense] = useState<any | null>(null);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const [h, a, r, s, act, lic] = await Promise.all([
      fetch("/api/system/health", { credentials: "include" }),
      fetch("/api/system/about", { credentials: "include" }),
      fetch("/api/system/runtime", { credentials: "include" }),
      fetch("/api/system/storage", { credentials: "include" }),
      fetch("/api/system/recent-activity", { credentials: "include" }),
      fetch("/api/licensing/status", { credentials: "include" }),
    ]);
    if (h.ok) setHealth((await h.json()).health ?? null);
    if (a.ok) setAbout((await a.json()).about ?? null);
    if (r.ok) setRuntime((await r.json()).runtime ?? null);
    if (s.ok) setStorage((await s.json()).storage ?? null);
    if (act.ok) setActivity(await act.json());
    if (lic.ok) setLicense((await lic.json()).licensing ?? null);
  }

  return (
    <div>
      <PageHeader title="System" subtitle="Appliance runtime status, version, and health." action={<button onClick={load}>Refresh</button>} />

      <div className="cards">
        <Card title="Health">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            DB: <Pill status={health?.db ?? "unknown"} />
          </p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Bootstrap: {health?.bootstrap?.configured ? "configured" : "not configured"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Cloud: {health?.cloud?.enabled ? "enabled" : "disabled"}</p>
        </Card>
        <Card title="Version">
          <p style={{ margin: 0, color: "var(--muted)" }}>Version: {about?.version?.version ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Commit: {about?.version?.gitCommit ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Built: {about?.version?.builtAt ?? "unknown"}</p>
        </Card>
        <Card title="Runtime">
          <p style={{ margin: 0, color: "var(--muted)" }}>Server time: {about?.serverTime ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Workers registered: {health?.workers?.registered ?? "—"}</p>
          {runtime && (
            <>
              <p style={{ margin: 0, color: "var(--muted)" }}>Host: {runtime.hostname}</p>
              <p style={{ margin: 0, color: "var(--muted)" }}>
                {runtime.platform} / {runtime.arch} / Node {runtime.nodeVersion}
              </p>
              <p style={{ margin: 0, color: "var(--muted)" }}>Uptime: {runtime.uptimeSeconds}s</p>
            </>
          )}
        </Card>
        <Card title="Licensing">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            State: <Pill status={license?.effectiveStatus ?? "unknown"} />
          </p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Reason: {license?.reason ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Expires: {license?.expiresAt ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Grace until: {license?.graceUntil ?? "—"}</p>
        </Card>
      </div>

      <div className="split" style={{ marginTop: 16 }}>
        <Card title="Storage">
          <p style={{ margin: 0, color: "var(--muted)" }}>DB Path: {storage?.dbPath ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>DB Size: {storage?.dbSizeBytes ?? "—"} bytes</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Log Path: {storage?.logPath ?? "—"}</p>
        </Card>
        <Card title="Recent Activity">
          {activity?.audits?.length ? (
            <Table
              columns={[
                { key: "createdAt", header: "Time" },
                { key: "action", header: "Action", render: (e: any) => <Pill status={e.action} /> },
                {
                  key: "entity",
                  header: "Entity",
                  render: (e: any) => {
                    const link =
                      e.entityType === "poll_job"
                        ? "/jobs"
                        : e.entityType === "device"
                        ? "/devices"
                        : e.entityType === "backup"
                        ? "/backups"
                        : e.entityType === "license"
                        ? "/licensing"
                        : null;
                    const label = `${e.entityType}${e.entityId ? ` #${e.entityId}` : ""}`;
                    return link ? (
                      <a href={link} style={{ color: "var(--accent)" }}>
                        {label}
                      </a>
                    ) : (
                      label
                    );
                  },
                },
              ]}
              data={activity.audits.slice(0, 5)}
              empty="No recent audits"
            />
          ) : (
            <p style={{ margin: 0, color: "var(--muted)" }}>No recent activity.</p>
          )}
        </Card>
      </div>
    </div>
  );
}
