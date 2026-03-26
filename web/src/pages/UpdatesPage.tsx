import { useEffect, useState } from "react";
import { Card, PageHeader } from "../components/UI";

export default function UpdatesPage() {
  const [about, setAbout] = useState<any>(null);
  const [runtime, setRuntime] = useState<any>(null);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const [a, r] = await Promise.all([
      fetch("/api/system/about", { credentials: "include" }),
      fetch("/api/system/runtime", { credentials: "include" }),
    ]);
    if (a.ok) setAbout((await a.json()).about ?? null);
    if (r.ok) setRuntime((await r.json()).runtime ?? null);
  }

  return (
    <div>
      <PageHeader
        title="Updates"
        subtitle="Current appliance version and the controlled update posture."
        action={<button onClick={load}>Refresh</button>}
      />
      <div className="cards">
        <Card title="Current Version">
          <p style={{ margin: 0, color: "var(--muted)" }}>Version: {about?.version?.version ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Commit: {about?.version?.gitCommit ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Built: {about?.version?.builtAt ?? "unknown"}</p>
          {runtime && <p style={{ margin: 0, color: "var(--muted)" }}>Host: {runtime.hostname}</p>}
        </Card>
        <Card title="Update Guidance">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Updates are applied today via OS package install (e.g., deb/rpm) outside the UI. UI-based orchestration is not enabled yet.
          </p>
          <ul style={{ margin: "6px 0", paddingLeft: 18, color: "var(--muted)" }}>
            <li>Before updating: export config & inventory (<a href="/backups" style={{ color: "var(--accent)" }}>Backups</a>).</li>
            <li>Verify licensing is active to avoid post-update lockout (<a href="/licensing" style={{ color: "var(--accent)" }}>Licensing</a>).</li>
            <li>After updating: check <a href="/system" style={{ color: "var(--accent)" }}>System</a>, <a href="/services" style={{ color: "var(--accent)" }}>Services</a>, and <a href="/logs" style={{ color: "var(--accent)" }}>Logs</a>.</li>
          </ul>
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Future: in-app package channel selection, staged rollout, and rollback guidance.
          </p>
        </Card>
      </div>
    </div>
  );
}
