import { useEffect, useState } from "react";
import { Card, PageHeader } from "../components/UI";

type About = { version?: any; serverTime?: string; db?: string; bootstrap?: any };

export default function AboutPage() {
  const [about, setAbout] = useState<About | null>(null);

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const res = await fetch("/api/system/about", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setAbout(data.about ?? null);
    }
  }

  return (
    <div>
      <PageHeader
        title="About"
        subtitle="Version, build, and appliance identity for this xByte collector appliance."
        action={<button onClick={load}>Refresh</button>}
      />
      <div className="cards" style={{ gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))" }}>
        <Card title="Version">
          <p style={{ margin: 0, color: "var(--muted)" }}>Version: {about?.version?.version ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Commit: {about?.version?.gitCommit ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Built: {about?.version?.builtAt ?? "unknown"}</p>
        </Card>
        <Card title="Runtime">
          <p style={{ margin: 0, color: "var(--muted)" }}>Server time: {about?.serverTime ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>DB: {about?.db ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Bootstrap: {JSON.stringify(about?.bootstrap ?? {})}</p>
        </Card>
      </div>
    </div>
  );
}
