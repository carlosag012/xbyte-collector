import { useMemo } from "react";

type Status = {
  bootstrap?: { configured: boolean; status: string };
  cloud?: { enabled?: boolean; status?: string };
  workers?: { registered?: number };
};
type Config = {
  applianceName?: string;
  companyName?: string;
  orgId?: string;
  cloudEnabled?: boolean;
};
type Licensing = {
  effectiveStatus?: string;
  state?: { expiresAt?: string | null; graceUntil?: string | null };
  allowed?: boolean;
  reason?: string;
};

type Props = {
  status: Status | null;
  config: Config | null;
  licensing: Licensing | null;
  onRefresh: () => Promise<void>;
};

export default function DashboardPage({ status, config, licensing, onRefresh }: Props) {
  const cards = useMemo(
    () => [
      {
        title: "Bootstrap",
        value: status?.bootstrap?.status ?? "unknown",
        hint: status?.bootstrap?.configured ? "Configured" : "Not configured",
      },
      {
        title: "Cloud",
        value: status?.cloud?.status ?? "unknown",
        hint: status?.cloud?.enabled ? "Enabled" : "Disabled",
      },
      {
        title: "Workers",
        value: status?.workers?.registered ?? 0,
        hint: "Registered",
      },
      {
        title: "Appliance",
        value: config?.applianceName || "Unnamed",
        hint: config?.companyName || "Company",
      },
      {
        title: "License",
        value: licensing?.effectiveStatus ?? "unknown",
        hint: licensing?.state?.expiresAt ? `Expires: ${licensing.state.expiresAt}` : licensing?.reason ?? "—",
      },
      {
        title: "Collection Ready",
        value: licensing?.allowed === false ? "blocked" : "ready",
        hint: licensing?.allowed === false ? licensing?.reason ?? "license required" : "Queue claims allowed",
      },
    ],
    [status, config, licensing]
  );

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span style={{ color: "white" }}>xMon | </span>
            <span className="heading-gradient">Collector</span>
          </h1>
          <p className="page-subtitle">Operational snapshot of the on-prem collector appliance.</p>
        </div>
        <button onClick={onRefresh} className="btn-topbar">Refresh</button>
      </div>

      <div className="cards">
        {cards.map((card) => (
          <div className="card" key={card.title}>
            <h3>{card.title}</h3>
            <div style={{ fontSize: 22, fontWeight: 700 }}>{card.value}</div>
            <p>{card.hint}</p>
          </div>
        ))}
      </div>

      <div className="split" style={{ marginTop: 16 }}>
        <div className="card">
          <h3>Quick Links</h3>
          <ul style={{ margin: 0, paddingLeft: 18, color: "var(--muted)" }}>
            <li><a href="/devices" style={{ color: "var(--accent)" }}>Devices</a>: add managed hosts</li>
            <li><a href="/profiles" style={{ color: "var(--accent)" }}>Profiles</a>: configure ping/SNMP</li>
            <li><a href="/targets" style={{ color: "var(--accent)" }}>Polling Bindings</a>: attach profiles to devices</li>
            <li><a href="/jobs" style={{ color: "var(--accent)" }}>Jobs</a>: review poll activity</li>
            <li><a href="/backups" style={{ color: "var(--accent)" }}>Recovery</a>: export/restore safely</li>
            <li><a href="/licensing" style={{ color: "var(--accent)" }}>Licensing</a>: verify entitlement</li>
          </ul>
        </div>
        <div className="card">
          <h3>Environment</h3>
          <p>Appliance Name: {config?.applianceName ?? "not set"}</p>
          <p>Org ID: {config?.orgId ?? "not set"}</p>
          <p>Cloud Enabled: {String(config?.cloudEnabled ?? false)}</p>
          <p>Audit: view <a href="/audit" style={{ color: "var(--accent)" }}>recent changes</a></p>
        </div>
      </div>
    </div>
  );
}
