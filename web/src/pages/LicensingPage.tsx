import { useEffect, useState } from "react";
import { Card, PageHeader } from "../components/UI";

export default function LicensingPage() {
  const [state, setState] = useState<any | null>(null);
  const [licenseKey, setLicenseKey] = useState("");
  const [customer, setCustomer] = useState("");

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const res = await fetch("/api/licensing/status", { credentials: "include" });
    if (res.ok) setState((await res.json()).licensing ?? null);
  }

  async function activate(e: React.FormEvent) {
    e.preventDefault();
    const res = await fetch("/api/licensing/activate", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ licenseKey, customer }),
    });
    if (res.ok) {
      await load();
      setLicenseKey("");
    }
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Licensing & Subscription</h1>
          <p className="page-subtitle">Enforcement is active: appliance requires valid license + paid subscription.</p>
        </div>
      </div>
      <div className="cards">
        <Card title="Warning">
          <p style={{ margin: 0, color: "#fbbf24" }}>
            Appliance functionality is restricted until a valid xByte license and active subscription are configured.
          </p>
        </Card>
        <Card title="Status">
          <p style={{ margin: 0, color: "var(--muted)" }}>State: {state?.effectiveStatus ?? state?.status ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Subscription: {state?.subscriptionStatus ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Customer: {state?.customer ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Expires: {state?.expiresAt ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Last validated: {state?.validatedAt ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Last error: {state?.lastError ?? "—"}</p>
        </Card>
        <Card title="Activate License">
          <form onSubmit={activate} className="form-grid">
            <label>
              <span>License Key</span>
              <input value={licenseKey} onChange={(e) => setLicenseKey(e.target.value)} required />
            </label>
            <label>
              <span>Customer</span>
              <input value={customer} onChange={(e) => setCustomer(e.target.value)} />
            </label>
            <button type="submit">Activate</button>
          </form>
        </Card>
        <Card title="Future enforcement">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Planned: remote validation, grace periods, subscription sync, offline token validation, automated shutdown.
          </p>
          <p style={{ margin: 0, color: "var(--muted)" }}>
            For restricted state troubleshooting, see <a href="/logs" style={{ color: "var(--accent)" }}>Logs</a> and <a href="/system" style={{ color: "var(--accent)" }}>System</a>.
          </p>
        </Card>
      </div>
    </div>
  );
}
