import { useEffect, useState } from "react";
import { Card } from "../components/UI";

export default function LicensingPage() {
  const [state, setState] = useState<any | null>(null);
  const [apiBase, setApiBase] = useState("");
  const [collectorId, setCollectorId] = useState("");
  const [apiKey, setApiKey] = useState("");
  const [verifyResult, setVerifyResult] = useState<any | null>(null);
  const [verifyError, setVerifyError] = useState<string | null>(null);

  useEffect(() => {
    load();
    loadConfig();
  }, []);

  async function load() {
    const res = await fetch("/api/licensing/status", { credentials: "include" });
    if (res.ok) setState((await res.json()).licensing ?? null);
  }

  async function loadConfig() {
    const res = await fetch("/api/xmon/config", { credentials: "include" });
    if (!res.ok) return;
    const body = await res.json();
    setApiBase(body.apiBase ?? "");
    setCollectorId(body.collectorId ?? "");
  }

  async function saveConfig(e: React.FormEvent) {
    e.preventDefault();
    const res = await fetch("/api/xmon/config", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ apiBase, collectorId, apiKey }),
    });
    if (!res.ok) {
      alert("Save failed");
      return;
    }
    setVerifyError(null);
    setVerifyResult(null);
    if (!apiKey) setApiKey("");
  }

  async function verifyAuth() {
    setVerifyError(null);
    setVerifyResult(null);
    const res = await fetch("/api/xmon/verify", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ apiBase, collectorId, apiKey }),
    });
    const body = await res.json();
    if (!res.ok || !body.ok) {
      setVerifyError(body.error ?? `verify_failed_${res.status}`);
      return;
    }
    setVerifyResult(body);
    // refresh cached status
    load();
  }

  return (
    <div>
      <div className="page-header">
        <div>
          <h1 className="page-title">Collector Authorization</h1>
          <p className="page-subtitle">Configure xMon cloud credentials and verify collector authorization.</p>
        </div>
      </div>
      <div className="cards">
        <Card title="Status">
          <p style={{ margin: 0, color: "var(--muted)" }}>State: {state?.effectiveStatus ?? state?.status ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Subscription: {state?.subscriptionStatus ?? "unknown"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Customer: {state?.customer ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Expires: {state?.expiresAt ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Last validated: {state?.validatedAt ?? "—"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Last error: {state?.lastError ?? "—"}</p>
        </Card>
        <Card title="Cloud credentials">
          <form onSubmit={saveConfig} className="form-grid">
            <label>
              <span>API Base</span>
              <input value={apiBase} onChange={(e) => setApiBase(e.target.value)} placeholder="https://api.example.com/api/xmon" />
            </label>
            <label>
              <span>Collector ID</span>
              <input value={collectorId} onChange={(e) => setCollectorId(e.target.value)} required />
            </label>
            <label>
              <span>API Key</span>
              <input value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="Paste API key" />
            </label>
            <button type="submit">Save</button>
          </form>
        </Card>
        <Card title="Verify authorization">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Uses the saved credentials to call the xMon backend and update cached license state.
          </p>
          <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
            <button onClick={verifyAuth}>Verify authorization</button>
            <button onClick={load}>Refresh local status</button>
          </div>
          {verifyError ? <p style={{ color: "var(--danger)", marginTop: 8 }}>Error: {verifyError}</p> : null}
          {verifyResult ? (
            <div style={{ marginTop: 10, fontSize: 14, color: "var(--muted)" }}>
              <p style={{ margin: 0 }}>Authorized: {String(verifyResult.authorized)}</p>
              <p style={{ margin: 0 }}>Collection allowed: {String(verifyResult.collectionAllowed)}</p>
              <p style={{ margin: 0 }}>Reason: {verifyResult.reason ?? "—"}</p>
              <p style={{ margin: 0 }}>License status: {verifyResult.licenseStatus ?? "—"}</p>
              <p style={{ margin: 0 }}>Effective until: {verifyResult.effectiveUntil ?? "—"}</p>
              <p style={{ margin: 0 }}>Collector registered: {String(verifyResult.collectorRegistered ?? false)}</p>
              <p style={{ margin: 0 }}>Collector limit: {verifyResult.collectorLimit ?? "—"}</p>
              <p style={{ margin: 0 }}>Active collectors: {verifyResult.activeCollectorCount ?? "—"}</p>
            </div>
          ) : null}
        </Card>
      </div>
    </div>
  );
}
