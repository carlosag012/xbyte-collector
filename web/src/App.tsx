import { useEffect, useState } from "react";

type User = { id: number; username: string; role: string; isActive: boolean };
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

export default function App() {
  const [me, setMe] = useState<User | null>(null);
  const [status, setStatus] = useState<Status | null>(null);
  const [config, setConfig] = useState<Config | null>(null);
  const [configDraft, setConfigDraft] = useState<Config>({ applianceName: "", companyName: "", orgId: "", cloudEnabled: false });
  const [loginUsername, setLoginUsername] = useState("admin");
  const [loginPassword, setLoginPassword] = useState("");
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    refreshAuth();
  }, []);

  async function refreshAuth() {
    setLoading(true);
    setError(null);
    const meRes = await fetch("/api/auth/me", { credentials: "include" });
    if (meRes.ok) {
      const data = await meRes.json();
      setMe(data.user);
      await loadAuthenticatedData();
    } else {
      clearAuthenticatedState();
    }
    setLoading(false);
  }

  function clearAuthenticatedState() {
    setMe(null);
    setStatus(null);
    setConfig(null);
    setConfigDraft({ applianceName: "", companyName: "", orgId: "", cloudEnabled: false });
  }

  async function loadAuthenticatedData() {
    const [statusData, configData] = await Promise.all([fetchStatus(), fetchConfig()]);
    if (statusData) setStatus(statusData);
    if (configData) {
      setConfig(configData);
      setConfigDraft({
        applianceName: configData.applianceName ?? "",
        companyName: configData.companyName ?? "",
        orgId: configData.orgId ?? "",
        cloudEnabled: Boolean(configData.cloudEnabled),
      });
    }
  }

  async function fetchStatus(): Promise<Status | null> {
    const res = await fetch("/api/status", { credentials: "include" });
    if (!res.ok) return null;
    const data = await res.json();
    return {
      bootstrap: data.bootstrap,
      cloud: data.cloud,
      workers: data.workers,
    };
  }

  async function fetchConfig(): Promise<Config | null> {
    const res = await fetch("/api/config", { credentials: "include" });
    if (!res.ok) return null;
    const data = await res.json();
    return data.config ?? null;
  }

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: loginUsername, password: loginPassword }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.error ?? "login failed");
        clearAuthenticatedState();
      } else {
        setLoginPassword("");
        await refreshAuth();
      }
    } catch (err: any) {
      setError(err?.message ?? "login failed");
    } finally {
      setBusy(false);
    }
  }

  async function handleLogout() {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await fetch("/api/auth/logout", { method: "POST", credentials: "include" });
      clearAuthenticatedState();
      setMessage("Logged out");
    } catch (err: any) {
      setError(err?.message ?? "logout failed");
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveConfig(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = await fetch("/api/config", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(configDraft),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.error ?? "save failed");
      } else {
        setMessage("Config saved");
        await loadAuthenticatedData();
      }
    } catch (err: any) {
      setError(err?.message ?? "save failed");
    } finally {
      setBusy(false);
    }
  }

  async function handleBootstrap() {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = await fetch("/api/bootstrap/mark-configured", {
        method: "POST",
        credentials: "include",
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.error ?? "bootstrap failed");
      } else {
        setMessage("Bootstrap marked configured");
        await loadAuthenticatedData();
      }
    } catch (err: any) {
      setError(err?.message ?? "bootstrap failed");
    } finally {
      setBusy(false);
    }
  }

  async function handleChangePassword(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      const res = await fetch("/api/auth/change-password", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ currentPassword, newPassword }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setError(data.error ?? "change password failed");
      } else {
        setMessage("Password updated");
        setCurrentPassword("");
        setNewPassword("");
      }
    } catch (err: any) {
      setError(err?.message ?? "change password failed");
    } finally {
      setBusy(false);
    }
  }

  return (
    <div style={{ maxWidth: 780, margin: "0 auto", padding: "32px", fontFamily: "Inter, system-ui, sans-serif" }}>
      <header style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0 }}>xbyte-collector</h1>
        <p style={{ margin: "8px 0 0 0", color: "#555" }}>Phase 1 local appliance UI</p>
      </header>

      {loading ? (
        <p>Loading...</p>
      ) : me ? (
        <div style={{ display: "flex", gap: "24px", flexDirection: "column" }}>
          <section>
            <h2 style={{ margin: "0 0 8px 0", fontSize: 18 }}>Current User</h2>
            <p style={{ margin: 0 }}>Username: {me.username}</p>
            <p style={{ margin: "4px 0" }}>Role: {me.role}</p>
            <button onClick={handleLogout} disabled={busy} style={{ marginTop: 8 }}>
              {busy ? "Working..." : "Logout"}
            </button>
          </section>

          <section>
            <h2 style={{ margin: "0 0 8px 0", fontSize: 18 }}>Change Password</h2>
            <form onSubmit={handleChangePassword} style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 380 }}>
              <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <span>Current Password</span>
                <input
                  type="password"
                  value={currentPassword}
                  onChange={(e) => setCurrentPassword(e.target.value)}
                  disabled={busy}
                  autoComplete="current-password"
                />
              </label>
              <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                <span>New Password</span>
                <input
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  disabled={busy}
                  autoComplete="new-password"
                />
              </label>
              <button type="submit" disabled={busy || !currentPassword || !newPassword}>
                {busy ? "Working..." : "Change Password"}
              </button>
            </form>
          </section>

          <section>
            <h2 style={{ margin: "0 0 8px 0", fontSize: 18 }}>Status</h2>
            {status ? (
              <ul style={{ margin: 0, paddingLeft: 18 }}>
                <li>
                  Bootstrap: {status.bootstrap?.status ?? "unknown"} (configured:{" "}
                  {String(status.bootstrap?.configured ?? false)})
                </li>
                <li>
                  Cloud: {status.cloud?.status ?? "unknown"} (enabled: {String(status.cloud?.enabled ?? false)})
                </li>
                <li>Workers registered: {status.workers?.registered ?? 0}</li>
              </ul>
            ) : (
              <p style={{ margin: 0, color: "#666" }}>Status unavailable.</p>
            )}
          </section>

          <section>
            <h2 style={{ margin: "0 0 8px 0", fontSize: 18 }}>Config</h2>
            {config ? (
              <form onSubmit={handleSaveConfig} style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 480 }}>
                <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <span>Appliance Name</span>
                  <input
                    type="text"
                    value={configDraft.applianceName ?? ""}
                    onChange={(e) => setConfigDraft({ ...configDraft, applianceName: e.target.value })}
                    disabled={busy}
                  />
                </label>
                <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <span>Company Name</span>
                  <input
                    type="text"
                    value={configDraft.companyName ?? ""}
                    onChange={(e) => setConfigDraft({ ...configDraft, companyName: e.target.value })}
                    disabled={busy}
                  />
                </label>
                <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                  <span>Org ID</span>
                  <input
                    type="text"
                    value={configDraft.orgId ?? ""}
                    onChange={(e) => setConfigDraft({ ...configDraft, orgId: e.target.value })}
                    disabled={busy}
                  />
                </label>
                <label style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <input
                    type="checkbox"
                    checked={Boolean(configDraft.cloudEnabled)}
                    onChange={(e) => setConfigDraft({ ...configDraft, cloudEnabled: e.target.checked })}
                    disabled={busy}
                  />
                  <span>Cloud Enabled</span>
                </label>
                <button type="submit" disabled={busy}>
                  {busy ? "Saving..." : "Save Config"}
                </button>
              </form>
            ) : (
              <p style={{ margin: 0, color: "#666" }}>Config unavailable.</p>
            )}
          </section>

          <section>
            <h2 style={{ margin: "0 0 8px 0", fontSize: 18 }}>Bootstrap</h2>
            <p style={{ margin: "0 0 8px 0" }}>
              Status: {status?.bootstrap?.status ?? "unknown"} (configured: {String(status?.bootstrap?.configured ?? false)})
            </p>
            <button onClick={handleBootstrap} disabled={busy}>
              {busy ? "Working..." : "Mark Configured"}
            </button>
          </section>

          <section>
            <h2 style={{ margin: "0 0 8px 0", fontSize: 18 }}>Setup Summary</h2>
            <ul style={{ margin: 0, paddingLeft: 18 }}>
              <li>Bootstrap configured: {String(status?.bootstrap?.configured ?? false)}</li>
              <li>Appliance name set: {Boolean(config?.applianceName ?? "").toString()}</li>
              <li>Company name set: {Boolean(config?.companyName ?? "").toString()}</li>
              <li>Org ID set: {Boolean(config?.orgId ?? "").toString()}</li>
              <li>Cloud enabled: {String(config?.cloudEnabled ?? false)}</li>
            </ul>
          </section>

          {(error || message) && <p style={{ margin: 0, color: error ? "red" : "green" }}>{error ?? message}</p>}
        </div>
      ) : (
        <section>
          <h2 style={{ margin: "0 0 12px 0", fontSize: 18 }}>Login</h2>
          <form onSubmit={handleLogin} style={{ display: "flex", flexDirection: "column", gap: 8, maxWidth: 320 }}>
            <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <span>Username</span>
              <input
                type="text"
                value={loginUsername}
                onChange={(e) => setLoginUsername(e.target.value)}
                disabled={busy}
              />
            </label>
            <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              <span>Password</span>
              <input
                type="password"
                value={loginPassword}
                onChange={(e) => setLoginPassword(e.target.value)}
                disabled={busy}
              />
            </label>
            <button type="submit" disabled={busy || !loginUsername || !loginPassword}>
              {busy ? "Working..." : "Login"}
            </button>
            {error && <p style={{ color: "red", margin: 0 }}>{error}</p>}
          </form>
        </section>
      )}
    </div>
  );
}
