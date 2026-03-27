import { useEffect, useMemo, useState } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import AppShell, { NavGroup } from "./layout/AppShell";
import DashboardPage from "./pages/DashboardPage";
import DevicesPage from "./pages/DevicesPage";
import ProfilesPage from "./pages/ProfilesPage";
import TargetsPage from "./pages/TargetsPage";
import JobsPage from "./pages/JobsPage";
import NeighborsPage from "./pages/NeighborsPage";
import WorkersPage from "./pages/WorkersPage";
import SettingsPage from "./pages/SettingsPage";
import LogsPage from "./pages/LogsPage";
import AboutPage from "./pages/AboutPage";
import LicensingPage from "./pages/LicensingPage";
import LoginPage from "./pages/LoginPage";
import SystemPage from "./pages/SystemPage";
import ServicesPage from "./pages/ServicesPage";
import BackupsPage from "./pages/BackupsPage";
import UpdatesPage from "./pages/UpdatesPage";
import AccessPage from "./pages/AccessPage";
import AuditPage from "./pages/AuditPage";
import { ToastProvider } from "./components/UI";

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
type Licensing = { status: string; subscriptionStatus: string; allowed: boolean };

export default function App() {
  const [user, setUser] = useState<User | null>(null);
  const [status, setStatus] = useState<Status | null>(null);
  const [config, setConfig] = useState<Config | null>(null);
  const [licensing, setLicensing] = useState<Licensing | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const location = useLocation();

  useEffect(() => {
    refreshAuth();
  }, []);

  async function refreshAuth() {
    setLoading(true);
    setError(null);
    try {
      const meRes = await fetch("/api/auth/me", { credentials: "include" });
      if (meRes.ok) {
        const data = await meRes.json();
        setUser(data.user);
        await loadAuthenticatedData();
      } else {
        clearAuthenticatedState();
      }
    } catch (err: any) {
      setError(err?.message ?? "auth check failed");
      clearAuthenticatedState();
    } finally {
      setLoading(false);
    }
  }

  function clearAuthenticatedState() {
    setUser(null);
    setStatus(null);
    setConfig(null);
  }

  async function loadAuthenticatedData() {
    const [statusData, configData, licData] = await Promise.all([fetchStatus(), fetchConfig(), fetchLicensing()]);
    if (statusData) setStatus(statusData);
    if (configData) setConfig(configData);
    if (licData) setLicensing(licData);
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

  async function fetchLicensing(): Promise<Licensing | null> {
    const res = await fetch("/api/licensing/status", { credentials: "include" });
    if (!res.ok) return null;
    const data = await res.json();
    return { ...(data.licensing ?? {}), allowed: data.allowed };
  }

  async function handleLogin(username: string, password: string) {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        setError(data.error ?? "login failed");
        clearAuthenticatedState();
      } else {
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
    try {
      await fetch("/api/auth/logout", { method: "POST", credentials: "include" });
      clearAuthenticatedState();
    } catch (err: any) {
      setError(err?.message ?? "logout failed");
    } finally {
      setBusy(false);
    }
  }

  const nav: NavGroup[] = useMemo(
    () => [
      {
        label: "Operations",
        items: [
          { label: "Dashboard", to: "/dashboard" },
          { label: "Devices", to: "/devices" },
          { label: "Profiles", to: "/profiles" },
          { label: "Targets", to: "/targets" },
          { label: "Jobs", to: "/jobs" },
          { label: "Neighbors", to: "/neighbors" },
        ],
      },
      {
        label: "System",
        items: [
          { label: "System", to: "/system" },
          { label: "Services", to: "/services" },
          { label: "Workers", to: "/workers" },
          { label: "Settings", to: "/settings" },
          { label: "Logs", to: "/logs" },
          { label: "Backups", to: "/backups" },
          { label: "Updates", to: "/updates" },
          { label: "Access", to: "/access" },
          { label: "Audit", to: "/audit" },
          { label: "About", to: "/about" },
          { label: "Licensing", to: "/licensing" },
        ],
      },
    ],
    []
  );

  if (loading) {
    return (
      <div className="page" style={{ padding: 32 }}>
        <p>Loading...</p>
      </div>
    );
  }

  if (!user) {
    return <LoginPage busy={busy} error={error} onLogin={handleLogin} />;
  }

  const restricted = licensing && licensing.allowed === false;

  return (
    <ToastProvider>
      <AppShell
        nav={nav}
        user={user}
        onLogout={handleLogout}
        currentPath={location.pathname}
        banner={restricted ? "Appliance restricted: valid license and active subscription required. Polling is disabled." : undefined}
      >
        <Routes>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />
          <Route path="/dashboard" element={<DashboardPage status={status} config={config} licensing={licensing} onRefresh={loadAuthenticatedData} />} />
          <Route path="/devices" element={<DevicesPage />} />
          <Route path="/profiles" element={<ProfilesPage />} />
          <Route path="/targets" element={<TargetsPage />} />
          <Route path="/jobs" element={<JobsPage licensing={licensing} />} />
          <Route path="/neighbors" element={<NeighborsPage />} />
          <Route path="/system" element={<SystemPage />} />
          <Route path="/services" element={<ServicesPage />} />
          <Route path="/workers" element={<WorkersPage />} />
          <Route path="/settings" element={<SettingsPage status={status} config={config} onReload={loadAuthenticatedData} />} />
          <Route path="/logs" element={<LogsPage />} />
          <Route path="/backups" element={<BackupsPage />} />
          <Route path="/updates" element={<UpdatesPage />} />
          <Route path="/access" element={<AccessPage user={user} />} />
          <Route path="/audit" element={<AuditPage />} />
          <Route path="/about" element={<AboutPage />} />
          <Route path="/licensing" element={<LicensingPage />} />
          <Route path="*" element={<Navigate to="/dashboard" replace />} />
        </Routes>
        {error && (
          <div style={{ padding: "12px 16px", background: "#fee2e2", color: "#b91c1c", borderRadius: 12, margin: "16px 20px" }}>
            {error}
          </div>
        )}
      </AppShell>
    </ToastProvider>
  );
}
