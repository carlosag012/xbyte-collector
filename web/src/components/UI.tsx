import React from "react";

export function PageHeader({ title, subtitle, action }: { title: string; subtitle?: string; action?: React.ReactNode }) {
  return (
    <div className="page-header">
      <div>
        <h1 className="page-title">{title}</h1>
        {subtitle && <p className="page-subtitle">{subtitle}</p>}
      </div>
      {action}
    </div>
  );
}

export function Card({ title, children, footer }: { title?: string; children: React.ReactNode; footer?: React.ReactNode }) {
  return (
    <div className="card">
      {title && <h3>{title}</h3>}
      {children}
      {footer}
    </div>
  );
}

function formatStatusLabel(status: string) {
  const clean = (status || "").replace(/[_-]+/g, " ").trim();
  return clean.replace(/\b\w/g, (c) => c.toUpperCase()) || "—";
}

function statusColor(status: string) {
  const s = (status || "").toLowerCase();
  if (["enabled", "ready", "running", "completed", "success", "active", "healthy", "up", "promoted", "linked"].includes(s)) return "#4ade80";
  if (["pending", "stale", "grace", "new", "unknown", "needs target", "needs profile", "warning"].includes(s)) return "#fbbf24";
  if (["failed", "disabled", "error", "blocked", "revoked", "unhealthy", "not ready", "expired"].includes(s)) return "#f87171";
  return "#cbd5e1";
}

export function Pill({ status, styleOverride }: { status: string; styleOverride?: React.CSSProperties }) {
  const color = statusColor(status);
  return (
    <span className="pill" style={{ background: "rgba(255,255,255,0.05)", color, ...(styleOverride ?? {}) }}>
      {formatStatusLabel(status)}
    </span>
  );
}

export function Table<T>({
  columns,
  data,
  empty,
}: {
  columns: Array<{ key: string; header: string; render?: (row: T) => React.ReactNode; minWidth?: number | string; nowrap?: boolean }>;
  data: T[];
  empty?: string;
}) {
  return (
    <div className="card" style={{ padding: 0, overflowX: "auto" }}>
      <table style={{ width: "max-content", minWidth: "100%", borderCollapse: "collapse" }}>
        <thead style={{ textAlign: "left", background: "rgba(255,255,255,0.02)" }}>
          <tr>
            {columns.map((c) => {
              const minWidth = c.minWidth ?? 140;
              return (
                <th
                  key={c.key}
                  style={{
                    padding: "12px 14px",
                    fontSize: 12,
                    color: "#9ca3af",
                    borderBottom: "1px solid var(--border)",
                    minWidth,
                    whiteSpace: c.nowrap ? "nowrap" : "normal",
                  }}
                >
                  {c.header}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {data.length === 0 ? (
            <tr>
              <td colSpan={columns.length} style={{ padding: 16, color: "var(--muted)" }}>
                {empty ?? "No data"}
              </td>
            </tr>
          ) : (
            data.map((row, idx) => (
              <tr key={idx} style={{ borderBottom: "1px solid var(--border)" }}>
                {columns.map((c) => {
                  const minWidth = c.minWidth ?? 140;
                  return (
                    <td
                      key={c.key}
                      style={{
                        padding: "12px 14px",
                        fontSize: 13,
                        minWidth,
                        whiteSpace: c.nowrap ? "nowrap" : "normal",
                      }}
                    >
                      {c.render ? c.render(row) : (row as any)[c.key]}
                    </td>
                  );
                })}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export function Empty({ message }: { message: string }) {
  return <div className="placeholder">{message}</div>;
}

// Toasts
type Toast = { id: number; message: string; tone?: "success" | "warning" | "error" };
type ToastContextValue = { push: (t: Omit<Toast, "id">) => void };

const ToastContext = React.createContext<ToastContextValue | null>(null);

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = React.useState<Toast[]>([]);
  const push = React.useCallback((t: Omit<Toast, "id">) => {
    const id = Date.now() + Math.random();
    setToasts((prev) => [...prev, { ...t, id }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((x) => x.id !== id));
    }, 4200);
  }, []);
  return (
    <ToastContext.Provider value={{ push }}>
      {children}
      <ToastStack toasts={toasts} />
    </ToastContext.Provider>
  );
}

export function useToast() {
  const ctx = React.useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx.push;
}

function toneColor(tone?: "success" | "warning" | "error") {
  if (tone === "success") return "#4ade80";
  if (tone === "warning") return "#fbbf24";
  if (tone === "error") return "#f87171";
  return "#e5e7eb";
}

function ToastStack({ toasts }: { toasts: Toast[] }) {
  return (
    <div style={{ position: "fixed", right: 20, bottom: 20, display: "flex", flexDirection: "column", gap: 10, zIndex: 2000 }}>
      {toasts.map((t) => (
        <div
          key={t.id}
          style={{
            background: "rgba(17,24,39,0.9)",
            border: `1px solid ${toneColor(t.tone)}40`,
            color: toneColor(t.tone),
            padding: "10px 12px",
            borderRadius: 10,
            boxShadow: "0 8px 28px rgba(0,0,0,0.35)",
            minWidth: 220,
            maxWidth: 360,
          }}
        >
          {t.message}
        </div>
      ))}
    </div>
  );
}
