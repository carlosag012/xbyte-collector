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

export function Pill({ status, styleOverride }: { status: string; styleOverride?: React.CSSProperties }) {
  const color =
    status === "running"
      ? "#22d3ee"
      : status === "pending"
      ? "#fbbf24"
      : status === "completed"
      ? "#4ade80"
      : status === "failed"
      ? "#f87171"
      : "#cbd5e1";
  return (
    <span className="pill" style={{ background: "rgba(255,255,255,0.05)", color, ...(styleOverride ?? {}) }}>
      {status}
    </span>
  );
}

export function Table<T>({
  columns,
  data,
  empty,
}: {
  columns: Array<{ key: string; header: string; render?: (row: T) => React.ReactNode }>;
  data: T[];
  empty?: string;
}) {
  return (
    <div className="card" style={{ padding: 0 }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead style={{ textAlign: "left", background: "rgba(255,255,255,0.02)" }}>
          <tr>
            {columns.map((c) => (
              <th key={c.key} style={{ padding: "12px 14px", fontSize: 12, color: "#9ca3af", borderBottom: "1px solid var(--border)" }}>
                {c.header}
              </th>
            ))}
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
                {columns.map((c) => (
                  <td key={c.key} style={{ padding: "12px 14px", fontSize: 13 }}>
                    {c.render ? c.render(row) : (row as any)[c.key]}
                  </td>
                ))}
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
