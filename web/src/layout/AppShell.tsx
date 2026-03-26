import { NavLink } from "react-router-dom";

export type NavItem = { label: string; to: string };
export type NavGroup = { label: string; items: NavItem[] };

type Props = {
  nav: NavGroup[];
  user: { username: string; role?: string };
  onLogout: () => void;
  currentPath: string;
  banner?: string;
  children: React.ReactNode;
};

export default function AppShell({ nav, user, onLogout, banner, children }: Props) {
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">xbyte-collector</div>
        {nav.map((group) => (
          <div key={group.label} className="nav-section">
            <div className="nav-section-title">{group.label}</div>
            {group.items.map((item) => (
              <NavLink key={item.to} to={item.to} className={({ isActive }) => `nav-link${isActive ? " active" : ""}`}>
                <span>{item.label}</span>
              </NavLink>
            ))}
          </div>
        ))}
        <div style={{ marginTop: "auto", fontSize: 12, color: "#93a4c4" }}>
          Logged in as <strong>{user.username}</strong>
        </div>
      </aside>
      <div className="main-area">
        <header className="topbar">
          <div>
            <div style={{ fontWeight: 700 }}>Enterprise Appliance</div>
            <div style={{ color: "var(--muted)", fontSize: 13 }}>Manage collector, workers, and discovery data</div>
          </div>
          <button onClick={onLogout} style={{ background: "#0f172a" }}>
            Logout
          </button>
        </header>
        {banner && (
          <div style={{ padding: "12px 20px", background: "rgba(251, 191, 36, 0.18)", color: "#fbbf24" }}>
            {banner} Go to Licensing to activate.
          </div>
        )}
        <main className="page">{children}</main>
      </div>
    </div>
  );
}
