import { PageHeader, Card } from "../components/UI";

export default function AccessPage({ user }: { user: { username: string; role?: string } }) {
  return (
    <div>
      <PageHeader
        title="Access & Users"
        subtitle="Current appliance access model, responsibilities, and the enterprise roadmap."
      />
      <div className="cards">
        <Card title="Current Session">
          <p style={{ margin: 0, color: "var(--muted)" }}>User: {user.username}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Role: {user.role ?? "admin (local)"}</p>
          <p style={{ margin: 0, color: "var(--muted)" }}>Auth mode: local session cookie (no SSO yet)</p>
        </Card>
        <Card title="Authentication">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Local auth is currently enabled. Sessions use HTTP-only cookies. There is one logical admin at this time.
          </p>
          <p style={{ margin: 0, color: "var(--muted)" }}>
            API access inherits the same session; API tokens and SSO are not yet available.
          </p>
        </Card>
        <Card title="Enterprise roadmap">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Planned: SSO (OIDC/SAML), multi-user with RBAC, per-tenant scoping, API tokens, full access audit for every action.
          </p>
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Until then: restrict OS-level access, protect session cookies, and monitor changes via <a href="/audit" style={{ color: "var(--accent)" }}>Audit</a>.
          </p>
        </Card>
      </div>
    </div>
  );
}
