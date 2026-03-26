import { useEffect, useState } from "react";
import { PageHeader, Table, Pill } from "../components/UI";

type AuditEvent = {
  id: number;
  entityType: string;
  entityId: number | null;
  action: string;
  actor?: string | null;
  note?: string | null;
  createdAt: string;
};

export default function AuditPage() {
  const [events, setEvents] = useState<AuditEvent[]>([]);
  const [entityType, setEntityType] = useState<string>("");
  const [entityId, setEntityId] = useState<string>("");
  const [action, setAction] = useState<string>("");

  useEffect(() => {
    load();
  }, []);

  async function load() {
    const params = new URLSearchParams();
    if (entityType) params.set("entityType", entityType);
    if (entityId) params.set("entityId", entityId);
    if (action) params.set("action", action);
    const res = await fetch(`/api/audit?${params.toString()}`, { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      setEvents(data.events ?? []);
    }
  }

  return (
    <div>
      <PageHeader
        title="Audit Trail"
        subtitle="Who changed what and when. Filter to investigate onboarding and poll operations."
        action={
          <div style={{ display: "flex", gap: 8 }}>
            <select value={entityType} onChange={(e) => setEntityType(e.target.value)}>
              <option value="">All entities</option>
              <option value="device">Device</option>
              <option value="profile">Profile</option>
              <option value="target">Target</option>
              <option value="poll_job">Poll Job</option>
              <option value="backup">Backup/Restore</option>
              <option value="license">Licensing</option>
            </select>
            <input placeholder="Entity ID" value={entityId} onChange={(e) => setEntityId(e.target.value)} style={{ width: 120 }} />
            <input placeholder="Action" value={action} onChange={(e) => setAction(e.target.value)} style={{ width: 140 }} />
            <button onClick={load}>Apply</button>
          </div>
        }
      />

      <Table
        columns={[
          { key: "createdAt", header: "Timestamp" },
          {
            key: "entityType",
            header: "Entity",
            render: (e: AuditEvent) => {
              const link =
                e.entityType === "device"
                  ? "/devices"
                  : e.entityType === "profile"
                  ? "/profiles"
                  : e.entityType === "target"
                  ? "/targets"
                  : e.entityType === "poll_job"
                  ? "/jobs"
                  : e.entityType === "backup"
                  ? "/backups"
                  : e.entityType === "license"
                  ? "/licensing"
                  : null;
              const label = `${e.entityType}${e.entityId ? ` #${e.entityId}` : ""}`;
              return link ? (
                <a href={link} style={{ color: "var(--accent)" }}>
                  {label}
                </a>
              ) : (
                label
              );
            },
          },
          { key: "action", header: "Action", render: (e: AuditEvent) => <Pill status={e.action} /> },
          { key: "actor", header: "Actor", render: (e: AuditEvent) => e.actor ?? "local-user" },
          { key: "note", header: "Note", render: (e: AuditEvent) => e.note ?? "—" },
        ]}
        data={events}
        empty="No audit events yet"
      />
    </div>
  );
}
