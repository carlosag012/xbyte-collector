import { useEffect, useState } from "react";
import { Card, PageHeader, Pill, Table } from "../components/UI";
import { Modal } from "../components/Modal";

type Job = {
  id: number;
  targetId: number;
  status: string;
  leaseOwner: string | null;
  attemptCount: number;
  scheduledAt: string | null;
  startedAt: string | null;
  finishedAt: string | null;
  result: any;
};

export default function JobsPage() {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [targets, setTargets] = useState<any[]>([]);
  const [devices, setDevices] = useState<any[]>([]);
  const [profiles, setProfiles] = useState<any[]>([]);
  const [relations, setRelations] = useState<Record<number, string>>({});
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [targetId, setTargetId] = useState<string>("");
  const [selectedJob, setSelectedJob] = useState<Job | null>(null);

  useEffect(() => {
    load();
  }, [statusFilter]);

  async function load() {
    const query = statusFilter ? `?status=${statusFilter}` : "";
    const [jRes, tRes, dRes, pRes, nRes] = await Promise.all([
      fetch(`/api/poll-jobs${query}`, { credentials: "include" }),
      fetch("/api/poll-targets", { credentials: "include" }),
      fetch("/api/devices", { credentials: "include" }),
      fetch("/api/poll-profiles", { credentials: "include" }),
      fetch("/api/neighbors", { credentials: "include" }),
    ]);
    if (jRes.ok) setJobs((await jRes.json()).jobs ?? []);
    if (tRes.ok) setTargets((await tRes.json()).targets ?? []);
    if (dRes.ok) setDevices((await dRes.json()).devices ?? []);
    if (pRes.ok) setProfiles((await pRes.json()).profiles ?? []);
    if (nRes.ok) {
      const data = await nRes.json();
      const map: Record<number, string> = {};
      (data.neighbors ?? []).forEach((neigh: any) => {
        if (neigh.promotedDeviceId) map[neigh.promotedDeviceId] = "promoted";
        if (neigh.linkedDeviceId) map[neigh.linkedDeviceId] = "linked";
      });
      setRelations(map);
    }
  }

  async function enqueue(e: React.FormEvent) {
    e.preventDefault();
    if (!targetId) return;
    await fetch("/api/poll-jobs/enqueue", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ targetId: Number(targetId) }),
    });
    await load();
  }

  return (
    <div>
      <PageHeader
        title="Jobs"
        subtitle="Inspect poll activity and enqueue manual jobs."
        action={
          <div style={{ display: "flex", gap: 8 }}>
            <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
              <option value="">All statuses</option>
              <option value="pending">Pending</option>
              <option value="running">Running</option>
              <option value="completed">Completed</option>
              <option value="failed">Failed</option>
            </select>
            <button className="btn-topbar" onClick={load}>
              Refresh
            </button>
          </div>
        }
      />

      <div className="cards" style={{ marginBottom: 16 }}>
        <Card title="Manual enqueue">
          <form onSubmit={enqueue} className="form-grid">
            <label>
              <span>Target ID</span>
              <input value={targetId} onChange={(e) => setTargetId(e.target.value)} placeholder="e.g. 3" />
            </label>
            <button type="submit" className="btn-collector" style={{ width: "100%" }}>
              <span className="btn-collector-label">Enqueue</span>
            </button>
          </form>
        </Card>
      </div>

      <Table
        columns={[
          { key: "id", header: "ID" },
          {
            key: "targetId",
            header: "Target",
            render: (j: Job) => {
              const t = targets.find((t) => t.id === j.targetId);
              const dev = devices.find((d) => d.id === t?.deviceId);
              const prof = profiles.find((p) => p.id === t?.profileId);
              return (
                <div>
                  <div>Target #{j.targetId}</div>
                  {t && (
                    <div style={{ color: "var(--muted)", fontSize: 12 }}>
                      {dev?.hostname ?? `device ${t.deviceId}`} · {prof?.name ?? `profile ${t.profileId}`}{" "}
                      {relations[t.deviceId] ? `· ${relations[t.deviceId]}` : ""}
                    </div>
                  )}
                </div>
              );
            },
          },
          { key: "status", header: "Status", render: (j: Job) => <Pill status={j.status} /> },
          { key: "leaseOwner", header: "Lease Owner", render: (j: Job) => j.leaseOwner ?? "—" },
          { key: "attemptCount", header: "Attempts" },
          { key: "startedAt", header: "Started" },
          { key: "finishedAt", header: "Finished" },
          {
            key: "result",
            header: "Result",
            render: (j: Job) => {
              if (!j.result) return "—";
              const msg = j.result?.message || j.result?.error || j.result?.status || JSON.stringify(j.result).slice(0, 80);
              const licenseBlocked = j.result?.blockedByLicense;
              const authError =
                typeof j.result?.error === "string" && (j.result.error.toLowerCase().includes("auth") || j.result.error.toLowerCase().includes("credential"));
              const timeout =
                typeof j.result?.error === "string" &&
                (j.result.error.toLowerCase().includes("timeout") || j.result.error.toLowerCase().includes("unreachable"));
              return (
                <div>
                  <div>{msg}</div>
                  {licenseBlocked && (
                    <a href="/licensing" style={{ color: "var(--accent)", fontSize: 12 }}>
                      Licensing state
                    </a>
                  )}
                  {authError && <span style={{ color: "var(--muted)", fontSize: 12, marginLeft: 6 }}>Check profile credentials</span>}
                  {timeout && <span style={{ color: "var(--muted)", fontSize: 12, marginLeft: 6 }}>Timeout/unreachable — verify IP/reachability</span>}
                  <div style={{ fontSize: 12, color: "var(--muted)" }}>
                    <a href="/logs" style={{ color: "var(--accent)" }}>
                      View logs
                    </a>
                  </div>
                </div>
              );
            },
          },
          {
            key: "actions",
            header: "Actions",
            render: (j: Job) => (
              <div style={{ display: "flex", gap: 8 }}>
                <button className="btn-secondary" onClick={() => setSelectedJob(j)}>Detail</button>
              </div>
            ),
          },
        ]}
        data={jobs}
        empty="No jobs yet"
      />

      {selectedJob && <JobDetailModal job={selectedJob} devices={devices} profiles={profiles} targets={targets} onClose={() => setSelectedJob(null)} />}
    </div>
  );
}

function categorizeJob(job: Job) {
  const res = job.result || {};
  const reason = (res.error || res.message || "").toString().toLowerCase();
  if (res.blockedByLicense) return { category: "licensing", text: "Blocked by license/subscription" };
  if (reason.includes("auth") || reason.includes("credential")) return { category: "auth", text: "Authentication / credential failure" };
  if (reason.includes("timeout") || reason.includes("unreachable")) return { category: "timeout", text: "Timeout / unreachable" };
  if (reason.includes("missing_context")) return { category: "config", text: "Missing target/profile context" };
  if (job.status === "completed") return { category: "success", text: "Completed" };
  return { category: "other", text: res.error || res.message || job.status };
}

function JobDetailModal({ job, devices, profiles, targets, onClose }: { job: Job; devices: any[]; profiles: any[]; targets: any[]; onClose: () => void }) {
  const target = targets.find((t) => t.id === job.targetId);
  const device = devices.find((d) => d.id === target?.deviceId);
  const profile = profiles.find((p) => p.id === target?.profileId);
  const res = job.result || {};
  const cat = categorizeJob(job);

  const guidance: Record<string, { text: string; link?: string }> = {
    licensing: { text: "Resolve entitlement in Licensing", link: "/licensing" },
    auth: { text: "Check profile credentials (Profiles) and Logs", link: "/profiles" },
    timeout: { text: "Verify device IP/reachability and Logs", link: "/logs" },
    config: { text: "Verify device-target-profile linkage", link: "/targets" },
    other: { text: "Inspect Logs and Services", link: "/logs" },
    success: { text: "No action needed" },
  };
  const guide = guidance[cat.category] ?? guidance.other;

  return (
    <Modal title={`Job #${job.id}`} onClose={onClose}>
      <div style={{ display: "grid", gap: 8, color: "var(--muted)" }}>
        <div>
          <strong>Status:</strong> <Pill status={job.status} /> <Pill status={cat.category} /> {cat.text}
        </div>
        <div>
          <strong>Target:</strong> #{job.targetId} {device ? `· ${device.hostname} (${device.ipAddress})` : ""}{" "}
          {profile ? `· ${profile.name} (${profile.kind})` : ""}
        </div>
        <div>
          <strong>Lease Owner:</strong> {job.leaseOwner ?? "—"} | <strong>Attempts:</strong> {job.attemptCount}
        </div>
        <div>
          <strong>Times:</strong> sched {job.scheduledAt ?? "—"} · start {job.startedAt ?? "—"} · finish {job.finishedAt ?? "—"}
        </div>
        <div>
          <strong>Result:</strong> {res.message || res.error || res.status || "—"}
        </div>
        {res.summary && (
          <div>
            <strong>Summary:</strong> interfaces {res.summary.interfacesCount ?? 0} · neighbors {res.summary.lldpNeighborsCount ?? 0}
          </div>
        )}
        {res.error && (
          <div>
            <strong>Error snippet:</strong> <code style={{ fontSize: 12 }}>{res.error}</code>
          </div>
        )}
        {res.blockedByLicense && <div style={{ color: "#fbbf24" }}>License block: {res.code ?? "license_required"} — see Licensing.</div>}
        <div>
          <strong>Next step:</strong>{" "}
          {guide.link ? (
            <a href={guide.link} style={{ color: "var(--accent)" }}>
              {guide.text}
            </a>
          ) : (
            guide.text
          )}
        </div>
        <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 6 }}>
          <a href="/logs" style={{ color: "var(--accent)" }}>
            Logs
          </a>
          <a href="/devices" style={{ color: "var(--accent)" }}>
            Devices
          </a>
          <a href="/profiles" style={{ color: "var(--accent)" }}>
            Profiles
          </a>
          <a href="/targets" style={{ color: "var(--accent)" }}>
            Targets
          </a>
          <a href="/licensing" style={{ color: "var(--accent)" }}>
            Licensing
          </a>
          <a href="/audit" style={{ color: "var(--accent)" }}>
            Audit
          </a>
        </div>
      </div>
    </Modal>
  );
}
