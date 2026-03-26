import { PageHeader, Card } from "../components/UI";
import { useState } from "react";

function download(name: string, data: any) {
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

export default function BackupsPage() {
  const [configResult, setConfigResult] = useState<any | null>(null);
  const [inventoryPreview, setInventoryPreview] = useState<any | null>(null);
  const [inventoryApplied, setInventoryApplied] = useState<any | null>(null);
  const [inventoryPayload, setInventoryPayload] = useState<any | null>(null);
  const [invLoading, setInvLoading] = useState<"preview" | "apply" | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function exportConfig() {
    const res = await fetch("/api/backups/export/config", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      download("config-backup.json", data);
    }
  }

  async function exportInventory() {
    const res = await fetch("/api/backups/export/inventory", { credentials: "include" });
    if (res.ok) {
      const data = await res.json();
      download("inventory-backup.json", data);
    }
  }

  async function importConfig(file?: File) {
    if (!file) return;
    const text = await file.text();
    const parsed = JSON.parse(text);
    const res = await fetch("/api/backups/import/config", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(parsed),
    });
    const data = await res.json().catch(() => ({}));
    if (res.ok) {
      setConfigResult(data);
      setError(null);
    } else {
      setError(data.error ?? "import failed");
    }
  }

  async function loadInventoryFile(file?: File) {
    if (!file) return;
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const inventory = parsed.inventory ?? parsed;
      setInventoryPayload(inventory);
      setInventoryApplied(null);
      setError(null);
      await previewInventory(inventory);
    } catch (e: any) {
      setError("Invalid JSON file");
    }
  }

  async function previewInventory(inventory: any) {
    setInvLoading("preview");
    const res = await fetch("/api/backups/import/inventory", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inventory, dryRun: true }),
    });
    const data = await res.json().catch(() => ({}));
    setInvLoading(null);
    if (res.ok) {
      setInventoryPreview(data.summary ?? data);
      setError(null);
    } else {
      setError(data.error ?? "preview failed");
    }
  }

  async function applyInventory() {
    if (!inventoryPayload || !inventoryPreview) return;
    if (!window.confirm("Apply this import? Merge-only, no deletes.")) return;
    setInvLoading("apply");
    const res = await fetch("/api/backups/import/inventory", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inventory: inventoryPayload, dryRun: false }),
    });
    const data = await res.json().catch(() => ({}));
    setInvLoading(null);
    if (res.ok) {
      setInventoryApplied(data.summary ?? data);
      setError(null);
    } else {
      setError(data.error ?? "import failed");
    }
  }

  return (
    <div>
      <PageHeader
        title="Backups / Recovery"
        subtitle="Safely export or restore appliance config and inventory. Restore flows are merge-only and non-destructive."
      />
      <div className="cards">
        <Card title="Configuration Export">
          <p style={{ margin: 0, color: "var(--muted)" }}>Download appliance configuration as JSON.</p>
          <button style={{ marginTop: 10 }} onClick={exportConfig}>
            Export config
          </button>
        </Card>
        <Card title="Inventory Export">
          <p style={{ margin: 0, color: "var(--muted)" }}>Download devices, profiles, targets, and neighbor review state.</p>
          <button style={{ marginTop: 10 }} onClick={exportInventory}>
            Export inventory
          </button>
        </Card>
      </div>

      <div className="cards" style={{ marginTop: 16 }}>
        <Card title="Import Config">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Merge-import config JSON previously exported. Non-destructive: existing keys are updated, new keys added.
          </p>
          <input type="file" accept="application/json" onChange={(e) => importConfig(e.target.files?.[0])} />
          {configResult && <p style={{ margin: "8px 0 0 0", color: "#a3e635" }}>Updated keys: {configResult.updated}</p>}
        </Card>
        <Card title="Import Inventory">
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Merge-import devices/profiles/targets. Restore runs preview first, then explicit apply. No deletes are performed.
          </p>
          <div style={{ marginTop: 8 }}>
            <input type="file" accept="application/json" onChange={(e) => loadInventoryFile(e.target.files?.[0])} />
            <small style={{ display: "block", color: "var(--muted)", marginTop: 4 }}>
              Preview is required; apply is enabled after a successful preview.
            </small>
          </div>
          {invLoading === "preview" && <p style={{ marginTop: 8, color: "var(--muted)" }}>Running preview…</p>}
          {inventoryPreview && (
            <div style={{ marginTop: 10, color: "#a3e635" }}>
              <strong>Preview (dry-run)</strong>
              <div>Devices: +{inventoryPreview.devices?.created ?? 0} / updated {inventoryPreview.devices?.updated ?? 0}</div>
              <div>Profiles: +{inventoryPreview.profiles?.created ?? 0} / updated {inventoryPreview.profiles?.updated ?? 0}</div>
              <div>Targets: +{inventoryPreview.targets?.created ?? 0}</div>
              {inventoryPreview.errorsDetailed?.length ? (
                <div style={{ color: "#f97316", marginTop: 6 }}>
                  {inventoryPreview.errorsDetailed.map((e: any, idx: number) => (
                    <div key={idx}>{e.code} {e.deviceKey || e.profileKey || e.hostname || e.ipAddress ? `(${[e.hostname, e.ipAddress, e.deviceKey, e.profileKey].filter(Boolean).join(" / ")})` : ""}</div>
                  ))}
                </div>
              ) : null}
              <button
                style={{ marginTop: 8 }}
                disabled={invLoading === "apply"}
                onClick={applyInventory}
              >
                {invLoading === "apply" ? "Applying…" : "Apply import"}
              </button>
            </div>
          )}
          {inventoryApplied && (
            <div style={{ marginTop: 10, color: "#a3e635" }}>
              <strong>Applied</strong>
              <div>Devices: +{inventoryApplied.devices?.created ?? 0} / updated {inventoryApplied.devices?.updated ?? 0}</div>
              <div>Profiles: +{inventoryApplied.profiles?.created ?? 0} / updated {inventoryApplied.profiles?.updated ?? 0}</div>
              <div>Targets: +{inventoryApplied.targets?.created ?? 0}</div>
              {inventoryApplied.errorsDetailed?.length ? (
                <div style={{ color: "#f97316", marginTop: 6 }}>
                  {inventoryApplied.errorsDetailed.map((e: any, idx: number) => (
                    <div key={idx}>{e.code}</div>
                  ))}
                </div>
              ) : null}
            </div>
          )}
        </Card>
      </div>
      <div className="cards" style={{ marginTop: 16 }}>
        <Card title="Recovery Guidance">
          <ul style={{ margin: "6px 0", paddingLeft: 18, color: "var(--muted)" }}>
            <li>Restore order: config first (app settings), then inventory (devices/profiles/targets).</li>
            <li>Current restore is merge-only: no destructive deletes, no DB replacement.</li>
            <li>Verify after apply: check Devices, Profiles, Targets, and Jobs for expected entries.</li>
            <li>Production tip: take a fresh export before applying a restore on a live appliance.</li>
            <li>Not yet available: full database snapshot restore or automatic rollback.</li>
          </ul>
          <p style={{ margin: 0, color: "var(--muted)" }}>
            Need audit history? See <a href="/audit" style={{ color: "var(--accent)" }}>Audit</a> for who exported/imported and when.
          </p>
        </Card>
      </div>
      {error && <p style={{ color: "#f87171" }}>{error}</p>}
    </div>
  );
}
