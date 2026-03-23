Legacy deploy cleanup (not a git repo at cleanup time)

- Removed legacy deploy assets under `deploy/`:
  - `deploy/README.md`
  - `deploy/install-linux.sh`
  - `deploy/xbyte-collector.env.example`
  - `deploy/xbyte-collector.service`
- These files were tied to the old monolithic collector CLI and manual `/opt/xbyte-collector` service install model.
- New packaging/systemd assets will be recreated later to match the Ubuntu Phase 1 appliance architecture.

Legacy runtime source cleanup (monolithic polling model)
- Removed legacy runtime sources:
  - `src/index.ts`
  - `src/api.ts`
  - `src/config.ts`
  - `src/types.ts`
  - `src/ping.ts`
  - `src/snmp.ts`
  - `src/snapshot.ts`
  - `src/db.ts`
- These implemented the old remote-config, ping/SNMP polling, snapshot, and telemetry flow.
- Replacement code will be rebuilt around the new Phase 1 Ubuntu appliance backend (local API + static frontend + SQLite).
