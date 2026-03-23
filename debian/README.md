# Debian packaging scaffold

This directory holds the early Debian packaging scaffold for the `xbyte-collector` appliance target. It is intentionally minimal and will be completed alongside the Phase 1 Ubuntu appliance build.

Included so far:
- Package metadata (`control`, `changelog`)
- Install mapping scaffold (`xbyte-collector.install`)
- Maintainer script placeholders (`postinst`, `prerm`)

Planned install/runtime layout (staged later by tooling):
- Binaries and compiled assets under `/usr/lib/xbyte-collector/`
- Config under `/etc/xbyte-collector/`
- Data under `/var/lib/xbyte-collector/`
- Logs under `/var/log/xbyte-collector/`
- systemd units under `/etc/systemd/system/`

The actual `.deb` build wiring, maintainer script logic, and file mapping will be expanded in later packaging tasks.
