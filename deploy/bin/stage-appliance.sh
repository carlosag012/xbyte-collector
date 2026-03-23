#!/usr/bin/env bash
set -euo pipefail

# Stage the xbyte-collector appliance layout locally (no installation, no .deb).
# Expected to run after backend (dist/) and frontend (web/dist/) builds exist.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
STAGE_ROOT="$ROOT/.staging/appliance"

BACKEND_DIST="$ROOT/dist"
FRONTEND_DIST="$ROOT/web/dist"
BOOTSTRAP_SRC="$ROOT/deploy/bin/bootstrap.sh"
ENV_SRC="$ROOT/deploy/xbyte-collector.env.example"
SYSTEMD_SRC_DIR="$ROOT/deploy/systemd"

if [[ ! -d "$BACKEND_DIST" ]]; then
  echo "ERROR: backend build output not found at $BACKEND_DIST" >&2
  exit 1
fi

if [[ ! -d "$FRONTEND_DIST" ]]; then
  echo "ERROR: frontend build output not found at $FRONTEND_DIST" >&2
  exit 1
fi

rm -rf "$STAGE_ROOT"
mkdir -p \
  "$STAGE_ROOT/usr/lib/xbyte-collector/dist" \
  "$STAGE_ROOT/usr/lib/xbyte-collector/web/dist" \
  "$STAGE_ROOT/usr/lib/xbyte-collector/bin" \
  "$STAGE_ROOT/etc/xbyte-collector" \
  "$STAGE_ROOT/etc/systemd/system" \
  "$STAGE_ROOT/var/lib/xbyte-collector" \
  "$STAGE_ROOT/var/log/xbyte-collector"

cp -r "$BACKEND_DIST/"* "$STAGE_ROOT/usr/lib/xbyte-collector/dist/"
cp -r "$FRONTEND_DIST/"* "$STAGE_ROOT/usr/lib/xbyte-collector/web/dist/"

if [[ -f "$BOOTSTRAP_SRC" ]]; then
  cp "$BOOTSTRAP_SRC" "$STAGE_ROOT/usr/lib/xbyte-collector/bin/bootstrap.sh"
  chmod +x "$STAGE_ROOT/usr/lib/xbyte-collector/bin/bootstrap.sh"
fi

if [[ -f "$ENV_SRC" ]]; then
  cp "$ENV_SRC" "$STAGE_ROOT/etc/xbyte-collector/xbyte-collector.env.example"
fi

if [[ -d "$SYSTEMD_SRC_DIR" ]]; then
  cp "$SYSTEMD_SRC_DIR/"*.service "$STAGE_ROOT/etc/systemd/system/" || true
fi

echo "Staged appliance layout at: $STAGE_ROOT"
echo "Contents:"
find "$STAGE_ROOT" -maxdepth 3 -type f | sed "s|$ROOT/||"
