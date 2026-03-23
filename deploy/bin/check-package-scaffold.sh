#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

missing=0

require() {
  local path="$1"
  if [ ! -e "${ROOT}/${path}" ]; then
    echo "MISSING: ${path}"
    missing=1
  fi
}

# Debian metadata and maintainer scripts
require "debian/control"
require "debian/changelog"
require "debian/rules"
require "debian/xbyte-collector.install"
require "debian/postinst"
require "debian/prerm"
require "debian/postrm"
require "debian/source/format"
require "debian/source/options"

# Deploy assets
require "deploy/systemd/xbyte-collector.service"
require "deploy/systemd/xbyte-collector-bootstrap.service"
require "deploy/bin/bootstrap.sh"
require "deploy/bin/stage-appliance.sh"
require "deploy/xbyte-collector.env.example"

# Build outputs expected before packaging
require "dist"
require "web/dist"

if [ "${missing}" -ne 0 ]; then
  echo "Validation failed: one or more required files/dirs are missing."
  exit 1
fi

echo "Packaging scaffold check: OK"
echo "Root: ${ROOT}"
