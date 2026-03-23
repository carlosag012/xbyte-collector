#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

cd "${ROOT}"

# Ensure required build tools are present
for tool in dpkg-buildpackage dh; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "ERROR: required tool not found: ${tool}" >&2
    exit 1
  fi
done

# Validate scaffold and build outputs before packaging
./deploy/bin/check-package-scaffold.sh

# Build Debian package (no signing)
dpkg-buildpackage -us -uc

echo "Deb build completed. Packages should be in: $(cd "${ROOT}/.." && pwd)"
