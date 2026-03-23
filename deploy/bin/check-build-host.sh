#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

missing_tools=0

check_tool() {
  local tool="$1"
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "MISSING TOOL: ${tool}"
    missing_tools=1
  fi
}

check_tool node
check_tool npm
check_tool dpkg-buildpackage
check_tool dh

dns_ok=1
if command -v getent >/dev/null 2>&1; then
  if ! getent hosts registry.npmjs.org >/dev/null 2>&1; then
    dns_ok=0
  fi
else
  if ! nslookup registry.npmjs.org >/dev/null 2>&1; then
    dns_ok=0
  fi
fi

if [ "${dns_ok}" -eq 0 ]; then
  echo "NETWORK: cannot resolve registry.npmjs.org (npm registry unreachable)"
fi

missing_outputs=0
for path in dist web/dist; do
  if [ ! -e "${ROOT}/${path}" ]; then
    echo "MISSING BUILD OUTPUT: ${path}"
    missing_outputs=1
  fi
done

if [ "${missing_tools}" -eq 0 ] && [ "${dns_ok}" -eq 1 ]; then
  echo "HOST TOOLS: OK (node/npm/dpkg-buildpackage/dh present)"
else
  echo "HOST TOOLS: NOT READY"
fi

if [ "${missing_outputs}" -eq 0 ]; then
  echo "BUILD OUTPUTS: present (dist/, web/dist/)"
else
  echo "BUILD OUTPUTS: missing (build after host prereqs are fixed)"
fi

if [ "${missing_tools}" -eq 0 ] && [ "${dns_ok}" -eq 1 ] && [ "${missing_outputs}" -eq 0 ]; then
  echo "CHECK RESULT: PASS"
else
  echo "CHECK RESULT: FAIL"
fi
