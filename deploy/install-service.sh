#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must run as root (use sudo)." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="${SERVICE_NAME:-xbyte-collector}"
SERVICE_USER="${SERVICE_USER:-xbyte}"
SERVICE_GROUP="${SERVICE_GROUP:-${SERVICE_USER}}"
APP_DIR="${APP_DIR:-${ROOT_DIR}}"
ENV_FILE="${ENV_FILE:-/etc/xbyte-collector/xbyte-collector.env}"
SYSTEMD_UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
TEMPLATE_PATH="${ROOT_DIR}/deploy/systemd/xbyte-collector.service"
RESTART="${RESTART:-1}"

if [[ ! -f "${TEMPLATE_PATH}" ]]; then
  echo "Service template not found: ${TEMPLATE_PATH}" >&2
  exit 1
fi

if [[ ! -d "${APP_DIR}" ]]; then
  echo "APP_DIR does not exist: ${APP_DIR}" >&2
  exit 1
fi

if [[ ! -f "${APP_DIR}/dist/src/server.js" ]]; then
  echo "Expected build output missing: ${APP_DIR}/dist/src/server.js" >&2
  echo "Run npm run build first." >&2
  exit 1
fi

install -d -m 0755 /etc/xbyte-collector
if [[ ! -f "${ENV_FILE}" && -f "${ROOT_DIR}/deploy/xbyte-collector.env.example" ]]; then
  install -m 0640 "${ROOT_DIR}/deploy/xbyte-collector.env.example" "${ENV_FILE}"
fi

install -d -m 0750 /var/lib/xbyte-collector
install -d -m 0750 /var/log/xbyte-collector
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" /var/lib/xbyte-collector
chown -R "${SERVICE_USER}:${SERVICE_GROUP}" /var/log/xbyte-collector

tmp_unit="$(mktemp)"
cp "${TEMPLATE_PATH}" "${tmp_unit}"
sed -i \
  -e "s|__SERVICE_USER__|${SERVICE_USER}|g" \
  -e "s|__SERVICE_GROUP__|${SERVICE_GROUP}|g" \
  -e "s|__APP_DIR__|${APP_DIR}|g" \
  -e "s|__ENV_FILE__|${ENV_FILE}|g" \
  "${tmp_unit}"
install -m 0644 "${tmp_unit}" "${SYSTEMD_UNIT_PATH}"
rm -f "${tmp_unit}"

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
if [[ "${RESTART}" == "1" ]]; then
  systemctl restart "${SERVICE_NAME}"
fi

echo "Installed ${SYSTEMD_UNIT_PATH}"
echo "Service user/group: ${SERVICE_USER}:${SERVICE_GROUP}"
echo "App dir: ${APP_DIR}"
echo "Env file: ${ENV_FILE}"
