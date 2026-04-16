#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must run as root (use sudo)." >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE_NAME="${SERVICE_NAME:-xbyte-collector}"
WORKER_SERVICE_NAME="${WORKER_SERVICE_NAME:-xbyte-collector-workers}"
SERVICE_USER="${SERVICE_USER:-xbyte}"
SERVICE_GROUP="${SERVICE_GROUP:-${SERVICE_USER}}"
APP_DIR="${APP_DIR:-${ROOT_DIR}}"
ENV_FILE="${ENV_FILE:-/etc/xbyte-collector/xbyte-collector.env}"
SYSTEMD_UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
TEMPLATE_PATH="${ROOT_DIR}/deploy/systemd/xbyte-collector.service"
WORKER_SYSTEMD_UNIT_PATH="/etc/systemd/system/${WORKER_SERVICE_NAME}.service"
WORKER_TEMPLATE_PATH="${ROOT_DIR}/deploy/systemd/xbyte-collector-workers.service"
INSTALL_WORKERS="${INSTALL_WORKERS:-1}"
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
if [[ "${INSTALL_WORKERS}" == "1" && ! -f "${APP_DIR}/dist/src/workers-supervisor.js" ]]; then
  echo "Expected build output missing: ${APP_DIR}/dist/src/workers-supervisor.js" >&2
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

if [[ "${INSTALL_WORKERS}" == "1" ]]; then
  if [[ ! -f "${WORKER_TEMPLATE_PATH}" ]]; then
    echo "Worker service template not found: ${WORKER_TEMPLATE_PATH}" >&2
    exit 1
  fi
  worker_tmp_unit="$(mktemp)"
  cp "${WORKER_TEMPLATE_PATH}" "${worker_tmp_unit}"
  sed -i \
    -e "s|__SERVICE_USER__|${SERVICE_USER}|g" \
    -e "s|__SERVICE_GROUP__|${SERVICE_GROUP}|g" \
    -e "s|__APP_DIR__|${APP_DIR}|g" \
    -e "s|__ENV_FILE__|${ENV_FILE}|g" \
    "${worker_tmp_unit}"
  install -m 0644 "${worker_tmp_unit}" "${WORKER_SYSTEMD_UNIT_PATH}"
  rm -f "${worker_tmp_unit}"
fi

systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
if [[ "${INSTALL_WORKERS}" == "1" ]]; then
  systemctl enable "${WORKER_SERVICE_NAME}"
fi
if [[ "${RESTART}" == "1" ]]; then
  systemctl restart "${SERVICE_NAME}"
  if [[ "${INSTALL_WORKERS}" == "1" ]]; then
    systemctl restart "${WORKER_SERVICE_NAME}"
  fi
fi

echo "Installed ${SYSTEMD_UNIT_PATH}"
if [[ "${INSTALL_WORKERS}" == "1" ]]; then
  echo "Installed ${WORKER_SYSTEMD_UNIT_PATH}"
fi
echo "Service user/group: ${SERVICE_USER}:${SERVICE_GROUP}"
echo "App dir: ${APP_DIR}"
echo "Env file: ${ENV_FILE}"
