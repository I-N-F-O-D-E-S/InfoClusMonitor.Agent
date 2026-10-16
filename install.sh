#!/usr/bin/env bash
set -e

# ==============================================================================
# InfoClus Agent - Instalador Limpio y Gestor de Servicio Systemd
# ==============================================================================

INSTALL_DIR="/opt/infodescluster-agent"
SERVICE_NAME="infodescluster-agent"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "=========================================================="
echo "    Instalador / Actualizador InfoClus (Linux)"
echo "=========================================================="

# 1. Verificar permisos de root
if [ "$EUID" -ne 0 ]; then
    echo "[!] Por favor ejecuta este instalador como root (o usa sudo)."
    exit 1
fi

mkdir -p "${INSTALL_DIR}"

# Preservar agent_id existente si ya estaba configurado
if [ -f "/root/.infoclus_agent_id" ]; then
    cp "/root/.infoclus_agent_id" "${INSTALL_DIR}/.infoclus_agent_id" 2>/dev/null || true
fi

echo "[1/4] Verificando dependencias del sistema y Python..."
if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq || true
    apt-get install -y -qq python3 python3-pip python3-venv curl || true
elif command -v dnf >/dev/null 2>&1; then
    dnf install -y python3 python3-pip curl || true
elif command -v yum >/dev/null 2>&1; then
    yum install -y python3 python3-pip curl || true
elif command -v pacman >/dev/null 2>&1; then
    pacman -Sy --noconfirm python python-pip curl || true
elif command -v apk >/dev/null 2>&1; then
    apk add --no-cache python3 py3-pip curl || true
fi

echo "[2/4] Configurando entorno virtual Python..."
if [ ! -d "${INSTALL_DIR}/venv" ]; then
    python3 -m venv "${INSTALL_DIR}/venv" || python3 -m venv --without-pip "${INSTALL_DIR}/venv" || true
fi

# Instalar/actualizar dependencias en el entorno virtual
if [ -f "${INSTALL_DIR}/venv/bin/pip" ]; then
    "${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q 2>/dev/null || true
    "${INSTALL_DIR}/venv/bin/pip" install pika psutil -q
else
    # Fallback si venv no tiene pip
    python3 -m pip install pika psutil -q 2>/dev/null || pip3 install pika psutil -q || true
fi

echo "[3/4] Instalando script del agente..."
SCRIPT_SRC=""
if [ -f "agent.py" ]; then
    SCRIPT_SRC="agent.py"
elif [ -f "$(dirname "$0")/agent.py" ]; then
    SCRIPT_SRC="$(dirname "$0")/agent.py"
elif [ -f "/tmp/infoclus_agent_pkg/agent.py" ]; then
    SCRIPT_SRC="/tmp/infoclus_agent_pkg/agent.py"
fi

if [ -n "${SCRIPT_SRC}" ]; then
    cp "${SCRIPT_SRC}" "${INSTALL_DIR}/agent.py"
else
    echo "[!] Error: No se encontró agent.py para instalar."
    exit 1
fi

sed -i 's/\r$//' "${INSTALL_DIR}/agent.py" 2>/dev/null || true
chmod +x "${INSTALL_DIR}/agent.py"

# Determinar binario de Python a utilizar
PYTHON_BIN="${INSTALL_DIR}/venv/bin/python"
if [ ! -f "${PYTHON_BIN}" ]; then
    PYTHON_BIN="$(command -v python3 || command -v python)"
fi

echo "[4/4] Configurando servicio systemd..."
cat << EOF > "${SERVICE_FILE}"
[Unit]
Description=InfodesCluster Linux Telemetry and Remote Execution Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=${INSTALL_DIR}
ExecStart=${PYTHON_BIN} ${INSTALL_DIR}/agent.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment="RABBITMQ_HOST=173.212.195.170"
Environment="RABBITMQ_PORT=5672"
Environment="RABBITMQ_USER=admin"
Environment="RABBITMQ_PASS=Maca*2023"
Environment="HEARTBEAT_INTERVAL=15"

[Install]
WantedBy=multi-user.target
EOF

if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}" >/dev/null 2>&1 || true

    echo "[✓] Reiniciando servicio ${SERVICE_NAME} en segundo plano..."
    (sleep 1 && systemctl restart "${SERVICE_NAME}") >/dev/null 2>&1 &
fi

echo "=========================================================="
echo " [✓] ¡Agente InfoClus INSTALADO / ACTUALIZADO con éxito!"
echo "=========================================================="
