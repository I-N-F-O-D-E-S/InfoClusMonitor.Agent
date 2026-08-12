#!/usr/bin/env bash
set -e

# ==============================================================================
# BuhoControl Agent - Instalador Limpio y Gestor de Servicio Systemd
# ==============================================================================

INSTALL_DIR="/opt/infodescluster-agent"
SERVICE_NAME="infodescluster-agent"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "=========================================================="
echo "    Instalador / Actualizador BuhoControl (Linux)"
echo "=========================================================="

# 1. Verificar permisos de root
if [ "$EUID" -ne 0 ]; then
    echo "[!] Por favor ejecuta este instalador como root (o usa sudo)."
    exit 1
fi

mkdir -p "${INSTALL_DIR}"

# Preservar o configurar agent_id
AGENT_ID=""
if [ -f "/root/.infoclus_agent_id" ]; then
    AGENT_ID=$(cat "/root/.infoclus_agent_id")
fi

echo "[1/4] Verificando dependencias del sistema y Python..."
if [ ! -d "${INSTALL_DIR}/venv" ]; then
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq
        apt-get install -y -qq python3 python3-pip python3-venv curl
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y python3 python3-pip curl
    elif command -v yum >/dev/null 2>&1; then
        yum install -y python3 python3-pip curl
    elif command -v pacman >/dev/null 2>&1; then
        pacman -Sy --noconfirm python python-pip curl
    fi

    echo "[2/4] Creando entorno virtual Python limpio..."
    python3 -m venv "${INSTALL_DIR}/venv"
    "${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q
    "${INSTALL_DIR}/venv/bin/pip" install pika psutil -q
else
    echo "[2/4] Entorno virtual ya configurado."
fi

echo "[3/4] Instalando script actualizado del agente..."
if [ -f "agent.py" ]; then
    cp agent.py "${INSTALL_DIR}/agent.py"
elif [ -f "$(dirname "$0")/agent.py" ]; then
    cp "$(dirname "$0")/agent.py" "${INSTALL_DIR}/agent.py"
else
    echo "[!] Error: No se encontró agent.py en la carpeta actual."
    exit 1
fi

chmod +x "${INSTALL_DIR}/agent.py"

echo "[4/4] Verificando servicio systemd..."
if [ ! -f "${SERVICE_FILE}" ]; then
    cat << 'EOF' > "${SERVICE_FILE}"
[Unit]
Description=InfodesCluster Linux Telemetry and Remote Execution Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/infodescluster-agent
ExecStart=/opt/infodescluster-agent/venv/bin/python /opt/infodescluster-agent/agent.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal
Environment="RABBITMQ_HOST=45.10.154.37"
Environment="RABBITMQ_PORT=5672"
Environment="RABBITMQ_USER=infodes"
Environment="RABBITMQ_PASS=SydJqe93jV4o"
Environment="HEARTBEAT_INTERVAL=15"

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable "${SERVICE_NAME}"
fi

echo "[✓] Reiniciando servicio en segundo plano..."
(sleep 1 && systemctl restart "${SERVICE_NAME}") >/dev/null 2>&1 &

echo "=========================================================="
echo " [✓] ¡Agente BuhoControl ACTUALIZADO exitosamente!"
echo "=========================================================="
