#!/usr/bin/env bash
set -e

# ==============================================================================
# InfodesCluster Agent - Instalador Limpio y Gestor de Servicio Systemd
# ==============================================================================

INSTALL_DIR="/opt/infodescluster-agent"
SERVICE_NAME="infodescluster-agent"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

echo "=========================================================="
echo "    Instalador / Actualizador InfodesCluster (Linux)"
echo "=========================================================="

# 1. Verificar permisos de root
if [ "$EUID" -ne 0 ]; then
    echo "[!] Por favor ejecuta este instalador como root (o usa sudo)."
    exit 1
fi

echo "[1/6] Deteniendo y limpiando versiones anteriores..."
# Detener y deshabilitar servicios anteriores si existen
systemctl stop "${SERVICE_NAME}" 2>/dev/null || true
systemctl disable "${SERVICE_NAME}" 2>/dev/null || true
systemctl stop infoclus-agent 2>/dev/null || true
systemctl disable infoclus-agent 2>/dev/null || true

# Matar procesos residuales de agent.py si hubieran quedado colgados
pkill -f "python.*agent.py" 2>/dev/null || true

# Limpiar directorio de instalación previo pero conservando el agent_id si ya existía
AGENT_ID=""
if [ -f "/root/.infoclus_agent_id" ]; then
    AGENT_ID=$(cat "/root/.infoclus_agent_id")
fi

rm -rf "${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

if [ -n "$AGENT_ID" ]; then
    echo "$AGENT_ID" > "/root/.infoclus_agent_id"
fi

echo "[2/6] Verificando e instalando dependencias del sistema..."
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

echo "[3/6] Creando entorno virtual Python limpio e instalando librerías..."
python3 -m venv "${INSTALL_DIR}/venv"
"${INSTALL_DIR}/venv/bin/pip" install --upgrade pip -q
"${INSTALL_DIR}/venv/bin/pip" install pika psutil -q

echo "[4/6] Instalando script actualizado del agente..."
# Copiar agent.py desde el directorio actual o donde se encuentre el script
if [ -f "agent.py" ]; then
    cp agent.py "${INSTALL_DIR}/agent.py"
elif [ -f "$(dirname "$0")/agent.py" ]; then
    cp "$(dirname "$0")/agent.py" "${INSTALL_DIR}/agent.py"
else
    echo "[!] Error: No se encontró agent.py en la carpeta actual."
    exit 1
fi

chmod +x "${INSTALL_DIR}/agent.py"

echo "[5/6] Configurando nuevo servicio systemd..."
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

echo "[6/6] Recargando e iniciando servicio..."
systemctl daemon-reload
systemctl enable "${SERVICE_NAME}"
systemctl restart "${SERVICE_NAME}"

# Esperar 2 segundos para verificar que arrancó correctamente
sleep 2

if systemctl is-active --quiet "${SERVICE_NAME}"; then
    echo "=========================================================="
    echo " [✓] ¡Agente InfodesCluster ACTUALIZADO y corriendo!"
    echo "=========================================================="
    echo " - Estado: ACTIVO (Running)"
    echo " - Comandos útiles:"
    echo "     systemctl status ${SERVICE_NAME}"
    echo "     journalctl -u ${SERVICE_NAME} -f"
    echo "=========================================================="
else
    echo "[!] El servicio no pudo iniciar. Revisa los logs con: journalctl -u ${SERVICE_NAME} -e"
fi
