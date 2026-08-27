#!/usr/bin/env python3
"""
=============================================================================
 INFODESCLUSTER AGENT - AGENTE DE MONITOREO, EJECUCIÓN Y RESPALDOS LINUX
=============================================================================
- Comunicación 100% AMQP vía RabbitMQ (Sin puertos HTTP expuestos).
- Monitoreo en tiempo real de CPU, RAM, Disco, Red y Uptime.
- Explorador remoto de archivos del servidor.
- Creación y Restauración ("Carga") de Copias de Seguridad en MinIO S3.
- Transferencias directas servidor a servidor.
- Arquitectura no bloqueante multihilo con reconexión automática indestructible.
=============================================================================
"""

import json
import os
import platform
import socket
import subprocess
import sys
import time
import uuid
import logging
import threading
import signal
import urllib.request
import urllib.error
import ssl
import stat
import datetime
import tarfile
import shutil
import tempfile

import psutil
import pika
import pika.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("InfoClusAgent")

CONFIG = {
    "RABBITMQ_HOST": os.getenv("RABBITMQ_HOST", "45.10.154.37"),
    "RABBITMQ_PORT": int(os.getenv("RABBITMQ_PORT", "5672")),
    "RABBITMQ_USER": os.getenv("RABBITMQ_USER", "infodes"),
    "RABBITMQ_PASS": os.getenv("RABBITMQ_PASS", "SydJqe93jV4o"),
    "AGENT_ID_FILE": os.path.expanduser("~/.infoclus_agent_id"),
    "AGENT_VERSION": "1.2.0",
    "HEARTBEAT_INTERVAL": int(os.getenv("HEARTBEAT_INTERVAL", "15")),
    "COMMAND_TIMEOUT": int(os.getenv("COMMAND_TIMEOUT", "180")),
}

EXCHANGE = "infoclus.commands"


# =============================================================================
# UTILIDADES DE IDENTIFICACIÓN Y CONEXIÓN
# =============================================================================

def generate_agent_id():
    machine_id_paths = [
        "/etc/machine-id",
        "/var/lib/dbus/machine-id",
    ]
    for path in machine_id_paths:
        if os.path.exists(path):
            try:
                with open(path) as f:
                    raw = f.read().strip()
                if raw:
                    return raw
            except Exception:
                pass

    hostname = socket.gethostname()
    kernel = platform.release()
    agent_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{hostname}:{kernel}"))
    logger.warning("No /etc/machine-id encontrado, ID generado de respaldo: %s", agent_id)
    return agent_id


def get_or_create_agent_id():
    if os.path.exists(CONFIG["AGENT_ID_FILE"]):
        try:
            with open(CONFIG["AGENT_ID_FILE"]) as f:
                saved = f.read().strip()
                if saved:
                    return saved
        except Exception:
            pass

    agent_id = generate_agent_id()
    try:
        os.makedirs(os.path.dirname(CONFIG["AGENT_ID_FILE"]), exist_ok=True)
        with open(CONFIG["AGENT_ID_FILE"], "w") as f:
            f.write(agent_id)
    except Exception as e:
        logger.warning("No se pudo guardar ID en archivo: %s", e)
    return agent_id


def get_connection_params(heartbeat=60):
    credentials = pika.PlainCredentials(
        CONFIG["RABBITMQ_USER"], CONFIG["RABBITMQ_PASS"]
    )
    return pika.ConnectionParameters(
        host=CONFIG["RABBITMQ_HOST"],
        port=CONFIG["RABBITMQ_PORT"],
        credentials=credentials,
        heartbeat=heartbeat,
        connection_attempts=3,
        retry_delay=2,
        socket_timeout=15,
        blocked_connection_timeout=15,
    )


def _safe_close(conn):
    if conn:
        try:
            if conn.is_open:
                conn.close()
        except Exception:
            pass


def publish_message_safe(routing_key, body_dict, delivery_mode=2, max_retries=3):
    """
    Publicador seguro y aislado por hilo para evitar colisiones con canales compartidos.
    Reintenta automáticamente en caso de micro-cortes de red.
    """
    body_json = json.dumps(body_dict)
    for attempt in range(1, max_retries + 1):
        conn = None
        try:
            conn = pika.BlockingConnection(get_connection_params(heartbeat=0))
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            ch.basic_publish(
                exchange=EXCHANGE,
                routing_key=routing_key,
                body=body_json,
                properties=pika.BasicProperties(
                    delivery_mode=delivery_mode,
                    content_type="application/json",
                    message_id=str(uuid.uuid4()),
                ),
            )
            _safe_close(conn)
            return True
        except Exception as e:
            _safe_close(conn)
            if attempt < max_retries:
                time.sleep(1.0 * attempt)
            else:
                logger.error("Error al publicar mensaje a [%s] tras %s intentos: %s", routing_key, max_retries, e)
    return False


# =============================================================================
# DETECCIÓN DE RED Y TELEMETRÍA
# =============================================================================

def _get_private_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1.5)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass

    try:
        for iface_name, addrs in psutil.net_if_addrs().items():
            if iface_name.lower().startswith(("lo", "docker", "veth", "br-", "virbr")):
                continue
            for addr in addrs:
                if addr.family == socket.AF_INET and addr.address and not addr.address.startswith("127."):
                    return addr.address
    except Exception:
        pass

    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass

    return "127.0.0.1"


_cached_public_ip = None
_cached_public_ip_time = 0


def _get_public_ip():
    global _cached_public_ip, _cached_public_ip_time
    now = time.time()
    if _cached_public_ip and (now - _cached_public_ip_time < 3600) and not _cached_public_ip.startswith("127."):
        return _cached_public_ip

    for cmd in [
        "curl -s --max-time 2 https://api.ipify.org",
        "curl -s --max-time 2 https://ifconfig.me/ip",
        "curl -s --max-time 2 https://icanhazip.com",
    ]:
        try:
            r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=3)
            ip = (r.stdout or "").strip()
            if ip and len(ip) <= 45 and not ip.startswith("127.") and "<" not in ip:
                _cached_public_ip = ip
                _cached_public_ip_time = now
                return ip
        except Exception:
            continue

    urls = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
        "https://checkip.amazonaws.com",
    ]
    try:
        ctx = ssl._create_unverified_context()
    except Exception:
        ctx = None

    for url in urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "curl/7.68.0"})
            kwargs = {"context": ctx} if ctx else {}
            with urllib.request.urlopen(req, timeout=2.5, **kwargs) as resp:
                ip = resp.read().decode("utf-8").strip()
                if ip and len(ip) <= 45 and not ip.startswith("127.") and "<" not in ip:
                    _cached_public_ip = ip
                    _cached_public_ip_time = now
                    return ip
        except Exception:
            continue

    return _get_private_ip()


def _get_disk_percent():
    try:
        return psutil.disk_usage("/").percent
    except Exception:
        return 0.0


def _get_uptime():
    try:
        return int(time.time() - psutil.boot_time())
    except Exception:
        return 0


def get_system_stats(agent_id):
    priv_ip = _get_private_ip()
    pub_ip = _get_public_ip()
    try:
        cpu = psutil.cpu_percent(interval=0.5)
    except Exception:
        cpu = 0.0

    try:
        ram = psutil.virtual_memory().percent
    except Exception:
        ram = 0.0

    return {
        "agentId": agent_id,
        "agentVersion": CONFIG["AGENT_VERSION"],
        "os": f"{platform.system()} {platform.release()}",
        "ipAddress": pub_ip or priv_ip,
        "privateIpAddress": priv_ip,
        "publicIpAddress": pub_ip,
        "cpuPercent": cpu,
        "memoryPercent": ram,
        "diskPercent": _get_disk_percent(),
        "uptime": _get_uptime(),
    }


def get_registration_payload(agent_id):
    priv_ip = _get_private_ip()
    pub_ip = _get_public_ip()
    return {
        "agentId": agent_id,
        "hostname": socket.gethostname(),
        "os": f"{platform.system()} {platform.release()}",
        "ipAddress": pub_ip or priv_ip,
        "privateIpAddress": priv_ip,
        "publicIpAddress": pub_ip,
        "agentVersion": CONFIG["AGENT_VERSION"],
    }


def publish_registration(agent_id):
    payload = get_registration_payload(agent_id)
    routing_key = f"register.{agent_id}"
    ok = publish_message_safe(routing_key, payload, delivery_mode=2)
    if ok:
        logger.info("Agente registrado vía RabbitMQ [%s]: %s (Pub: %s, Priv: %s, v%s)",
                    routing_key, payload["hostname"], payload["publicIpAddress"], payload["privateIpAddress"], CONFIG["AGENT_VERSION"])


def publish_heartbeat(agent_id):
    payload = get_system_stats(agent_id)
    routing_key = f"heartbeat.{agent_id}"
    publish_message_safe(routing_key, payload, delivery_mode=1)
    logger.info("Heartbeat enviado a RabbitMQ [%s]: CPU %s%%, RAM %s%% (Pub: %s)",
                routing_key, payload["cpuPercent"], payload["memoryPercent"], payload["publicIpAddress"])


def heartbeat_worker(agent_id, stop_event):
    """
    Hilo de telemetría independiente con reconexión persistente.
    """
    logger.info("Heartbeat worker iniciado (intervalo: %ss)", CONFIG["HEARTBEAT_INTERVAL"])
    while not stop_event.is_set():
        try:
            publish_heartbeat(agent_id)
        except Exception as e:
            logger.warning("Error enviando heartbeat: %s", e)

        # Esperar intervalo de forma reactiva a stop_event
        stop_event.wait(CONFIG["HEARTBEAT_INTERVAL"])


# =============================================================================
# EXPLORADOR REMOTO DE ARCHIVOS
# =============================================================================

def _format_size(size_bytes):
    if size_bytes <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(size_bytes)
    idx = 0
    while size >= 1024 and idx < len(units) - 1:
        size /= 1024.0
        idx += 1
    return f"{size:.2f} {units[idx]}"


def browse_directory(target_path):
    target_path = os.path.abspath(target_path or "/")
    if not os.path.exists(target_path):
        return {
            "currentPath": target_path,
            "parentPath": os.path.dirname(target_path) if target_path != "/" else None,
            "items": [],
            "error": f"La ruta '{target_path}' no existe en este servidor.",
        }

    if not os.path.isdir(target_path):
        return {
            "currentPath": target_path,
            "parentPath": os.path.dirname(target_path),
            "items": [],
            "error": f"La ruta '{target_path}' no es un directorio.",
        }

    parent_path = os.path.dirname(target_path) if target_path != "/" else None
    items = []

    try:
        with os.scandir(target_path) as entries:
            for entry in entries:
                try:
                    st = entry.stat(follow_symlinks=False)
                    is_dir = entry.is_dir(follow_symlinks=False)
                    size = st.st_size if not is_dir else 0
                    mode = stat.filemode(st.st_mode)
                    mod_time = datetime.datetime.fromtimestamp(st.st_mtime, tz=datetime.timezone.utc).isoformat()
                    _, ext = os.path.splitext(entry.name)

                    items.append({
                        "name": entry.name,
                        "path": entry.path,
                        "isDirectory": is_dir,
                        "size": size,
                        "sizeFormatted": _format_size(size) if not is_dir else "-",
                        "modifiedAt": mod_time,
                        "permissions": mode,
                        "extension": ext.lower() if ext else "",
                    })
                except Exception as ex:
                    logger.debug("No se pudo leer entrada %s: %s", entry.name, ex)
                    continue

        items.sort(key=lambda x: (not x["isDirectory"], x["name"].lower()))

        return {
            "currentPath": target_path,
            "parentPath": parent_path,
            "items": items[:1000],
            "error": None,
        }
    except PermissionError:
        return {
            "currentPath": target_path,
            "parentPath": parent_path,
            "items": [],
            "error": f"Permiso denegado para leer '{target_path}'.",
        }
    except Exception as e:
        return {
            "currentPath": target_path,
            "parentPath": parent_path,
            "items": [],
            "error": str(e),
        }


# =============================================================================
# EJECUCIÓN DE COMANDOS Y RESULTADOS
# =============================================================================

def execute_command(parameters):
    logger.info("Ejecutando comando en bash: %s", parameters)
    try:
        result = subprocess.run(
            parameters,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=CONFIG["COMMAND_TIMEOUT"],
        )
        output = result.stdout or ""
        if result.stderr:
            output += ("\n" if output else "") + result.stderr

        output = output.strip() if output.strip() else "(ejecutado sin salida en consola)"
        status = "Completed" if result.returncode == 0 else "Failed"
        return status, output
    except subprocess.TimeoutExpired:
        return "Failed", f"ERROR: Tiempo de espera agotado ({CONFIG['COMMAND_TIMEOUT']}s)"
    except Exception as e:
        return "Failed", f"ERROR: {e}"


def send_result(command_id, status, result_text, agent_id):
    routing_key = f"result.{agent_id}"
    payload = {
        "commandId": str(command_id),
        "status": status,
        "result": result_text,
    }
    publish_message_safe(routing_key, payload, delivery_mode=2)
    logger.info("Resultado enviado vía RabbitMQ [%s]: %s -> %s", routing_key, command_id, status)


# =============================================================================
# TRANSFERENCIAS ENTRE SERVIDORES
# =============================================================================

def send_transfer_upload_result(transfer_id, status, size_bytes, error_msg, agent_id):
    routing_key = f"transfer.upload.{agent_id}"
    payload = {
        "commandId": str(transfer_id),
        "transferId": str(transfer_id),
        "type": "TransferUploadResult",
        "status": status,
        "sizeBytes": size_bytes,
        "error": error_msg,
    }
    publish_message_safe(routing_key, payload, delivery_mode=2)
    logger.info("Resultado de subida de transferencia [%s]: %s -> %s (%s bytes)", routing_key, transfer_id, status, size_bytes)


def send_transfer_download_result(transfer_id, status, error_msg, agent_id):
    routing_key = f"transfer.download.{agent_id}"
    payload = {
        "commandId": str(transfer_id),
        "transferId": str(transfer_id),
        "type": "TransferDownloadResult",
        "status": status,
        "error": error_msg,
    }
    publish_message_safe(routing_key, payload, delivery_mode=2)
    logger.info("Resultado de descarga de transferencia [%s]: %s -> %s", routing_key, transfer_id, status)


def _safe_tar_filter(tarinfo):
    """
    Filtro de seguridad para empaquetar directorios Linux en .tar.gz:
    Omite sockets UNIX, FIFOs, nodos de dispositivo especiales o sockets de sockets.
    """
    if tarinfo is None:
        return None
    if tarinfo.isreg() or tarinfo.isdir() or tarinfo.issym() or tarinfo.islnk():
        return tarinfo
    return None


def do_transfer_upload(transfer_id, source_path, is_directory, upload_url, agent_id):
    logger.info("Iniciando subida para transferencia [%s] desde '%s'...", transfer_id, source_path)
    temp_archive = None
    try:
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"La ruta de origen '{source_path}' no existe.")

        file_to_upload = source_path
        if is_directory or os.path.isdir(source_path):
            temp_dir = tempfile.gettempdir()
            temp_archive = os.path.join(temp_dir, f"infoclus_transfer_{transfer_id}.tar.gz")
            logger.info("Comprimiendo directorio '%s' en '%s'...", source_path, temp_archive)
            with tarfile.open(temp_archive, "w:gz") as tar:
                tar.add(
                    source_path,
                    arcname=os.path.basename(source_path.rstrip("/\\")) or "root",
                    recursive=True,
                    filter=_safe_tar_filter
                )
            file_to_upload = temp_archive

        file_size = os.path.getsize(file_to_upload)
        logger.info("Subiendo '%s' (%s bytes)...", file_to_upload, file_size)

        with open(file_to_upload, "rb") as f:
            req = urllib.request.Request(upload_url, data=f, method="PUT")
            req.add_header("Content-Length", str(file_size))
            req.add_header("Content-Type", "application/octet-stream")
            with urllib.request.urlopen(req, timeout=1800) as response:
                if response.status not in (200, 201, 204):
                    raise RuntimeError(f"Error HTTP en transferencia de subida: status code {response.status}")

        logger.info("Subida de transferencia [%s] completada exitosamente.", transfer_id)
        send_transfer_upload_result(transfer_id, "Completed", file_size, None, agent_id)

    except Exception as e:
        logger.error("Fallo en subida para transferencia [%s]: %s", transfer_id, e)
        send_transfer_upload_result(transfer_id, "Failed", 0, str(e), agent_id)
    finally:
        if temp_archive and os.path.exists(temp_archive):
            try:
                os.remove(temp_archive)
            except Exception:
                pass


def do_transfer_download(transfer_id, target_path, is_directory, download_url, agent_id):
    logger.info("Iniciando descarga para transferencia [%s] hacia '%s'...", transfer_id, target_path)
    temp_download = None
    try:
        target_dir = os.path.dirname(os.path.abspath(target_path))
        os.makedirs(target_dir, exist_ok=True)

        if is_directory or download_url.endswith(".tar.gz") or "tar.gz" in download_url:
            temp_dir = tempfile.gettempdir()
            temp_download = os.path.join(temp_dir, f"infoclus_dl_{transfer_id}.tar.gz")
            logger.info("Descargando archivo comprimido a '%s'...", temp_download)

            req = urllib.request.Request(download_url, method="GET")
            with urllib.request.urlopen(req, timeout=1800) as resp, open(temp_download, "wb") as out_f:
                shutil.copyfileobj(resp, out_f, length=64 * 1024)

            logger.info("Extrayendo archivo comprimido en '%s'...", target_path)
            os.makedirs(target_path, exist_ok=True)
            with tarfile.open(temp_download, "r:gz") as tar:
                # Sanitización de extracción
                def is_within_dir(directory, target):
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    return prefix == abs_directory

                for member in tar.getmembers():
                    member_path = os.path.join(target_path, member.name)
                    if not is_within_dir(target_path, member_path):
                        raise Exception(f"Intento de Directory Traversal detectado: {member.name}")
                tar.extractall(path=target_path)

        else:
            logger.info("Descargando archivo directo a '%s'...", target_path)
            req = urllib.request.Request(download_url, method="GET")
            with urllib.request.urlopen(req, timeout=1800) as resp, open(target_path, "wb") as out_f:
                shutil.copyfileobj(resp, out_f, length=64 * 1024)

        logger.info("Descarga y extracción completada con éxito para [%s] en '%s'", transfer_id, target_path)
        send_transfer_download_result(transfer_id, "Completed", None, agent_id)

    except Exception as e:
        logger.error("Fallo en descarga para transferencia [%s]: %s", transfer_id, e)
        send_transfer_download_result(transfer_id, "Failed", str(e), agent_id)
    finally:
        if temp_download and os.path.exists(temp_download):
            try:
                os.remove(temp_download)
            except Exception:
                pass


# =============================================================================
# DESCARGA DIRECTA DE ARCHIVOS (PREPARE DOWNLOAD)
# =============================================================================

def send_prepare_download_result(download_id, status, size_bytes, error_msg, agent_id):
    routing_key = f"result.{agent_id}"
    payload = {
        "commandId": str(download_id),
        "downloadId": str(download_id),
        "type": "DownloadReadyResult",
        "status": status,
        "sizeBytes": size_bytes,
        "error": error_msg,
    }
    publish_message_safe(routing_key, payload, delivery_mode=2)
    logger.info("Resultado de descarga directa enviado [%s]: %s -> %s (%s bytes)", routing_key, download_id, status, size_bytes)


def do_prepare_download(download_id, source_path, is_directory, selected_paths, upload_url, agent_id):
    logger.info("Preparando descarga directa [%s] (source: %s, is_dir: %s, selected: %s)...",
                download_id, source_path, is_directory, selected_paths)
    temp_archive = None
    try:
        paths_to_pack = [p for p in (selected_paths if (selected_paths and len(selected_paths) > 0) else [source_path]) if p]

        if not paths_to_pack:
            raise FileNotFoundError("No se especificó ninguna ruta válida para descargar.")

        single_path = paths_to_pack[0]
        if len(paths_to_pack) == 1 and not is_directory and os.path.isfile(single_path) and not os.path.islink(single_path):
            file_to_upload = single_path
        else:
            temp_dir = tempfile.gettempdir()
            temp_archive = os.path.join(temp_dir, f"infoclus_dl_{download_id}.tar.gz")
            logger.info("Comprimiendo %s elementos en '%s'...", len(paths_to_pack), temp_archive)
            with tarfile.open(temp_archive, "w:gz") as tar:
                for p in paths_to_pack:
                    if os.path.exists(p) or os.path.islink(p):
                        arcname = os.path.basename(p.rstrip("/\\"))
                        if not arcname:
                            arcname = "root"
                        tar.add(p, arcname=arcname, recursive=True, filter=_safe_tar_filter)
            file_to_upload = temp_archive

        if not os.path.exists(file_to_upload):
            raise FileNotFoundError(f"No se encontró el archivo '{file_to_upload}' para descargar.")

        file_size = os.path.getsize(file_to_upload)
        logger.info("Subiendo descarga [%s] (%s bytes)...", download_id, file_size)

        with open(file_to_upload, "rb") as f:
            req = urllib.request.Request(upload_url, data=f, method="PUT")
            req.add_header("Content-Length", str(file_size))
            req.add_header("Content-Type", "application/octet-stream")
            with urllib.request.urlopen(req, timeout=1800) as response:
                if response.status not in (200, 201, 204):
                    raise RuntimeError(f"HTTP error subiendo descarga: {response.status}")

        logger.info("Descarga lista para [%s] (%s bytes)", download_id, file_size)
        send_prepare_download_result(download_id, "Completed", file_size, None, agent_id)

    except Exception as e:
        logger.error("Error preparando descarga [%s]: %s", download_id, e)
        send_prepare_download_result(download_id, "Failed", 0, str(e), agent_id)
    finally:
        if temp_archive and os.path.exists(temp_archive):
            try:
                os.remove(temp_archive)
            except Exception:
                pass


# =============================================================================
# COPIAS DE SEGURIDAD (CREAR Y RESTAURAR / CARGAR)
# =============================================================================

def send_backup_result(backup_id, status, size_bytes, error_msg, agent_id):
    routing_key = f"result.{agent_id}"
    payload = {
        "commandId": str(backup_id),
        "backupId": str(backup_id),
        "type": "BackupResult",
        "status": status,
        "sizeBytes": size_bytes,
        "error": error_msg,
    }
    publish_message_safe(routing_key, payload, delivery_mode=2)
    logger.info("Resultado de copia de seguridad enviado [%s]: %s -> %s (%s bytes)", routing_key, backup_id, status, size_bytes)


def send_restore_backup_result(backup_id, status, target_path, error_msg, agent_id):
    routing_key = f"result.{agent_id}"
    payload = {
        "commandId": str(backup_id),
        "backupId": str(backup_id),
        "type": "RestoreBackupResult",
        "status": status,
        "targetPath": target_path,
        "error": error_msg,
    }
    publish_message_safe(routing_key, payload, delivery_mode=2)
    logger.info("Resultado de restauración de copia enviado [%s]: %s -> %s en '%s'", routing_key, backup_id, status, target_path)


def do_create_backup(backup_id, source_path, file_name, upload_url, agent_id):
    logger.info("Iniciando creación de copia de seguridad [%s] de '%s' hacia MinIO...", backup_id, source_path)
    temp_archive = None
    try:
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"La ruta de origen '{source_path}' no existe en este servidor.")

        temp_dir = tempfile.gettempdir()
        temp_archive = os.path.join(temp_dir, f"infoclus_bk_{backup_id}.tar.gz")
        logger.info("Comprimiendo copia de seguridad '%s' en '%s'...", source_path, temp_archive)

        with tarfile.open(temp_archive, "w:gz") as tar:
            arcname = os.path.basename(source_path.rstrip("/\\"))
            if not arcname:
                arcname = "root"
            tar.add(source_path, arcname=arcname, recursive=True, filter=_safe_tar_filter)

        if not os.path.exists(temp_archive):
            raise RuntimeError("Fallo al generar archivo comprimido de copia de seguridad.")

        file_size = os.path.getsize(temp_archive)
        logger.info("Subiendo copia de seguridad [%s] (%s bytes) a MinIO...", backup_id, file_size)

        with open(temp_archive, "rb") as f:
            req = urllib.request.Request(upload_url, data=f, method="PUT")
            req.add_header("Content-Length", str(file_size))
            req.add_header("Content-Type", "application/gzip")
            with urllib.request.urlopen(req, timeout=3600) as response:
                if response.status not in (200, 201, 204):
                    raise RuntimeError(f"HTTP error subiendo copia a MinIO: status {response.status}")

        logger.info("Copia de seguridad [%s] subida exitosamente a MinIO (%s bytes)", backup_id, file_size)
        send_backup_result(backup_id, "Completed", file_size, None, agent_id)

    except Exception as e:
        logger.error("Error procesando creación de copia [%s]: %s", backup_id, e)
        send_backup_result(backup_id, "Failed", 0, str(e), agent_id)
    finally:
        if temp_archive and os.path.exists(temp_archive):
            try:
                os.remove(temp_archive)
            except Exception:
                pass


def do_restore_backup(backup_id, download_url, target_path, file_name, agent_id):
    """
    Descarga y restaura ("carga") una copia de seguridad en el directorio especificado.
    """
    logger.info("Iniciando restauración/carga de copia de seguridad [%s] ('%s') hacia '%s'...",
                backup_id, file_name, target_path)
    temp_download = None
    try:
        if not download_url:
            raise ValueError("URL de descarga no proporcionada para restaurar la copia de seguridad.")

        target_path = os.path.abspath(target_path or "/")
        os.makedirs(target_path, exist_ok=True)

        temp_dir = tempfile.gettempdir()
        temp_download = os.path.join(temp_dir, f"infoclus_restore_{backup_id}.tar.gz")

        logger.info("Descargando archivo de copia de seguridad desde MinIO a '%s'...", temp_download)
        req = urllib.request.Request(download_url, method="GET")
        with urllib.request.urlopen(req, timeout=3600) as resp, open(temp_download, "wb") as out_f:
            shutil.copyfileobj(resp, out_f)

        dl_size = os.path.getsize(temp_download)
        logger.info("Descarga completada (%s bytes). Verificando y extrayendo en '%s'...", dl_size, target_path)

        if not tarfile.is_tarfile(temp_download):
            raise RuntimeError("El archivo descargado no es un archivo tar.gz válido o está dañado.")

        # Extracción segura
        with tarfile.open(temp_download, "r:*") as tar:
            # Prevención de ataques de path traversal '../'
            def is_within_directory(directory, target):
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
                prefix = os.path.commonprefix([abs_directory, abs_target])
                return prefix == abs_directory

            def safe_extract(tar_obj, path=".", members=None, *, numeric_owner=False):
                for member in (members or tar_obj.getmembers()):
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception(f"Intento de Directory Traversal detectado en el archivo tar: {member.name}")
                tar_obj.extractall(path, members, numeric_owner=numeric_owner)

            safe_extract(tar, path=target_path)

        logger.info("Copia de seguridad [%s] restaurada/cargada con éxito en '%s'", backup_id, target_path)
        send_restore_backup_result(backup_id, "Completed", target_path, None, agent_id)
        send_result(backup_id, "Completed", f"Copia de seguridad restaurada exitosamente en '{target_path}'.", agent_id)

    except Exception as e:
        logger.error("Error restaurando copia de seguridad [%s]: %s", backup_id, e)
        send_restore_backup_result(backup_id, "Failed", target_path, str(e), agent_id)
        send_result(backup_id, "Failed", f"Error al restaurar copia: {e}", agent_id)
    finally:
        if temp_download and os.path.exists(temp_download):
            try:
                os.remove(temp_download)
            except Exception:
                pass


# =============================================================================
# DESPACHADOR DE MENSAJES Y COMANDOS (NO BLOQUEANTE)
# =============================================================================

def handle_incoming_message(ch, method, properties, body, agent_id):
    """
    Recepción asíncrona de mensajes AMQP. Se confirma (ACK) inmediatamente
    y se delega la ejecución a hilos de trabajo independientes.
    """
    try:
        ch.basic_ack(method.delivery_tag)
    except Exception as ack_err:
        logger.debug("Aviso al confirmar mensaje (ACK): %s", ack_err)

    command_id = None
    try:
        msg = json.loads(body)
        command_id = msg.get("commandId") or msg.get("CommandId") or (properties.message_id if properties else None)
        cmd_type = msg.get("type") or msg.get("Type") or "Exe"
        parameters = msg.get("parameters") or msg.get("Parameters") or ""

        if not command_id:
            logger.warning("Mensaje recibido sin commandId: %s", body)
            return

        # Auto-detección de parámetros si el tipo es genérico
        if cmd_type in ("Exe", "Command", None, "") and isinstance(parameters, str) and parameters.startswith("{"):
            try:
                p_parsed = json.loads(parameters)
                if isinstance(p_parsed, dict):
                    if "backupId" in p_parsed and ("uploadUrl" in p_parsed or "sourcePath" in p_parsed):
                        cmd_type = "CreateBackup"
                        parameters = p_parsed
                    elif "backupId" in p_parsed and ("downloadUrl" in p_parsed or "targetPath" in p_parsed):
                        cmd_type = "RestoreBackup"
                        parameters = p_parsed
                    elif "downloadId" in p_parsed or "selectedPaths" in p_parsed:
                        cmd_type = "PrepareDownload"
                        parameters = p_parsed
                    elif "uploadUrl" in p_parsed:
                        cmd_type = "TransferUpload"
                        parameters = p_parsed
                    elif "downloadUrl" in p_parsed:
                        cmd_type = "TransferDownload"
                        parameters = p_parsed
                    elif "path" in p_parsed:
                        cmd_type = "BrowseFiles"
                        parameters = p_parsed
            except Exception:
                pass

        # 1. Explorador de archivos
        if cmd_type == "BrowseFiles":
            path_to_browse = "/"
            if isinstance(parameters, dict):
                path_to_browse = parameters.get("path", "/")
            elif isinstance(parameters, str) and parameters.startswith("{"):
                try:
                    p_obj = json.loads(parameters)
                    path_to_browse = p_obj.get("path", "/")
                except Exception:
                    path_to_browse = parameters
            elif isinstance(parameters, str) and parameters:
                path_to_browse = parameters

            def run_browse():
                logger.info("Explorando directorio remoto [%s]: '%s'", command_id, path_to_browse)
                browse_result = browse_directory(path_to_browse)
                send_result(command_id, "Completed", json.dumps(browse_result), agent_id)

            threading.Thread(target=run_browse, daemon=True).start()
            return

        # 2. Subida de transferencia a MinIO
        if cmd_type == "TransferUpload":
            t_params = parameters if isinstance(parameters, dict) else (json.loads(parameters) if str(parameters).startswith("{") else {})
            transfer_id = t_params.get("transferId", command_id)
            source_path = t_params.get("sourcePath", "")
            is_directory = t_params.get("isDirectory", False)
            upload_url = t_params.get("uploadUrl", "")

            threading.Thread(
                target=do_transfer_upload,
                args=(transfer_id, source_path, is_directory, upload_url, agent_id),
                daemon=True,
            ).start()
            return

        # 3. Descarga de transferencia desde MinIO
        if cmd_type == "TransferDownload":
            t_params = parameters if isinstance(parameters, dict) else (json.loads(parameters) if str(parameters).startswith("{") else {})
            transfer_id = t_params.get("transferId", command_id)
            target_path = t_params.get("targetPath", "")
            is_directory = t_params.get("isDirectory", False)
            download_url = t_params.get("downloadUrl", "")

            threading.Thread(
                target=do_transfer_download,
                args=(transfer_id, target_path, is_directory, download_url, agent_id),
                daemon=True,
            ).start()
            return

        # 4. Preparación de descarga directa al navegador
        if cmd_type == "PrepareDownload":
            p_params = parameters if isinstance(parameters, dict) else (json.loads(parameters) if str(parameters).startswith("{") else {})
            download_id = p_params.get("downloadId", command_id)
            source_path = p_params.get("sourcePath", "")
            is_directory = p_params.get("isDirectory", False)
            selected_paths = p_params.get("selectedPaths") or []
            upload_url = p_params.get("uploadUrl", "")

            threading.Thread(
                target=do_prepare_download,
                args=(download_id, source_path, is_directory, selected_paths, upload_url, agent_id),
                daemon=True,
            ).start()
            return

        # 5. Crear Copia de Seguridad a MinIO
        if cmd_type == "CreateBackup":
            b_params = parameters if isinstance(parameters, dict) else (json.loads(parameters) if str(parameters).startswith("{") else {})
            backup_id = b_params.get("backupId", command_id)
            source_path = b_params.get("sourcePath", "")
            file_name = b_params.get("fileName", "")
            upload_url = b_params.get("uploadUrl", "")

            threading.Thread(
                target=do_create_backup,
                args=(backup_id, source_path, file_name, upload_url, agent_id),
                daemon=True,
            ).start()
            return

        # 6. Restaurar / Cargar Copia de Seguridad desde MinIO
        if cmd_type in ("RestoreBackup", "LoadBackup"):
            r_params = parameters if isinstance(parameters, dict) else (json.loads(parameters) if str(parameters).startswith("{") else {})
            backup_id = r_params.get("backupId", command_id)
            download_url = r_params.get("downloadUrl", "")
            target_path = r_params.get("targetPath", "/")
            file_name = r_params.get("fileName", "backup.tar.gz")

            threading.Thread(
                target=do_restore_backup,
                args=(backup_id, download_url, target_path, file_name, agent_id),
                daemon=True,
            ).start()
            return

        # 7. Refresco de telemetría bajo demanda
        if parameters == "__refresh_telemetry__":
            def run_refresh():
                logger.info("Petición de refresco de telemetría bajo demanda recibida.")
                send_result(command_id, "Running", "", agent_id)
                publish_heartbeat(agent_id)
                send_result(command_id, "Completed", "Telemetría actualizada en tiempo real.", agent_id)

            threading.Thread(target=run_refresh, daemon=True).start()
            return

        # 8. Ejecución de comandos Linux bash estándar (Asíncrono en hilo)
        def run_bash_cmd():
            logger.info("Ejecutando comando remoto [%s]: %s", command_id, parameters)
            send_result(command_id, "Running", "", agent_id)
            status, result_text = execute_command(parameters)
            send_result(command_id, status, result_text, agent_id)
            logger.info("Comando [%s] finalizado (%s): %s", command_id, status, result_text[:100])

        threading.Thread(target=run_bash_cmd, daemon=True).start()

    except Exception as e:
        logger.error("Error al procesar mensaje de comando: %s", e)
        if command_id:
            try:
                send_result(command_id, "Failed", str(e), agent_id)
            except Exception:
                pass


# =============================================================================
# BUCLE PRINCIPAL CON RECONEXIÓN AUTOMÁTICA INDESTRUCTIBLE
# =============================================================================

def main():
    stop_event = threading.Event()

    def handle_signal(sig, frame):
        logger.info("Señal de apagado/reinicio recibida (%s). Deteniendo agente limpiamente...", sig)
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    if hasattr(signal, "SIGHUP"):
        signal.signal(signal.SIGHUP, handle_signal)

    agent_id = get_or_create_agent_id()
    logger.info("Iniciando InfoClus Agent ID: %s (v%s)", agent_id, CONFIG["AGENT_VERSION"])

    # Iniciar hilo de telemetría en segundo plano
    heartbeat_thread = threading.Thread(
        target=heartbeat_worker, args=(agent_id, stop_event), daemon=True
    )
    heartbeat_thread.start()

    retry_delay = 5

    while not stop_event.is_set():
        conn = None
        try:
            logger.info("Conectando a RabbitMQ en %s:%s...", CONFIG["RABBITMQ_HOST"], CONFIG["RABBITMQ_PORT"])
            conn = pika.BlockingConnection(get_connection_params(heartbeat=60))
            channel = conn.channel()

            channel.exchange_declare(
                exchange=EXCHANGE, exchange_type="topic", durable=True
            )

            # Publicar registro inicial del nodo
            publish_registration(agent_id)

            queue_name = f"agent-{agent_id}"
            channel.queue_declare(
                queue=queue_name, durable=True, auto_delete=True
            )
            channel.queue_bind(
                queue=queue_name,
                exchange=EXCHANGE,
                routing_key=f"command.{agent_id}",
            )
            channel.basic_qos(prefetch_count=1)

            logger.info("Escuchando comandos en cola: '%s' [routing_key: command.%s]", queue_name, agent_id)

            channel.basic_consume(
                queue=queue_name,
                on_message_callback=lambda ch, method, props, body: handle_incoming_message(
                    ch, method, props, body, agent_id
                ),
                auto_ack=False
            )

            channel.start_consuming()

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError,
                pika.exceptions.StreamLostError, pika.exceptions.ChannelClosedByBroker,
                pika.exceptions.ConnectionClosedByBroker, socket.error, OSError) as e:
            if not stop_event.is_set():
                logger.warning("Conexión con RabbitMQ perdida o no disponible (%s). Reintentando en %ss de forma continua...", e, retry_delay)
        except Exception as e:
            if not stop_event.is_set():
                logger.error("Error en bucle de RabbitMQ (%s). Reintentando en %ss...", e, retry_delay)
        finally:
            _safe_close(conn)

        if not stop_event.is_set():
            stop_event.wait(retry_delay)

    logger.info("InfoClus Agent se detuvo correctamente.")


if __name__ == "__main__":
    main()
