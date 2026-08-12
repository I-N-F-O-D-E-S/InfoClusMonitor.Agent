#!/usr/bin/env python3
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
import ssl
import stat
import datetime
import tarfile
import shutil
import tempfile

import psutil
import pika

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
    "AGENT_VERSION": "1.1.0",
    "HEARTBEAT_INTERVAL": int(os.getenv("HEARTBEAT_INTERVAL", "15")),
    "COMMAND_TIMEOUT": int(os.getenv("COMMAND_TIMEOUT", "120")),
}

EXCHANGE = "infoclus.commands"


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
    logger.warning("No /etc/machine-id found, generated fallback ID: %s", agent_id)
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
        logger.warning("Could not persist agent ID to file: %s", e)
    return agent_id


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
        "curl -s --max-time 3 https://api.ipify.org",
        "curl -s --max-time 3 https://ifconfig.me/ip",
        "curl -s --max-time 3 https://icanhazip.com",
        "wget -qO- --timeout=3 https://api.ipify.org",
    ]:
        try:
            r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=4)
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
            with urllib.request.urlopen(req, timeout=3, **kwargs) as resp:
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


def get_system_stats(agent_id):
    priv_ip = _get_private_ip()
    pub_ip = _get_public_ip()
    return {
        "agentId": agent_id,
        "agentVersion": CONFIG["AGENT_VERSION"],
        "os": f"{platform.system()} {platform.release()}",
        "ipAddress": pub_ip or priv_ip,
        "privateIpAddress": priv_ip,
        "publicIpAddress": pub_ip,
        "cpuPercent": psutil.cpu_percent(interval=1),
        "memoryPercent": psutil.virtual_memory().percent,
        "diskPercent": _get_disk_percent(),
        "uptime": int(time.time() - psutil.boot_time()),
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


def get_connection_params():
    credentials = pika.PlainCredentials(
        CONFIG["RABBITMQ_USER"], CONFIG["RABBITMQ_PASS"]
    )
    return pika.ConnectionParameters(
        host=CONFIG["RABBITMQ_HOST"],
        port=CONFIG["RABBITMQ_PORT"],
        credentials=credentials,
        heartbeat=30,
        connection_attempts=5,
        retry_delay=3,
    )


def publish_registration(channel, agent_id):
    payload = get_registration_payload(agent_id)
    routing_key = f"register.{agent_id}"
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=str(uuid.uuid4()),
        ),
    )
    logger.info("Agent registered via RabbitMQ [%s]: %s (Pub: %s, Priv: %s)",
                routing_key, payload["hostname"], payload["publicIpAddress"], payload["privateIpAddress"])


def publish_heartbeat(channel, agent_id):
    payload = get_system_stats(agent_id)
    routing_key = f"heartbeat.{agent_id}"
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=1,
            content_type="application/json",
            message_id=str(uuid.uuid4()),
        ),
    )
    logger.info("Heartbeat enviado a RabbitMQ [%s]: CPU %s%%, RAM %s%% (Pub: %s, Priv: %s)",
                routing_key, payload["cpuPercent"], payload["memoryPercent"], payload["publicIpAddress"], payload["privateIpAddress"])


def heartbeat_worker(agent_id, stop_event):
    logger.info("Heartbeat worker started (interval: %ss)", CONFIG["HEARTBEAT_INTERVAL"])
    while not stop_event.is_set():
        conn = None
        try:
            conn = pika.BlockingConnection(get_connection_params())
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

            while not stop_event.is_set():
                publish_heartbeat(ch, agent_id)
                stop_event.wait(CONFIG["HEARTBEAT_INTERVAL"])

        except Exception as e:
            if not stop_event.is_set():
                logger.warning("Heartbeat connection error: %s. Retrying in 5s...", e)
                stop_event.wait(5)
        finally:
            if conn and conn.is_open:
                try:
                    conn.close()
                except Exception:
                    pass


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

        # Ordenar: Directorios primero, luego archivos, ambos alfabéticamente
        items.sort(key=lambda x: (not x["isDirectory"], x["name"].lower()))

        return {
            "currentPath": target_path,
            "parentPath": parent_path,
            "items": items[:1000],  # Límite seguro
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


def execute_command(parameters):
    logger.info("Executing command in bash: %s", parameters)
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


def send_result(channel, command_id, status, result_text, agent_id):
    routing_key = f"result.{agent_id}"
    body = json.dumps({
        "commandId": str(command_id),
        "status": status,
        "result": result_text,
    })
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type="application/json",
            message_id=str(uuid.uuid4()),
        ),
    )
    logger.info("Result sent via RabbitMQ [%s]: %s -> %s", routing_key, command_id, status)


def send_transfer_upload_result(transfer_id, status, size_bytes, error_msg, agent_id):
    try:
        conn = pika.BlockingConnection(get_connection_params())
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

        routing_key = f"transfer.upload.{agent_id}"
        body = json.dumps({
            "commandId": str(transfer_id),
            "transferId": str(transfer_id),
            "type": "TransferUploadResult",
            "status": status,
            "sizeBytes": size_bytes,
            "error": error_msg,
        })
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=str(uuid.uuid4()),
            ),
        )
        conn.close()
        logger.info("Transfer upload result sent [%s]: %s -> %s (%s bytes)", routing_key, transfer_id, status, size_bytes)
    except Exception as e:
        logger.error("Error sending transfer upload result: %s", e)


def send_transfer_download_result(transfer_id, status, error_msg, agent_id):
    try:
        conn = pika.BlockingConnection(get_connection_params())
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

        routing_key = f"transfer.download.{agent_id}"
        body = json.dumps({
            "commandId": str(transfer_id),
            "transferId": str(transfer_id),
            "type": "TransferDownloadResult",
            "status": status,
            "error": error_msg,
        })
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=str(uuid.uuid4()),
            ),
        )
        conn.close()
        logger.info("Transfer download result sent [%s]: %s -> %s", routing_key, transfer_id, status)
    except Exception as e:
        logger.error("Error sending transfer download result: %s", e)


def do_transfer_upload(transfer_id, source_path, is_directory, upload_url, agent_id):
    logger.info("Iniciando subida para transferencia [%s] desde '%s' a MinIO...", transfer_id, source_path)
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
                tar.add(source_path, arcname=os.path.basename(source_path.rstrip("/\\")))
            file_to_upload = temp_archive

        file_size = os.path.getsize(file_to_upload)
        logger.info("Subiendo '%s' (%s bytes) a MinIO...", file_to_upload, file_size)

        with open(file_to_upload, "rb") as f:
            req = urllib.request.Request(upload_url, data=f, method="PUT")
            req.add_header("Content-Length", str(file_size))
            req.add_header("Content-Type", "application/octet-stream")
            with urllib.request.urlopen(req, timeout=600) as response:
                if response.status not in (200, 201, 204):
                    raise RuntimeError(f"HTTP error subiendo a MinIO: status code {response.status}")

        logger.info("Subida a MinIO completada exitosamente para [%s]", transfer_id)
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
            logger.info("Descargando archivo comprimido desde MinIO a '%s'...", temp_download)
            
            req = urllib.request.Request(download_url, method="GET")
            with urllib.request.urlopen(req, timeout=600) as resp, open(temp_download, "wb") as out_f:
                shutil.copyfileobj(resp, out_f)

            logger.info("Extrayendo archivo comprimido en '%s'...", target_path)
            os.makedirs(target_path, exist_ok=True)
            with tarfile.open(temp_download, "r:gz") as tar:
                tar.extractall(path=target_path)

        else:
            logger.info("Descargando archivo directo desde MinIO a '%s'...", target_path)
            req = urllib.request.Request(download_url, method="GET")
            with urllib.request.urlopen(req, timeout=600) as resp, open(target_path, "wb") as out_f:
                shutil.copyfileobj(resp, out_f)

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


def send_prepare_download_result(download_id, status, size_bytes, error_msg, agent_id):
    try:
        conn = pika.BlockingConnection(get_connection_params())
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

        routing_key = f"result.{agent_id}"
        body = json.dumps({
            "commandId": str(download_id),
            "downloadId": str(download_id),
            "type": "DownloadReadyResult",
            "status": status,
            "sizeBytes": size_bytes,
            "error": error_msg,
        })
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,
                content_type="application/json",
                message_id=str(uuid.uuid4()),
            ),
        )
        conn.close()
        logger.info("Download ready result sent [%s]: %s -> %s (%s bytes)", routing_key, download_id, status, size_bytes)
    except Exception as e:
        logger.error("Error sending download ready result: %s", e)


def do_prepare_download(download_id, source_path, is_directory, selected_paths, upload_url, agent_id):
    logger.info("Preparando descarga [%s] para MinIO (source: %s, is_dir: %s, selected: %s)...",
                download_id, source_path, is_directory, selected_paths)
    temp_archive = None
    try:
        paths_to_pack = [p for p in (selected_paths if (selected_paths and len(selected_paths) > 0) else [source_path]) if p]

        if not paths_to_pack:
            raise FileNotFoundError("No se especificó ninguna ruta válida para descargar.")

        # Si es un solo archivo normal y no es directorio
        single_path = paths_to_pack[0]
        if len(paths_to_pack) == 1 and not is_directory and os.path.isfile(single_path) and not os.path.islink(single_path):
            file_to_upload = single_path
        else:
            # Comprimir carpeta, symlink o múltiples archivos en .tar.gz
            temp_dir = tempfile.gettempdir()
            temp_archive = os.path.join(temp_dir, f"infoclus_dl_{download_id}.tar.gz")
            logger.info("Comprimiendo %s elementos en '%s'...", len(paths_to_pack), temp_archive)
            with tarfile.open(temp_archive, "w:gz") as tar:
                for p in paths_to_pack:
                    if os.path.exists(p) or os.path.islink(p):
                        arcname = os.path.basename(p.rstrip("/\\"))
                        if not arcname:
                            arcname = "root"
                        tar.add(p, arcname=arcname, recursive=True)
            file_to_upload = temp_archive

        if not os.path.exists(file_to_upload):
            raise FileNotFoundError(f"No se encontró el archivo '{file_to_upload}' para descargar.")

        file_size = os.path.getsize(file_to_upload)
        logger.info("Subiendo descarga [%s] (%s bytes) a MinIO...", download_id, file_size)

        with open(file_to_upload, "rb") as f:
            req = urllib.request.Request(upload_url, data=f, method="PUT")
            req.add_header("Content-Length", str(file_size))
            req.add_header("Content-Type", "application/octet-stream")
            with urllib.request.urlopen(req, timeout=600) as response:
                if response.status not in (200, 201, 204):
                    raise RuntimeError(f"HTTP error subiendo descarga a MinIO: {response.status}")

        logger.info("Descarga lista en MinIO para [%s] (%s bytes)", download_id, file_size)
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


def on_command(ch, method, properties, body, channel_ref, agent_id):
    command_id = None
    try:
        msg = json.loads(body)
        command_id = msg.get("commandId") or msg.get("CommandId") or (properties.message_id if properties else None)
        cmd_type = msg.get("type") or msg.get("Type") or "Exe"
        parameters = msg.get("parameters") or msg.get("Parameters") or ""

        if not command_id:
            logger.error("Comando recibido sin commandId: %s", body)
            ch.basic_ack(method.delivery_tag)
            return

        # Auto-detección SOLO si cmd_type es genérico (Exe, None, "")
        if cmd_type in ("Exe", "Command", None, "") and isinstance(parameters, str) and parameters.startswith("{"):
            try:
                p_parsed = json.loads(parameters)
                if isinstance(p_parsed, dict):
                    if "downloadId" in p_parsed or "selectedPaths" in p_parsed:
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

            logger.info("Explorando directorio remoto [%s]: '%s'", command_id, path_to_browse)
            browse_result = browse_directory(path_to_browse)
            send_result(channel_ref, command_id, "Completed", json.dumps(browse_result), agent_id)
            ch.basic_ack(method.delivery_tag)
            return

        # 2. Subida de transferencia a MinIO (Asíncrono en hilo independiente)
        if cmd_type == "TransferUpload":
            t_params = {}
            if isinstance(parameters, dict):
                t_params = parameters
            elif isinstance(parameters, str) and parameters.startswith("{"):
                t_params = json.loads(parameters)

            transfer_id = t_params.get("transferId", command_id)
            source_path = t_params.get("sourcePath", "")
            is_directory = t_params.get("isDirectory", False)
            upload_url = t_params.get("uploadUrl", "")

            t = threading.Thread(
                target=do_transfer_upload,
                args=(transfer_id, source_path, is_directory, upload_url, agent_id),
                daemon=True,
            )
            t.start()
            ch.basic_ack(method.delivery_tag)
            return

        # 3. Descarga de transferencia desde MinIO (Asíncrono en hilo independiente)
        if cmd_type == "TransferDownload":
            t_params = {}
            if isinstance(parameters, dict):
                t_params = parameters
            elif isinstance(parameters, str) and parameters.startswith("{"):
                t_params = json.loads(parameters)

            transfer_id = t_params.get("transferId", command_id)
            target_path = t_params.get("targetPath", "")
            is_directory = t_params.get("isDirectory", False)
            download_url = t_params.get("downloadUrl", "")

            t = threading.Thread(
                target=do_transfer_download,
                args=(transfer_id, target_path, is_directory, download_url, agent_id),
                daemon=True,
            )
            t.start()
            ch.basic_ack(method.delivery_tag)
            return

        # 4. Preparación de descarga directa al navegador (Archivo o Carpeta .tar.gz)
        if cmd_type == "PrepareDownload":
            p_params = {}
            if isinstance(parameters, dict):
                p_params = parameters
            elif isinstance(parameters, str) and parameters.startswith("{"):
                p_params = json.loads(parameters)

            download_id = p_params.get("downloadId", command_id)
            source_path = p_params.get("sourcePath", "")
            is_directory = p_params.get("isDirectory", False)
            selected_paths = p_params.get("selectedPaths") or []
            upload_url = p_params.get("uploadUrl", "")

            t = threading.Thread(
                target=do_prepare_download,
                args=(download_id, source_path, is_directory, selected_paths, upload_url, agent_id),
                daemon=True,
            )
            t.start()
            ch.basic_ack(method.delivery_tag)
            return

        # 4. Refresco de telemetría bajo demanda
        if parameters == "__refresh_telemetry__":
            logger.info("Petición de refresco de telemetría bajo demanda recibida.")
            send_result(channel_ref, command_id, "Running", "", agent_id)
            publish_heartbeat(channel_ref, agent_id)
            send_result(channel_ref, command_id, "Completed", "Telemetría actualizada en tiempo real.", agent_id)
            ch.basic_ack(method.delivery_tag)
            return

        # 5. Ejecución estándar de comando Linux
        logger.info("Ejecutando comando remoto [%s]: %s", command_id, parameters)
        send_result(channel_ref, command_id, "Running", "", agent_id)

        status, result_text = execute_command(parameters)

        send_result(channel_ref, command_id, status, result_text, agent_id)
        logger.info("Comando [%s] finalizado (%s): %s", command_id, status, result_text[:100])

        ch.basic_ack(method.delivery_tag)
    except Exception as e:
        logger.error("Command processing error: %s", e)
        if command_id:
            send_result(channel_ref, command_id, "Failed", str(e), agent_id)
        ch.basic_ack(method.delivery_tag)


def main():
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    agent_id = get_or_create_agent_id()
    logger.info("Starting BuhoControl Agent ID: %s (v%s)", agent_id, CONFIG["AGENT_VERSION"])

    heartbeat_thread = threading.Thread(
        target=heartbeat_worker, args=(agent_id, stop_event), daemon=True
    )
    heartbeat_thread.start()

    while not stop_event.is_set():
        try:
            connection = pika.BlockingConnection(get_connection_params())
            channel = connection.channel()

            channel.exchange_declare(
                exchange=EXCHANGE, exchange_type="topic", durable=True
            )

            # Enviar registro inicial por RabbitMQ
            publish_registration(channel, agent_id)

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

            logger.info("Listening for commands on queue: %s [command.%s]", queue_name, agent_id)

            channel.basic_consume(
                queue=queue_name,
                on_message_callback=lambda ch, method, props, body: on_command(
                    ch, method, props, body, channel, agent_id
                ),
            )

            try:
                channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("RabbitMQ connection closed by broker, reconnecting...")
                continue
            except KeyboardInterrupt:
                break
            finally:
                if connection.is_open:
                    connection.close()

        except pika.exceptions.AMQPConnectionError:
            if not stop_event.is_set():
                logger.warning("Cannot connect to RabbitMQ broker at %s:%s. Retrying in 5s...",
                               CONFIG["RABBITMQ_HOST"], CONFIG["RABBITMQ_PORT"])
                stop_event.wait(5)
        except Exception as e:
            if not stop_event.is_set():
                logger.error("Unexpected error: %s. Retrying in 5s...", e)
                stop_event.wait(5)

    logger.info("InfoClus Agent stopped gracefully.")


if __name__ == "__main__":
    main()
