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
    "AGENT_VERSION": "1.0.0",
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
            with open(path) as f:
                raw = f.read().strip()
            return raw

    hostname = socket.gethostname()
    kernel = platform.release()
    agent_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{hostname}:{kernel}"))
    logger.warning("No /etc/machine-id found, generated fallback ID: %s", agent_id)
    return agent_id


def get_or_create_agent_id():
    if os.path.exists(CONFIG["AGENT_ID_FILE"]):
        with open(CONFIG["AGENT_ID_FILE"]) as f:
            return f.read().strip()

    agent_id = generate_agent_id()
    try:
        os.makedirs(os.path.dirname(CONFIG["AGENT_ID_FILE"]), exist_ok=True)
        with open(CONFIG["AGENT_ID_FILE"], "w") as f:
            f.write(agent_id)
    except Exception as e:
        logger.warning("Could not persist agent ID to file: %s", e)
    return agent_id


def _get_private_ip():
    # 1. Probar conectando a DNS público sin enviar paquetes reales
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

    # 2. Escanear interfaces de red con psutil
    try:
        for iface_name, addrs in psutil.net_if_addrs().items():
            if iface_name.lower().startswith(("lo", "docker", "veth", "br-", "virbr")):
                continue
            for addr in addrs:
                if addr.family == socket.AF_INET and addr.address and not addr.address.startswith("127."):
                    return addr.address
    except Exception:
        pass

    # 3. Intentar resolución de hostname
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

    # 1. Probar curl / wget nativos de Linux
    for cmd in [
        "curl -s --max-time 3 https://api.ipify.org",
        "curl -s --max-time 3 https://ifconfig.me/ip",
        "curl -s --max-time 3 https://icanhazip.com",
        "wget -qO- --timeout=3 https://api.ipify.org",
    ]:
        try:
            r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=4)
            ip = (r.stdout or "").strip()
            if ip and len(ip) <= 45 and not ip.startswith("127.") and not "<" in ip:
                _cached_public_ip = ip
                _cached_public_ip_time = now
                return ip
        except Exception:
            continue

    # 2. Probar urllib con contexto SSL tolerante
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
                if ip and len(ip) <= 45 and not ip.startswith("127.") and not "<" in ip:
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


def on_command(ch, method, properties, body, channel_ref, agent_id):
    command_id = None
    try:
        msg = json.loads(body)
        command_id = msg.get("commandId") or msg.get("CommandId") or (properties.message_id if properties else None)
        parameters = msg.get("parameters") or msg.get("Parameters") or ""

        if not command_id:
            logger.error("Comando recibido sin commandId: %s", body)
            ch.basic_ack(method.delivery_tag)
            return

        # Si es una petición especial para refrescar telemetría bajo demanda
        if parameters == "__refresh_telemetry__":
            logger.info("Petición de refresco de telemetría bajo demanda recibida.")
            send_result(channel_ref, command_id, "Running", "", agent_id)
            publish_heartbeat(channel_ref, agent_id)
            send_result(channel_ref, command_id, "Completed", "Telemetría actualizada en tiempo real.", agent_id)
            ch.basic_ack(method.delivery_tag)
            return

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
    logger.info("Starting InfoClus Agent ID: %s (v%s)", agent_id, CONFIG["AGENT_VERSION"])

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
