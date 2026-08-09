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

import requests
import psutil
import pika

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("InfoClusAgent")

CONFIG = {
    "API_URL": os.getenv("API_URL", "http://localhost:5000/api"),
    "RABBITMQ_HOST": os.getenv("RABBITMQ_HOST", "localhost"),
    "RABBITMQ_PORT": int(os.getenv("RABBITMQ_PORT", "5672")),
    "RABBITMQ_USER": os.getenv("RABBITMQ_USER", "guest"),
    "RABBITMQ_PASS": os.getenv("RABBITMQ_PASS", "guest"),
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
    os.makedirs(os.path.dirname(CONFIG["AGENT_ID_FILE"]), exist_ok=True)
    with open(CONFIG["AGENT_ID_FILE"], "w") as f:
        f.write(agent_id)
    return agent_id


def register_agent(agent_id):
    hostname = socket.gethostname()
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        ip = "127.0.0.1"
    os_info = f"{platform.system()} {platform.release()}"

    try:
        resp = requests.post(
            f"{CONFIG['API_URL']}/agents/register",
            json={
                "agentId": agent_id,
                "hostname": hostname,
                "os": os_info,
                "ipAddress": ip,
                "agentVersion": CONFIG["AGENT_VERSION"],
            },
            timeout=10,
        )
        resp.raise_for_status()
        machine = resp.json()
        logger.info("Registered: %s (%s) -> %s", hostname, agent_id, machine.get("id", "?"))
        return True
    except requests.RequestException as e:
        logger.error("Registration failed: %s", e)
        return False


def get_system_stats():
    return {
        "agentVersion": CONFIG["AGENT_VERSION"],
        "os": f"{platform.system()} {platform.release()}",
        "ipAddress": _get_ip(),
        "cpuPercent": psutil.cpu_percent(interval=1),
        "memoryPercent": psutil.virtual_memory().percent,
        "diskPercent": _get_disk_percent(),
        "uptime": int(time.time() - psutil.boot_time()),
    }


def _get_ip():
    try:
        return socket.gethostbyname(socket.gethostname())
    except socket.gaierror:
        return "127.0.0.1"


def _get_disk_percent():
    try:
        return psutil.disk_usage("/").percent
    except Exception:
        return 0.0


def send_heartbeat(agent_id):
    try:
        stats = get_system_stats()
        requests.post(
            f"{CONFIG['API_URL']}/machines/{agent_id}/heartbeat",
            json=stats,
            timeout=10,
        )
    except requests.RequestException:
        logger.warning("Heartbeat failed")


def heartbeat_loop(agent_id, stop_event):
    while not stop_event.is_set():
        send_heartbeat(agent_id)
        stop_event.wait(CONFIG["HEARTBEAT_INTERVAL"])


def execute_command(parameters):
    logger.info("Executing: %s", parameters)
    try:
        result = subprocess.run(
            parameters,
            shell=True,
            capture_output=True,
            text=True,
            timeout=CONFIG["COMMAND_TIMEOUT"],
        )
        output = result.stdout
        if result.stderr:
            output += "\n" + result.stderr
        return output if output.strip() else "(executed, no output)"
    except subprocess.TimeoutExpired:
        return f"ERROR: Command timed out after {CONFIG['COMMAND_TIMEOUT']}s"
    except Exception as e:
        return f"ERROR: {e}"


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
            persistent=True,
            messageId=str(uuid.uuid4()),
        ),
    )
    logger.info("Result sent: %s -> %s", command_id, status)


def on_command(ch, method, properties, body, channel_ref, agent_id):
    command_id = None
    try:
        msg = json.loads(body)
        command_id = msg["commandId"]
        parameters = msg.get("parameters", "")

        send_result(channel_ref, command_id, "Running", "", agent_id)

        result_text = execute_command(parameters)

        status = "Failed" if result_text.startswith("ERROR:") else "Completed"
        send_result(channel_ref, command_id, status, result_text, agent_id)

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
    logger.info("Agent ID: %s", agent_id)

    if not register_agent(agent_id):
        logger.error("Cannot start without registration")
        sys.exit(1)

    heartbeat_thread = threading.Thread(
        target=heartbeat_loop, args=(agent_id, stop_event), daemon=True
    )
    heartbeat_thread.start()

    while not stop_event.is_set():
        try:
            credentials = pika.PlainCredentials(
                CONFIG["RABBITMQ_USER"], CONFIG["RABBITMQ_PASS"]
            )
            params = pika.ConnectionParameters(
                host=CONFIG["RABBITMQ_HOST"],
                port=CONFIG["RABBITMQ_PORT"],
                credentials=credentials,
                heartbeat=30,
                connection_attempts=5,
                retry_delay=3,
            )
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.exchange_declare(
                exchange=EXCHANGE, exchange_type="topic", durable=True
            )

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

            logger.info("Connected to RabbitMQ, queue: %s", queue_name)

            channel.basic_consume(
                queue=queue_name,
                on_message_callback=lambda ch, method, props, body: on_command(
                    ch, method, props, body, channel, agent_id
                ),
            )

            try:
                channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("Disconnected from RabbitMQ")
                continue
            except KeyboardInterrupt:
                break
            finally:
                connection.close()

        except pika.exceptions.AMQPConnectionError:
            logger.warning("Cannot connect to RabbitMQ, retrying in 5s...")
            stop_event.wait(5)
        except Exception as e:
            logger.error("Connection error: %s", e)
            stop_event.wait(5)

    logger.info("Agent shutting down")


if __name__ == "__main__":
    main()
