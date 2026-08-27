#!/usr/bin/env python3
"""
=============================================================================
     INFODESCLUSTER - PUBLICADOR DE ACTUALIZACIONES DEL AGENTE A MINIO
=============================================================================
Este script empaqueta automáticamente agent.py e install.sh en agent-package.tar.gz
y los sube al bucket público 'infoclus-releases' en MinIO (storageapi.mrapy.com).

Uso:
    python publish_release.py
    o desde la raíz del proyecto:
    python agent/publish_release.py
=============================================================================
"""

import io
import os
import sys
import json
import tarfile
import py_compile
import urllib.request
import urllib.error
import hmac
import hashlib
from datetime import datetime, timezone

# Forzar encoding UTF-8 seguro en Windows / Linux
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Configuración MinIO
ENDPOINT = "storageapi.mrapy.com"
BUCKET = "infoclus-releases"
KEY = "agent-package.tar.gz"
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "buhoadmin")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "Maca*2023")
REGION = "us-east-1"
SERVICE = "s3"

def sign(k, msg):
    return hmac.new(k, msg.encode("utf-8"), hashlib.sha256).digest()

def main():
    print("=" * 70)
    print(" >>> INFODESCLUSTER - PUBLICANDO ACTUALIZACION DEL AGENTE A MINIO <<<")
    print("=" * 70)

    # 1. Determinar rutas de archivos
    script_dir = os.path.dirname(os.path.abspath(__file__))
    possible_agent_paths = [
        os.path.join(script_dir, "agent.py"),
        os.path.join(script_dir, "agent", "agent.py"),
        os.path.join(os.getcwd(), "agent", "agent.py"),
        os.path.join(os.getcwd(), "agent.py"),
    ]
    possible_install_paths = [
        os.path.join(script_dir, "install.sh"),
        os.path.join(script_dir, "agent", "install.sh"),
        os.path.join(os.getcwd(), "agent", "install.sh"),
        os.path.join(os.getcwd(), "install.sh"),
    ]

    agent_path = next((p for p in possible_agent_paths if os.path.exists(p)), None)
    install_path = next((p for p in possible_install_paths if os.path.exists(p)), None)

    if not agent_path or not install_path:
        print(f"[ERROR] No se encontraron agent.py o install.sh.")
        print(f"   agent.py: {agent_path}")
        print(f"   install.sh: {install_path}")
        sys.exit(1)

    print(f"[1/6] Archivo agente:    {agent_path}")
    print(f"[1/6] Archivo instalador: {install_path}")

    # 2. Verificar sintaxis de Python del agente antes de subir
    print("\n[2/6] Verificando sintaxis de Python en agent.py...")
    try:
        py_compile.compile(agent_path, doraise=True)
        print("   [OK] Sintaxis de agent.py verificada correctamente.")
    except py_compile.PyCompileError as e:
        print(f"[ERROR] Error de sintaxis en agent.py: {e}")
        sys.exit(1)

    # 3. Empaquetar en memoria
    print("\n[3/6] Empaquetando agent.py + install.sh en .tar.gz...")
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode="w:gz") as tar:
        tar.add(agent_path, arcname="agent.py")
        tar.add(install_path, arcname="install.sh")

    tar_bytes = tar_stream.getvalue()
    package_size = len(tar_bytes)
    print(f"   [OK] Paquete generado: {package_size:,} bytes")

    # Guardar copia local en la raiz
    local_pkg_paths = [
        os.path.join(os.path.dirname(agent_path), "..", "agent-package.tar.gz"),
        os.path.join(os.getcwd(), "agent-package.tar.gz")
    ]
    for lp in local_pkg_paths:
        try:
            with open(lp, "wb") as f:
                f.write(tar_bytes)
        except Exception:
            pass

    # 4. Asegurar Bucket en MinIO y Politica Publica
    now = datetime.now(timezone.utc)
    datestamp = now.strftime("%Y%m%d")
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    credential_scope = f"{datestamp}/{REGION}/{SERVICE}/aws4_request"

    k_date = sign(("AWS4" + SECRET_KEY).encode("utf-8"), datestamp)
    k_region = hmac.new(k_date, REGION.encode("utf-8"), hashlib.sha256).digest()
    k_service = hmac.new(k_region, SERVICE.encode("utf-8"), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()

    print(f"\n[4/6] Conectando a MinIO (https://{ENDPOINT}) bucket '{BUCKET}'...")

    # Crear bucket si no existe
    put_bucket_headers = {
        "host": ENDPOINT,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": hashlib.sha256(b"").hexdigest(),
    }
    canonical_headers = "".join(f"{k}:{v}\n" for k, v in sorted(put_bucket_headers.items()))
    signed_headers = ";".join(sorted(put_bucket_headers.keys()))
    canonical_request = f"PUT\n/{BUCKET}\n\n{canonical_headers}\n{signed_headers}\n{put_bucket_headers['x-amz-content-sha256']}"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    sig_bucket = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    req_bucket = urllib.request.Request(f"https://{ENDPOINT}/{BUCKET}", method="PUT")
    for k, v in put_bucket_headers.items():
        if k != "host": req_bucket.add_header(k, v)
    req_bucket.add_header("Authorization", f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, SignedHeaders={signed_headers}, Signature={sig_bucket}")

    try:
        with urllib.request.urlopen(req_bucket) as resp:
            print(f"   [OK] Bucket '{BUCKET}' asegurado (status {resp.status})")
    except urllib.error.HTTPError as e:
        if e.code in (409, 200, 204):
            print(f"   [OK] Bucket '{BUCKET}' listo.")
        else:
            print(f"   [AVISO] {e.code} {e.reason}")

    # Aplicar politica publica de lectura
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                "Resource": [f"arn:aws:s3:::{BUCKET}"]
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{BUCKET}/*"]
            }
        ]
    }
    policy_bytes = json.dumps(policy).encode("utf-8")
    put_policy_headers = {
        "host": ENDPOINT,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": hashlib.sha256(policy_bytes).hexdigest(),
    }
    can_headers_pol = "".join(f"{k}:{v}\n" for k, v in sorted(put_policy_headers.items()))
    sig_headers_pol = ";".join(sorted(put_policy_headers.keys()))
    can_req_pol = f"PUT\n/{BUCKET}\npolicy=\n{can_headers_pol}\n{sig_headers_pol}\n{put_policy_headers['x-amz-content-sha256']}"
    str_to_sign_pol = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(can_req_pol.encode('utf-8')).hexdigest()}"
    sig_pol = hmac.new(k_signing, str_to_sign_pol.encode("utf-8"), hashlib.sha256).hexdigest()

    req_pol = urllib.request.Request(f"https://{ENDPOINT}/{BUCKET}?policy", data=policy_bytes, method="PUT")
    for k, v in put_policy_headers.items():
        if k != "host": req_pol.add_header(k, v)
    req_pol.add_header("Authorization", f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, SignedHeaders={sig_headers_pol}, Signature={sig_pol}")
    try:
        with urllib.request.urlopen(req_pol) as resp:
            print(f"   [OK] Politica de lectura publica activa.")
    except Exception:
        pass

    # 5. Subir el archivo empaquetado a MinIO
    print(f"\n[5/6] Subiendo '{KEY}' ({package_size:,} bytes)...")
    put_obj_headers = {
        "host": ENDPOINT,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": hashlib.sha256(tar_bytes).hexdigest(),
        "content-type": "application/gzip",
    }
    can_headers_obj = "".join(f"{k}:{v}\n" for k, v in sorted(put_obj_headers.items()))
    sig_headers_obj = ";".join(sorted(put_obj_headers.keys()))
    can_req_obj = f"PUT\n/{BUCKET}/{KEY}\n\n{can_headers_obj}\n{sig_headers_obj}\n{put_obj_headers['x-amz-content-sha256']}"
    str_to_sign_obj = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{hashlib.sha256(can_req_obj.encode('utf-8')).hexdigest()}"
    sig_obj = hmac.new(k_signing, str_to_sign_obj.encode("utf-8"), hashlib.sha256).hexdigest()

    req_obj = urllib.request.Request(f"https://{ENDPOINT}/{BUCKET}/{KEY}", data=tar_bytes, method="PUT")
    for k, v in put_obj_headers.items():
        if k != "host": req_obj.add_header(k, v)
    req_obj.add_header("Authorization", f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, SignedHeaders={sig_headers_obj}, Signature={sig_obj}")

    try:
        with urllib.request.urlopen(req_obj) as resp:
            print(f"   [OK] Subida a MinIO completada exitosamente (status {resp.status})")
    except urllib.error.HTTPError as e:
        print(f"[ERROR] Error al subir archivo a MinIO: {e.code} {e.reason}")
        sys.exit(1)

    # 6. Test de Descarga Publica Anonima
    public_url = f"https://{ENDPOINT}/{BUCKET}/{KEY}"
    print(f"\n[6/6] Verificando descarga publica anonima en:")
    print(f"   {public_url}")
    try:
        with urllib.request.urlopen(public_url) as resp:
            read_bytes = len(resp.read())
            print(f"   [OK] Verificacion exitosa: {read_bytes:,} bytes descargados directamente.")
    except Exception as e:
        print(f"   [AVISO] Verificacion: {e}")

    # 7. Resumen y Comando listo
    print("\n" + "=" * 70)
    print(" [EXITO] ACTUALIZACION PUBLICADA Y LISTA PARA USAR")
    print("=" * 70)
    print(f"\nURL Directa y Permanente:\n   {public_url}")
    print(f"\nComando 1-Clic para ejecutar en cualquier servidor Linux (root/sudo):\n")
    print(f"   mkdir -p /tmp/infoclus_agent_pkg && cd /tmp/infoclus_agent_pkg && curl -fsSL '{public_url}' -o package.tar.gz && tar -xzf package.tar.gz && chmod +x install.sh agent.py && sudo bash install.sh")
    print("\n" + "=" * 70 + "\n")

if __name__ == "__main__":
    main()
