import os
import io
import uuid
import argparse
import datetime
import psycopg2
import paramiko
from dotenv import load_dotenv
from PIL import Image
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =============================
# 🔧 CONFIGURACIÓN
# =============================

load_dotenv()

SUPABASE_CONN = os.getenv("SUPABASE_CONN")
EC2_KEY_PATH = os.getenv("EC2_KEY_PATH")
EC2_HOST = os.getenv("EC2_HOST")
EC2_USER = os.getenv("EC2_USER", "ubuntu")
EC2_UPLOAD_PATH = os.getenv("EC2_UPLOAD_PATH", "/home/ubuntu/frontend/uploads/")
EC2_PUBLIC_URL = os.getenv("EC2_PUBLIC_URL", "http://78.13.176.172/uploads/")

short_id = str(uuid.uuid4())[:4]
archivo_log = f"logs/log_{datetime.datetime.now().strftime('%Y-%m-%d')}-fotosv5-{short_id}.txt"
os.makedirs("logs", exist_ok=True)

def write_log(msg: str):
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{fecha}] {msg}"
    with open(archivo_log, "a") as f:
        f.write(line + "\n")
    print(line)

# =============================
# 📸 CATÁLOGO DE MODELOS
# =============================

models_catalog = [
    {"name": "BASE PARA BOCINA", "model": "BasesBocina", "tabla": "basesBocina"},
    {"name": "EPICENTROS", "model": "Epicentros", "tabla": "Epicentros"},
    {"name": "SEGURIDAD", "model": "Seguridad", "tabla": "seguridad"},
    {"name": "AMPLIFICADORES", "model": "Amplificadores", "tabla": "Amplificadores"},
    {"name": "AUTOESTEREOS", "model": "Estereos", "tabla": "Estereos"},
    {"name": "ESTEREOS (2022)", "model": "Estereos", "tabla": "Estereos"},
    {"name": "BOCINAS", "model": "Bocinas", "tabla": "Bocinas"},
    {"name": "TWEETERS", "model": "Tweeters", "tabla": "Tweeters"},
    {"name": "FRENTES", "model": "Bases", "tabla": "Bases"},
    {"name": "CAJONES", "model": "Cajones", "tabla": "Cajones"},
    {"name": "ECUALIZADORES", "model": "Ecualizadores", "tabla": "Ecualizadores"},
    {"name": "KIT DE CABLES", "model": "KitsCables", "tabla": "KitsCables"},
    {"name": "MEDIOS RANGOS", "model": "MediosRangos", "tabla": "MediosRangos"},
    {"name": "PROCESADOR", "model": "Procesadores", "tabla": "Procesadores"},
    {"name": "SET DE MEDIOS", "model": "SetMedios", "tabla": "SetMedios"},
    {"name": "WOOFERS", "model": "Woofers", "tabla": "Woofers"},
    {"name": "ARNESES", "model": "Arneses", "tabla": "Arneses"},
    {"name": "ADAPTADORES DE ANTENA", "model": "AdaptadoresAntena", "tabla": "AdaptadoresAntena"},
    {"name": "ACCESORIOS DE INSTALACION", "model": "Accesorios", "tabla": "Accesorios"},
    {"name": "ADAPTADORES DE IMPEDANCIA", "model": "AdaptadoresImpedancia", "tabla": "AdaptadoresImpedancia"},
    {"name": "ILUMINACION", "model": "Iluminacion", "tabla": "iluminacion"},
    {"name": "VIDEO", "model": "Video", "tabla": "video"},
    {"name": "AUDIO MARINO", "model": "AudioMarino", "tabla": "audioMarino"},
    {"name": "SERVICIOS", "model": "Servicios", "tabla": "servicios"},
    {"name": "ACCESORIOS DE CAMIONETA", "model": "AccesoriosCamioneta", "tabla": "accesoriosCamioneta"},
    {"name": "COMPONENTES", "model": "Componentes", "tabla": "Componentes"}
]

# =============================
# 🔐 GOOGLE DRIVE + BD + SSH
# =============================

creds = service_account.Credentials.from_service_account_info({
    "type": os.environ["TYPE"],
    "project_id": os.environ["PROJECT_ID"],
    "private_key_id": os.environ["PRIVATE_KEY_ID"],
    "private_key": os.environ["PRIVATE_KEY"].replace('\\n', '\n'),
    "client_email": os.environ["CLIENT_EMAIL"],
    "client_id": os.environ["CLIENT_ID"],
    "auth_uri": os.environ["AUTH_URI"],
    "token_uri": os.environ["TOKEN_URI"],
    "auth_provider_x509_cert_url": os.environ["AUTH_PROVIDER_X509_CERT_URL"],
    "client_x509_cert_url": os.environ["CLIENT_X509_CERT_URL"]
})

folder_id = os.environ["FOLDER_ID"]
service = build("drive", "v3", credentials=creds)
conn = psycopg2.connect(SUPABASE_CONN)

# =============================
# 🧩 FUNCIONES AUXILIARES
# =============================

def find_model_by_name(name):
    for m in models_catalog:
        if m["name"].strip().upper() == name.strip().upper():
            return m
    return None

def get_folder_metadata(file_id):
    write_log(f"📡 Obteniendo metadata para carpeta ID={file_id}")
    try:
        meta = service.files().get(fileId=file_id, fields="name, parents").execute()
        name = meta.get("name")
        parents = meta.get("parents", [])
        write_log(f"📁 Carpeta detectada: {name} | Padres: {parents}")
        return name, parents
    except HttpError as http_err:
        if http_err.resp.status == 404:
            write_log(f"❌ No se encontró la carpeta (404): {file_id}")
        elif http_err.resp.status == 403:
            write_log(f"🚫 Sin permisos para acceder a carpeta ID={file_id}")
        else:
            write_log(f"⚠️ Error HTTP al obtener metadata ({file_id}): {http_err}")
        return None, []
    except Exception as e:
        write_log(f"⚠️ Error genérico obteniendo metadata de carpeta {file_id}: {e}")
        return None, []

def build_drive_path(file_id):
    path_parts = []
    current_id = file_id
    try:
        while True:
            meta = service.files().get(fileId=current_id, fields="name, parents").execute()
            path_parts.insert(0, meta["name"])
            parents = meta.get("parents")
            if not parents:
                break
            current_id = parents[0]
    except Exception as e:
        write_log(f"⚠️ Error construyendo ruta Drive (id={file_id}): {e}")
    full_path = "/".join(path_parts)
    write_log(f"📂 Ruta construida para ID {file_id}: {full_path}")
    return full_path

def resolve_model(folder_id, depth=0):
    if depth > 8 or not folder_id:
        return None
    name, parents = get_folder_metadata(folder_id)
    write_log(f"🔎 Buscando modelo en carpeta (nivel {depth}): {name} | ID={folder_id}")

    if not name:
        write_log(f"⚠️ No se pudo obtener nombre de carpeta con ID {folder_id}")
        return None

    model = find_model_by_name(name)
    if model:
        write_log(f"✅ Modelo detectado: {model['name']} → {model['model']}")
        return model

    if not parents:
        write_log(f"⚠️ La carpeta '{name}' no tiene padres visibles (posible falta de permisos o carpeta raíz)")
        return None

    write_log(f"↩️ Subiendo al padre: {parents[0]}")
    return resolve_model(parents[0], depth + 1)

def ssh_connect():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(EC2_HOST, username=EC2_USER, key_filename=EC2_KEY_PATH)
    sftp = ssh.open_sftp()
    return ssh, sftp

def ensure_remote_dir(sftp, remote_dir):
    dirs = remote_dir.strip("/").split("/")
    current = ""
    for d in dirs:
        current += "/" + d
        try:
            sftp.stat(current)
        except IOError:
            sftp.mkdir(current)

def upload_to_ec2(file_bytes, subfolder, file_name):
    ssh, sftp = ssh_connect()
    try:
        remote_dir = os.path.join(EC2_UPLOAD_PATH, subfolder)
        ensure_remote_dir(sftp, remote_dir)
        remote_path = os.path.join(remote_dir, file_name)
        with sftp.file(remote_path, "wb") as f:
            f.write(file_bytes)
        return f"{EC2_PUBLIC_URL}{subfolder}/{file_name}"
    finally:
        sftp.close()
        ssh.close()

# =============================
# 💾 BASE DE DATOS
# =============================

def find_product_in_db(model_data, file_name):
    product_code = file_name.split("_")[0].split(".")[0].strip()
    tabla = model_data["tabla"]

    query = f'SELECT id, "Nombre", "Modelo" FROM "{tabla}" WHERE "Modelo" ILIKE %s LIMIT 1;'
    param = f"%{product_code}%"
    write_log(f"🔍 Ejecutando query:\n{query}\n➡️ Parámetro: {param}")

    try:
        with conn.cursor() as cur:
            cur.execute(query, (param,))
            row = cur.fetchone()
            if row:
                write_log(f"✅ Coincidencia encontrada en {tabla}: id={row[0]}, nombre={row[1]}")
                return row
            else:
                write_log(f"⚠️ No se encontró producto '{product_code}' en tabla {tabla}")
                return None
    except Exception as e:
        write_log(f"❌ Error ejecutando SELECT en {tabla}: {e}")
        return None

def save_files_into_db(data):
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM "CatalogoFotos" WHERE sku=%s;', (data["sku"],))
            result = cur.fetchone()
            if result:
                id_catalogo = result[0]
                write_log(f"📸 SKU existente en CatalogoFotos: {data['sku']} (id={id_catalogo})")
            else:
                cur.execute('INSERT INTO "CatalogoFotos" (sku, model, created_at) VALUES (%s, %s, NOW()) RETURNING id;', (data["sku"], data["model"]))
                id_catalogo = cur.fetchone()[0]
                write_log(f"🆕 Insertado en CatalogoFotos: {data['sku']} (id={id_catalogo})")

            cur.execute('''
                INSERT INTO "DetalleFotos" 
                (type, "fileName", url, product, sku, size, model, "idCatalogoFotos", created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW());
            ''', (data["type"], data["fileName"], data["url"], data["product"], data["sku"], data["size"], data["model"], id_catalogo))
            conn.commit()
            write_log(f"✅ Insertado en DetalleFotos: {data['fileName']}")
            return id_catalogo
    except Exception as e:
        conn.rollback()
        write_log(f"❌ Error al insertar en BD ({data['fileName']}): {e}")
        return None


def verify_and_update_product(data, model_data):
    """Verifica el SKU en la tabla del producto y actualiza fotosId"""
    tabla = model_data["tabla"]
    sku = data["sku"]
    try:
        with conn.cursor() as cur:
            # Verificar si el SKU existe en la tabla del producto
            cur.execute(f'SELECT sku FROM "{tabla}" WHERE sku=%s;', (sku,))
            result = cur.fetchone()
            id_foto = save_files_into_db(data)

            if result:
                # Actualizar el campo fotosId del producto
                update_sql = f'UPDATE "{tabla}" SET "fotosId"=%s WHERE sku=%s;'
                cur.execute(update_sql, (id_foto, sku))
                conn.commit()
                write_log(f"🖼️ Actualizado {tabla}.fotosId={id_foto} para SKU={sku}")
            else:
                write_log(f"⚠️ SKU '{sku}' no encontrado en {tabla}. Se insertó foto pero no se actualizó producto.")
    except Exception as e:
        conn.rollback()
        write_log(f"❌ Error actualizando fotosId en {tabla} para SKU={sku}: {e}")

# =============================
# 🚀 PROCESAMIENTO
# =============================

def process_folder(folder_id):
    items = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,parents)"
    ).execute().get("files", [])

    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            process_folder(item["id"])
        else:
            file_id = item["id"]
            file_name = item["name"]
            parents = item.get("parents", [])
            drive_path = build_drive_path(file_id)

            write_log(f"📁 Archivo: {file_name} | Ruta completa en Drive: {drive_path}")

            model_data = None
            if parents:
                model_data = resolve_model(parents[0])

            if not model_data:
                write_log(f"⚠️ Sin modelo detectado → {file_name} | Ruta: {drive_path}")
                continue

            product_row = find_product_in_db(model_data, file_name)
            if not product_row:
                write_log(f"⚠️ Saltado (sin coincidencia en BD): {file_name}")
                continue

            try:
                write_log(f"📸 Descargando {file_name} (modelo {model_data['model']}, tabla {model_data['tabla']})")
                file_content = service.files().get_media(fileId=file_id).execute()

                img = Image.open(io.BytesIO(file_content))
                png_bytes = io.BytesIO()
                img.save(png_bytes, format="png", optimize=True)
                png_bytes.seek(0)

                subfolder = model_data["model"]
                url = upload_to_ec2(png_bytes.getvalue(), subfolder, file_name.replace(".jpg", ".png"))

                data = {
                    "type": "png",
                    "fileName": file_name,
                    "url": url,
                    "product": product_row[1] or product_row[2],
                    "sku": file_name.split("_")[0],
                    "size": "lg",
                    "model": model_data["model"],
                    "tabla": model_data["tabla"]
                }

                verify_and_update_product(data, model_data)

            except Exception as e:
                write_log(f"❌ Error procesando {file_name}: {e}")

# =============================
# MAIN
# =============================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["new", "add"])
    args = parser.parse_args()

    write_log(f"🚀 Iniciando proceso {args.action.upper()}")
    process_folder(folder_id)
    write_log("🎯 Proceso completado correctamente")

if __name__ == "__main__":
    main()
