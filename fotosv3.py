import os
import io
import sys
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
os.makedirs("logs", exist_ok=True)
archivo_log = f"logs/log_{datetime.datetime.now().strftime('%Y-%m-%d')}-fotosv3-{short_id}.txt"

def write_log(msg: str):
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{fecha}] {msg}"
    with open(archivo_log, "a") as f:
        f.write(line + "\n")
    sys.stdout.write(line + "\n")
    sys.stdout.flush()

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

service = build("drive", "v3", credentials=creds)
conn = psycopg2.connect(SUPABASE_CONN)

# =============================
# 🔧 FUNCIONES AUXILIARES
# =============================

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

def save_files_into_db(data):
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT id FROM "CatalogoFotos" WHERE sku=%s;', (data["sku"],))
            result = cur.fetchone()
            if result:
                id_catalogo = result[0]
            else:
                cur.execute('INSERT INTO "CatalogoFotos" (sku, model, created_at) VALUES (%s, %s, NOW()) RETURNING id;', (data["sku"], data["model"]))
                id_catalogo = cur.fetchone()[0]

            cur.execute('''
                INSERT INTO "DetalleFotos" 
                (type, "fileName", url, product, sku, size, model, "idCatalogoFotos", created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW());
            ''', (data["type"], data["fileName"], data["url"], data["product"], data["sku"], data["size"], data["model"], id_catalogo))
            conn.commit()
            return id_catalogo
    except Exception as e:
        conn.rollback()
        write_log(f"❌ Error BD: {e}")
        return None

# =============================
# 🚀 PROCESAMIENTO
# =============================

def process_folder(folder_id, model_data):
    items = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,parents)"
    ).execute().get("files", [])

    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            process_folder(item["id"], model_data)
        else:
            file_id = item["id"]
            file_name = item["name"]
            try:
                file_content = service.files().get_media(fileId=file_id).execute()
                img = Image.open(io.BytesIO(file_content))
                png_bytes = io.BytesIO()
                img.save(png_bytes, format="png", optimize=True)
                png_bytes.seek(0)

                url = upload_to_ec2(png_bytes.getvalue(), model_data["model"], file_name.replace(".jpg", ".png"))

                data = {
                    "type": "png",
                    "fileName": file_name,
                    "url": url,
                    "product": file_name,
                    "sku": file_name.split("_")[0],
                    "size": "lg",
                    "model": model_data["model"],
                    "tabla": model_data["tabla"]
                }

                save_files_into_db(data)
                write_log(f"✅ Subido y guardado: {file_name}")
            except Exception as e:
                write_log(f"❌ Error procesando {file_name}: {e}")

# =============================
# 🧠 SELECCIÓN DE MODELO
# =============================

def choose_model():
    print("\n=== Selecciona el modelo que quieres procesar ===\n")
    for i, m in enumerate(models_catalog, start=1):
        print(f"{i}. {m['name']} → {m['model']}")
    print("0. Cancelar\n")

    while True:
        try:
            choice = int(input("👉 Ingresa el número del modelo a procesar: "))
            if choice == 0:
                print("❌ Operación cancelada.")
                exit(0)
            if 1 <= choice <= len(models_catalog):
                selected = models_catalog[choice - 1]
                print(f"\n✅ Seleccionado: {selected['name']} ({selected['model']})")
                return selected
            else:
                print("⚠️ Opción inválida, intenta de nuevo.")
        except ValueError:
            print("⚠️ Ingresa un número válido.")

def find_drive_folder_for_model(model_name):
    write_log(f"🔍 Buscando carpeta en Drive para modelo: {model_name}")
    query = f"name = '{model_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    folders = results.get("files", [])

    if not folders:
        write_log(f"❌ No se encontró carpeta en Drive para '{model_name}'")
        return None

    folder_id = folders[0]["id"]
    write_log(f"📂 Carpeta encontrada: {folders[0]['name']} (ID={folder_id})")
    return folder_id

# =============================
# MAIN
# =============================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["new", "add"])
    args = parser.parse_args()

    print("🚀 Script iniciado correctamente.")
    write_log(f"🚀 Iniciando proceso {args.action.upper()}")

    selected_model = choose_model()
    model_folder_id = find_drive_folder_for_model(selected_model["name"])

    if not model_folder_id:
        write_log("⚠️ No se encontró la carpeta del modelo en Google Drive.")
        return

    process_folder(model_folder_id, selected_model)
    write_log(f"🎯 Proceso completado para {selected_model['name']}")

if __name__ == "__main__":
    main()
