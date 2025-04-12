import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import psycopg2
import datetime
import uuid
import shortuuid

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Configuración de logging
uuid_log = str(shortuuid.ShortUUID().random(length=6))
now = datetime.datetime.now()
fecha = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
hora = str(now.hour)+'_'+str(now.second)
nombre_archivo = 'logs/google_images/log_'
archivo_uuid = fecha[:10]+ '-' + hora + '-' + uuid_log + '.txt'
archivo_procesar = ""

# Configuración de credenciales de Google
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

# Configuración de Google Drive
folder_id = os.environ["FOLDER_ID"]
service = build("drive", "v3", credentials=creds)

# Conexión a PostgreSQL
conn = psycopg2.connect(
    host=os.environ["DB_HOST"],
    database=os.environ["DB_NAME"],
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"]
)

# Catálogo de modelos
models_catalog = [
    {
        "name": "BASE PARA BOCINA",
        "model": "BasesBocina",
        "tabla":"basesBocina",
        "google_id": ""
    },
    {
        "name": "EPICENTROS",
        "model": "Epicentros",
        "tabla":"Epicentros",
        "google_id": ""
    },
    {
        "name": "SEGURIDAD",
        "model": "Seguridad",
        "tabla":"seguridad",
        "google_id": ""
    },
    {
        "name": "AMPLIFICADORES",
        "model": "Amplificadores",
        "tabla":"Amplificadores",
        "google_id": ""
    },
    {
        "name": "AUTOESTEREOS",
        "model": "Estereos",
        "tabla":"Estereos",
        "google_id": ""
    },
    {
        "name": "ESTEREOS (2022)",
        "model": "Estereos",
        "tabla":"Estereos",
        "google_id": ""
    },
    {
        "name": "BOCINAS",
        "model": "Bocinas",
        "tabla":"Bocinas",
        "google_id": ""
    },
    {
        "name": "TWEETERS",
        "model": "Tweeters",
        "tabla":"Tweeters",
        "google_id": ""
    },
    {
        "name": "FRENTES",
        "model": "Bases",
        "tabla":"Bases",
        "google_id": ""
    },
    {
        "name": "CAJONES",
        "model": "Cajones",
        "tabla":"Cajones",
        "google_id": ""
    },
    {
        "name": "ECUALIZADORES",
        "model": "Ecualizadores",
        "tabla":"Ecualizadores",
        "google_id": ""
    },
    {
        "name": "KIT DE CABLES",
        "model": "KitsCables",
        "tabla":"KitsCables",
        "google_id": ""
    },
    {
        "name": "MEDIOS RANGOS",
        "model": "MediosRangos",
        "tabla":"MediosRangos",
        "google_id": ""
    },
    {
        "name": "PROCESADOR",
        "model": "Procesadores",
        "tabla":"Procesadores",
        "google_id": ""
    },
    {
        "name": "SET DE MEDIOS",
        "model": "SetMedios",
        "tabla":"SetMedios",
        "google_id": ""
    },
    {
        "name": "WOOFERS",
        "model": "Woofers",
        "tabla":"Woofers",
        "google_id": ""
    },
    {
        "name": "ARNESES",
        "model": "Arneses",
        "tabla":"Arneses",
        "google_id": ""
    },
    {
        "name": "ADAPTADORES DE ANTENA",
        "model": "AdaptadoresAntena",
        "tabla":"AdaptadoresAntena",
        "google_id": ""
    },
    {
        "name": "ACCESORIOS DE INSTALACION",
        "model": "Accesorios",
        "tabla":"Accesorios",
        "google_id": ""
    },
    {
        "name": "ADAPTADORES DE IMPEDANCIA",
        "model": "AdaptadoresImpedancia",
        "tabla":"AdaptadoresImpedancia",
        "google_id": ""
    },
    {
        "name": "ILUMINACION",
        "model": "Iluminacion",
        "tabla":"iluminacion",
        "google_id": ""
    },
    {
        "name": "VIDEO",
        "model": "Video",
        "tabla":"video",
        "google_id": ""
    },
    {
        "name": "AUDIO MARINO",
        "model": "AudioMarino",
        "tabla":"audioMarino",
        "google_id": ""
    },
    {
        "name": "SERVICIOS",
        "model": "Servicios",
        "tabla":"servicios",
        "google_id": ""
    },
    {
        "name": "ACCESORIOS DE CAMIONETA",
        "model": "AccesoriosCamioneta",
        "tabla":"accesoriosCamioneta",
        "google_id": ""
    },
    {
        "name": "COMPONENTES",
        "model": "Componentes",
        "tabla":"Componentes",
        "google_id": ""
    }       
]

def write_log(cadena):
    """Escribe en el archivo de log"""
    fecha_actual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    os.makedirs(os.path.dirname(nombre_archivo), exist_ok=True)
    with open(nombre_archivo + archivo_uuid, 'a') as archivo:
        archivo.write(f"{fecha_actual}: {cadena}\n")

def get_google_drive_link(file_id):
    """Genera el enlace público de visualización de Google Drive"""
    return f"https://drive.google.com/uc?id={file_id}"

def validate_model(modelName, modelId):
    """Valida y actualiza el ID de Google Drive para cada modelo"""
    for model in models_catalog:
        if model['name'] == modelName:
            model['google_id'] = modelId
            break

def get_model(parentid):
    """Obtiene la información del modelo basado en el ID del padre"""
    for model in models_catalog:
        if model['google_id'] == parentid:
            return [model['model'], model['name'], model['tabla']]
    return None

def process_image_file(file_id, file_name, parent_id):
    """Procesa un archivo de imagen y guarda su información en la BD"""
    try:
        model = get_model(parent_id)
        if not model:
            write_log(f"Modelo no encontrado para el archivo: {file_name}")
            return False

        # Obtener URL de Google Drive
        url = get_google_drive_link(file_id)
        
        # Procesar nombre del archivo
        file_name = file_name.replace('_Ig.', '_lg.')
        size = file_name.split("_")[2].split(".")[0]
        sku = file_name.split("_")[0]
        id_image = file_name.split("_")[1].split(".")[0]

        # Preparar datos
        data = {
            'type': file_name.split(".")[-1],
            'fileName': file_name,
            'url': url,
            'product': model[1],
            'sku': sku,
            'size': size,
            'model': model[0],
            'tabla': model[2],
            'id_image': id_image,
        }

        # Guardar en base de datos
        save_to_database(data)
        write_log(f"Archivo procesado exitosamente: {file_name}")
        return True

    except Exception as error:
        write_log(f"Error procesando archivo {file_name}: {str(error)}")
        return False

def save_to_database(data):
    """Guarda la información de la imagen en la base de datos y actualiza la tabla del producto"""
    try:
        cur = conn.cursor()
        
        # Verificar si existe el SKU en CatalogoFotos
        cur.execute('SELECT id FROM "CatalogoFotos" WHERE sku = %s', (data["sku"],))
        result = cur.fetchone()
        
        if result:
            id_catalogo = result[0]
        else:
            # Insertar nuevo registro en CatalogoFotos
            cur.execute(
                'INSERT INTO "CatalogoFotos" (sku, model, created_at) VALUES (%s, %s, NOW()) RETURNING id',
                (data["sku"], data["model"])
            )
            id_catalogo = cur.fetchone()[0]

        # Insertar en DetalleFotos
        cur.execute(
            'INSERT INTO "DetalleFotos" (type, "fileName", url, product, sku, size, model, "idCatalogoFotos", created_at) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())',
            (data["type"], data["fileName"], data["url"], data["product"], 
             data["sku"], data["size"], data["model"], id_catalogo)
        )
        
        # Actualizar fotosId en la tabla del producto
        update_query = f'UPDATE "{data["tabla"]}" SET "fotosId" = %s WHERE sku = %s'
        cur.execute(update_query, (id_catalogo, data["sku"]))
        
        conn.commit()
        cur.close()
        write_log(f"Datos guardados correctamente para SKU: {data['sku']} con ID de catálogo: {id_catalogo}")
        
    except Exception as error:
        conn.rollback()
        write_log(f"Error guardando en base de datos: {str(error)}")
        raise

def process_folder(folder_id, parent_id="", name=""):
    """Procesa recursivamente una carpeta de Google Drive"""
    try:
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, mimeType, parents)"
        ).execute()
        
        items = results.get("files", [])
        write_log(f"Procesando carpeta: {name} ({len(items)} elementos)")

        # Validar modelo si es una carpeta principal
        if name:
            validate_model(name, folder_id)

        for item in items:
            if item["mimeType"] == "application/vnd.google-apps.folder":
                # Procesar subcarpeta
                process_folder(item["id"], item.get('parents', [None])[0], item["name"])
            else:
                # Procesar archivo
                process_image_file(item["id"], item["name"], parent_id)

    except Exception as error:
        write_log(f"Error procesando carpeta {name}: {str(error)}")

def main():
    try:
        write_log("Iniciando proceso de sincronización de imágenes")
        
        # Obtener carpetas principales
        query = f"'{folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType)"
        ).execute()
        
        folders = [f for f in results.get("files", []) 
                  if f["mimeType"] == "application/vnd.google-apps.folder"]

        print("Carpetas disponibles:")
        for i, folder in enumerate(folders, 1):
            print(f"{i}. {folder['name']}")
        print("A. Procesar todas las carpetas")

        option = input("Seleccione una opción: ")

        if option.upper() == 'A':
            write_log("Procesando todas las carpetas")
            for folder in folders:
                process_folder(folder["id"], name=folder["name"])
        elif option.isdigit() and 1 <= int(option) <= len(folders):
            folder = folders[int(option)-1]
            write_log(f"Procesando carpeta: {folder['name']}")
            process_folder(folder["id"], name=folder["name"])
        else:
            print("Opción no válida")

        write_log("Proceso completado")

    except Exception as error:
        write_log(f"Error en el proceso principal: {str(error)}")
    finally:
        conn.close()

if __name__ == "__main__":
    main() 