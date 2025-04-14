import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import boto3
import io
from PIL import Image
import psycopg2
import argparse
import datetime
import uuid

# Cargar variables de entorno desde el archivo .env
load_dotenv()


short_id= str(uuid.uuid4())
uuid_log = short_id[:4]
archivo_procesar = ""
# Obtener credenciales de la cuenta de servicio desde las variables de entorno
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
bucket_name = os.environ["BUCKET_NAME"]
s3_client = boto3.client('s3')
models_catalog_str= [
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


models_catalog = models_catalog_str

# Crear una instancia de la API de Google Drive
service = build("drive", "v3", credentials=creds)


# Establecer la conexión con la base de datos
conn = psycopg2.connect(
    host= os.environ["DB_HOST"],
    database= os.environ["DB_NAME"],
    user= os.environ["DB_USER"],
    password= os.environ["DB_PASSWORD"],
    port=os.environ.get("DB_PORT", "5432")  # Agrega el puerto con valor predeterminado 5432
)


def list_files_in_folder(folder_id, archivos, parent="", product="" ):
    print(f"Entrando a la funcion list_files_in_folder con folder_id: {folder_id} y parent: {parent}")
    """Lista todos los archivos y carpetas en la carpeta especificada en Google Drive"""
    archivosinternos=0
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="nextPageToken, files(parents,name, id, mimeType)").execute()
    items = results.get("files", [])

    parents = []
    for item in items:
        validate_model(item["name"], item["id"])
   
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            print(f'Entrando a carpeta : {item["name"]}')
            archivos= archivos + list_files_in_folder(item["id"], archivos, item['parents'][0], item["name"])
        else:
            file_id = item['id']
            file_name = item['name']
            file_name = file_name.replace('_Ig.', '_lg.')
            #validamos la estructura 
            try:
                # Obtener el contenido del archivo y convertir a png
                file_content = service.files().get_media(fileId=file_id).execute()
                img = Image.open(io.BytesIO(file_content))
                png_content = io.BytesIO()
                img.save(png_content, format='png', optimize=True)
                png_content.seek(0)
                # Cargar el archivo png en S3
                s3_file_key = f'{bucket_name}/{file_name.split(".")[0]}.png'  # Cambiar la extensión del archivo a png
                s3_client.upload_fileobj(png_content, bucket_name, s3_file_key, ExtraArgs={'ACL': 'public-read'}) 
                url=f'https://{bucket_name}.s3.amazonaws.com/{s3_file_key}'
                archivos += 1
                print(f'Archivo {item["name"]} subido a S3.')
                # Obtengo el id de la carpeta padre
                model= get_model(parent)
                print(f'Descargando el archivo: {file_name}')
                size = file_name.split("_")[2].split(".")[0]
                sku = file_name.split("_")[0]
                id_image = file_name.split("_")[1].split(".")[0]
                data = {
                    'type':s3_file_key.split(".")[1],
                    'fileName':file_name,
                    'url':url,
                    'product':model[1],
                    'sku': sku,
                    'size': size,
                    'model': model[0],
                    'tabla': model[2],
                    'id_image': id_image,
                }
                verify_data(data)
            except (Exception) as error:
                log = f' el archivo : {file_name} genero el error: {error} '
                write_log(log)
                print(log)
    return archivosinternos

'''
* Se debe hacer catalogo de tamaños
** Plus
Crear imagenes tamaño md y small
'''

def validate_model(modelName, modelId):
    for i in range(len(models_catalog)):
        if models_catalog[i]['name'] == modelName:
            models_catalog[i]['google_id'] = modelId
            break

def get_model(parentid):
    for i in range(len(models_catalog)):
        valor=models_catalog[i]['google_id']
        if models_catalog[i]['google_id'] == parentid:
            #return models_catalog[i]['model']
            data = [models_catalog[i]['model'], models_catalog[i]['name'], models_catalog[i]['tabla']]
            return data
            break

def verify_data(data):
    print(f'validando sku : {data["sku"]}')  
    tabla = data["tabla"]
    cadena = f'select sku from "{tabla}" where sku=\'{data["sku"]}\';' 
    try:
        # busco el sku en el modelo 
        cur = conn.cursor()
        cur.execute(cadena) 
        result = cur.fetchone() 
        if(result):
            sku = result[0]
            #print(f' se ejecutara para el sku : {sku}')
            id_foto = save_files_into_db(data)
            #print(f' id_foto encontrado : {id_foto}')
            cadena = f'UPDATE "{tabla}" SET "fotosId"={id_foto}  where sku=\'{data["sku"]}\';' 
            print(f' ejecutando : {cadena}')
            cur.execute(cadena)
        else:
            id_foto = save_files_into_db(data)
            print(f' id_foto procesado : {id_foto}')
            log = f' sku NO encontrado : {data["sku"]} en el producto: {data["product"]} en la tabla: {tabla} '
            write_log(log)
            print(f' sku NO encontrado : {data["sku"]} en el producto: {data["product"]} en la tabla: {tabla} ')
            
        #cur.execute('INSERT INTO "DetalleFotos" ( type, "fileName", url, product, sku, size, model, created_at) VALUES (%s, %s ,%s, %s ,%s, %s, %s , NOW());', (data["type"], data["fileName"], data["url"], data["product"], data["sku"], data["size"] ,data["model"]))
        conn.commit()  
        cur.close()       
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cur.close() 
        log = f' verificando si existe el archivo : {data["sku"]} genero el error: {error} '
        write_log(log)
        print(log)

def save_files_into_db(data):
    """Lista todos los archivos y carpetas en la carpeta especificada en Google Drive"""
    try:
        # inserto en tabla CatalogoFotos y DetalleFotos
        cur = conn.cursor()
        #print(f' ejecutando : ')
        #print('select id from "CatalogoFotos" where sku=%s;', (data["sku"],))
        # Validamos si el sku ya existe en la tabla CatalogoFotos
        cur.execute('select id from "CatalogoFotos" where sku=%s;', (data["sku"],))
        result = cur.fetchone()
        if result:  # Verifica si result no es None
            id_catalogo = result[0]
            cur.execute('INSERT INTO "DetalleFotos" ( type, "fileName", url, product, sku, size, model, "idCatalogoFotos", created_at) VALUES (%s, %s ,%s, %s ,%s, %s, %s, %s , NOW());', (data["type"], data["fileName"], data["url"], data["product"], data["sku"], data["size"] ,data["model"], id_catalogo))
            conn.commit()  
            cur.close()
            return id_catalogo  
        else:
            cur.execute('INSERT INTO "CatalogoFotos" (sku, model, created_at) VALUES (%s, %s , NOW()) RETURNING id ;', (data["sku"], data["model"]))    
            # Obtener el ID del registro insertado
            id_insertado = cur.fetchone()[0]
            cur.execute('INSERT INTO "DetalleFotos" ( type, "fileName", url, product, sku, size, model, "idCatalogoFotos", created_at) VALUES (%s, %s ,%s, %s ,%s, %s, %s, %s , NOW());', (data["type"], data["fileName"], data["url"], data["product"], data["sku"], data["size"] ,data["model"], id_insertado))
            conn.commit()  
            cur.close()  
            return id_insertado    
        print(f' id_catalogo es : {id_catalogo}')
    except (Exception, psycopg2.DatabaseError) as error:
        log = f' salvando en la BD la informacion del sku : {data["sku"]} genero el error: {error} '
        write_log(log)
        print(log)
    


def add_files_in_folder(folder_id, archivos, parent="", product="", name="" ):
    """Lista todos los archivos y carpetas en la carpeta especificada en Google Drive"""
    archivosinternos=0
    query = f"'{folder_id}' in parents and trashed = false"
    page_token = None
    items = []
    while True:
        results = service.files().list(
            q=query,
            fields="nextPageToken, files(parents,name, id, mimeType)",
            pageToken=page_token
        ).execute()
        
        items += results.get("files", [])
        page_token = results.get("nextPageToken")

        if not page_token:
            break


    '''
    results = service.files().list(q=query, fields="nextPageToken, files(parents,name, id, mimeType)").execute()
    items = results.get("files", [])
       '''
    parents = []
    num_elementos = len(items)
    log = f"La carpeta tiene {num_elementos} elementos."
    #write_log(log)
    print(log)
 
    for item in items:
        validate_model(name, folder_id)
   
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            print(f'Entrando a carpeta : {item["name"]}')
            archivos= archivos + add_files_in_folder(item["id"], archivos, item['parents'][0], item["name"])
        else:
            file_id = item['id']
            file_name = item['name']
            file_name = file_name.replace('_Ig.', '_lg.')
            # Validar si la foto ya existe en AWS S3
            s3_file_key = f'{bucket_name}/{file_name.split(".")[0]}.png'

            #validamos la estructura 
            try:
                model= get_model(parent)
                if model is None or not model:
                    log = "El modelo no fue encontrado, se debe agregar al arreglo models_catalog_str saliendo del ciclo..."
                    write_log(log)
                    print(log)
                    return 0
                # Obtener el contenido del archivo y convertir a png
                file_content = service.files().get_media(fileId=file_id).execute()
                img = Image.open(io.BytesIO(file_content))
                png_content = io.BytesIO()
                img.save(png_content, format='png', optimize=True)
                png_content.seek(0)
                # Cargar el archivo png en S3
                s3_file_key = f'{bucket_name}/{file_name.split(".")[0]}.png'  # Cambiar la extensión del archivo a png
                s3_client.upload_fileobj(png_content, bucket_name, s3_file_key, ExtraArgs={'ACL': 'public-read'}) 
                url=f'https://{bucket_name}.s3.amazonaws.com/{s3_file_key}'
                archivos += 1
                #print(f'Archivo {item["name"]} subido a S3.')
                print(f'Archivo {file_name} subido a S3.')
                # Obtengo el id de la carpeta padre
                model= get_model(parent)
                size = file_name.split("_")[2].split(".")[0]
                sku = file_name.split("_")[0]
                id_image = file_name.split("_")[1].split(".")[0]
                #print(f'Se manda s3_key: {s3_file_key.split(".")[1]}  size: {size}   sku: {sku}   id_image:  {id_image} subido a S3.')
                #print(f'Se manda model[0]: {model[0]}  model[1]: {model[1]}   model[2]: {model[2]} subido a S3.')
                #print(f'Se manda model: {model}')

                data = {
                    'type':s3_file_key.split(".")[1],
                    'fileName':file_name,
                    'url':url,
                    'product':model[1],
                    'sku': sku,
                    'size': size,
                    'model': model[0],
                    'tabla': model[2],
                    'id_image': id_image,
                }
                #print(f'Se manda data: {data}')
                verify_data(data)
                log = f' el archivo : {file_name} se ha cargado correctamente '
                write_log(log)
                print(log)
            except (Exception) as error:
                log = f' el archivo : {file_name} genero el error: {error} '
                write_log(log)
                print(log)
    return archivosinternos



def get_folders(folder_id):

    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="nextPageToken, files(parents,name, id, mimeType)").execute()
    items = results.get("files", [])
    i=0
    data=[]
    #print(f"Procesando {folder_id} con {items} archivos")

    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            print(f" {i+1} .- {item['name']}")
            file = {
                    'id':i+1,
                    'uuid':item["id"],
                    'name': item["name"]
                }
            data.append(file)
            i=i+1
    return data



def write_log(cadena):
    global archivo_procesar
    fecha = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nombre_archivo = 'logs/log_' + fecha[:10] + "-" + archivo_procesar.replace(" ", "") + "-" + uuid_log + '.txt'  
    # crea la carpeta si no existe
    os.makedirs(os.path.dirname(nombre_archivo), exist_ok=True) 
    with open(nombre_archivo, 'a') as archivo:
        archivo.write(fecha + ': ' + cadena + '\n')

def truncate_tables():
    print("Truncando tablas CatalogoFotos y DetalleFotos y eliminando carpeta de s3  ")
    cur = conn.cursor()
    try:
        cur.execute('TRUNCATE TABLE "CatalogoFotos"')
        conn.commit()
        cur.execute('TRUNCATE TABLE "DetalleFotos"')
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        log = f' problema al truncar las tablas : {error} '
        write_log(log)
        print(log)
"""
    try:
        eliminar_carpeta_s3(bucket_name, "solucionesparatuauto")
    except (Exception) as error:
        log = f' problema al eliminar la carpeta : {bucket_name} en s3 : {error} '
        write_log(log)
        print(log)
"""




def eliminar_carpeta_s3(bucket_name, carpeta_a_eliminar):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=carpeta_a_eliminar)
    for obj in response['Contents']:
        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
    s3_client.delete_object(Bucket=bucket_name, Key=carpeta_a_eliminar)


def main():
    global archivo_procesar
    # Definir los argumentos que puede recibir el script
    parser = argparse.ArgumentParser(description='Este script Toma imagenes cargadas en una carpeta de google drive y las sube a s3 ')
    parser.add_argument('action', type=str, choices=['new', 'add'], help='Acción a realizar')
    args = parser.parse_args()
    archivos = 0
    if args.action == 'new':
        print('Se van a truncar las tablas de fotos y se cargaran todas nuevamente .... ')
        truncate_tables()
        archivos = list_files_in_folder(folder_id,  archivos )

    if args.action == 'add':
        print('Se van a agregar las nuevas fotos a las tablas .... ')
        print(f'this is an aplication that  walks through the product files')
        print(f'please select the file')
        print(f' A .- All')
        files = get_folders(folder_id)
        idFile = input("Please select an option : ")
        print(f'You selected {idFile}')
        if idFile == 'A':
            for file in files:
                print(f'Entrando a la carpeta : {file["name"]}')
                #archivos = archivos + add_files_in_folder(file["uuid"],  archivos )
        else:
            file = files[int(idFile)-1]
            archivo_procesar=file["name"]
            print(f'Entrando a la carpeta : {file["name"]}')
            archivos = add_files_in_folder(file["uuid"],  archivos, name = file["name"] )

        #clear_screen()
        #archivos = add_files_in_folder(folder_id,  archivos )
    print(f'Se pocesaron: {archivos} archivos')

if __name__ == "__main__":
    main()
