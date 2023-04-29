import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import boto3
import io
from PIL import Image

# Cargar variables de entorno desde el archivo .env
load_dotenv()

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

# ID de la carpeta compartida en Google Drive
#folder_id = "1K8xtUbGwcO3yLbcleGVf9eP0nHUYx8w4"
folder_id = os.environ["FOLDER_ID"]
bucket_name = os.environ["BUCKET_NAME"]

#AWS_ACCESS_KEY_ID= os.environ["AWS_ACCESS_KEY_ID"]
#AWS_SECRET_ACCESS_KEY= os.environ["AWS_SECRET_ACCESS_KEY"]
# Configurar cliente de S3 y obtener URL pre-firmada
s3_client = boto3.client('s3')


# Crear una instancia de la API de Google Drive
service = build("drive", "v3", credentials=creds)

def list_files_in_folder(folder_id, archivos):
    """Lista todos los archivos y carpetas en la carpeta especificada en Google Drive"""
    archivosinternos=0
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="nextPageToken, files(name, id, mimeType)").execute()
    items = results.get("files", [])
    for item in items:
        if item["mimeType"] == "application/vnd.google-apps.folder":
            print(f'Entrando a carpeta una carpeta con ID: {item["name"]}')
            archivos= archivos + list_files_in_folder(item["id"], archivos)
        else:
            print(f'Descargando el archivo: {item["name"]}')
            file_id = item['id']
            file_name = item['name']
            print(f' el archivo: {item["name"]}  tiene un mimetype : {item["mimeType"]}')
            file_ext = file_name.split('.')[-1].lower()  # Obtener la extensión del archivo
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
            print(f'aqui esta el resultado: {url}')
            archivos += 1
            print(f'Archivo {item["name"]} subido a S3.')
    return archivosinternos



# Ejecutar la función para listar los archivos en la carpeta compartida

def main():
    archivos = 0
    archivos = list_files_in_folder(folder_id,  archivos )
    print(f'Se pocesaron: {archivos} archivos')

if __name__ == "__main__":
    main()
