import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import psycopg2
import datetime
import shortuuid

# Cargar variables de entorno desde el archivo .env
load_dotenv()

json_credenciales = {
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
}

# Obtener credenciales de la cuenta de servicio desde las variables de entorno
creds = service_account.Credentials.from_service_account_info(json_credenciales)

scope = ["https://spreadsheets.google.com/feeds",
         "https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]
credsSheets = ServiceAccountCredentials.from_json_keyfile_dict(json_credenciales, scope)
client = gspread.authorize(creds)

folder_id = os.environ["FOLDER_CONFIGURADOR_TABLES_ID"]

# Crear una instancia de la API de Google Drive
service = build("drive", "v3", credentials=creds)

# Establecer la conexión con la base de datos
conn = psycopg2.connect(
    host= os.environ["DB_HOST"],
    database= os.environ["DB_NAME"],
    user= os.environ["DB_USER"],
    password= os.environ["DB_PASSWORD"]
)


#Generamos la informacion del archivo de logs 

uuid_log= str(shortuuid.ShortUUID().random(length=6))
now = datetime.datetime.now()
fecha = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
hora =  str(now.hour)+'_'+str(now.second)

nombre_archivo = 'logs/configuradortablas/log_configuradortablas_' 
archivo_uuid= fecha[:10]+ '-' + hora + '-' + uuid_log + '.txt' 


configuradortablas_catalog_str= [
    {
        "name": "PROCESADOR para csv",
        "table":"Procesadores",
    },{
        "name": "AMPLIFICADORES para csv",
        "table":"Amplificadores",
    },{
        "name": "AMPLIFICADORES 3 EN 1 para csv",
        "table":"Amplificadores3en1",
    },{
        "name": "Estereos (2022) para csv",
        "table":"Estereos",
    },{
        "name": "ACCESORIOS DE INSTALACIÓN para csv",
        "table":"Accesorios",
    },{
        "name": "BOCINAS para csv",
        "table":"Bocinas",
    }]






def clear_screen():
    # Comprueba si el sistema operativo es Windows
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')


def main():
    global nombre_archivo
    clear_screen()
    print(f'this is an aplication that  walks through the configurator tables files')
    print(f'processing ...')
    files = process_file(folder_id)

if __name__ == "__main__":
    main()