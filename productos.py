import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
import pandas as pd
import psycopg2

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

folder_id = os.environ["FOLDER_PRODUCTS_ID"]

# Crear una instancia de la API de Google Drive
service = build("drive", "v3", credentials=creds)

# Establecer la conexión con la base de datos
conn = psycopg2.connect(
    host= os.environ["DB_HOST"],
    database= os.environ["DB_NAME"],
    user= os.environ["DB_USER"],
    password= os.environ["DB_PASSWORD"]
)


def get_files(folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="nextPageToken, files(parents,name, id, mimeType)").execute()
    items = results.get("files", [])
    i=0
    data=[]
    #print(f" recorriendo {folder_id}")
    #print(f" recorriendo {items}")
    
    for item in items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            print(f" {i+1} .- {item['name']},  el nombre del id es: {item['id']}")
            #filesArray[i][item["id"], item["name"]]
            file = {
                    'id':i+1,
                    'uuid':item["id"],
                    'name': item["name"]
                }
            data.append(file)
            i=i+1
    return data


def validate_folder(id, folderobject):
    response=False
    if(id.isnumeric()):
       print('validando...')
    else: 
        return False 
    for i in range(len(folderobject)): 
        if int(id) == folderobject[i]['id']:
           response=True
           return response
    return response

def validate_file(idFile, files):
    file = get_file_object(idFile, files)
    # prompt utilizado como lees archivos de google sheets en python para validar si pueden ser insertados en una bd postgresql


def get_file_object(idFile, files):
    print(f'Validar {idFile}')


def main():
    print(f'this is an aplication that  walks through the product files')
    print(f'please select the file')
    print(f' A .- All')
    files = get_files(folder_id)
    #print(f'files contiene: {files}')
    idFile = input("Please select an option : ")
    if str(idFile) == 'A' or str(idFile) == 'a' or validate_folder(idFile, files):
        print(f'What do you want to do:')
        print(f'1.-Validate')
        print(f'2.-Validate / Insert')
        option = input("Please select an option: ")
        if option.isnumeric():
            if int(option) == 1 and (str(idFile) == 'A' or str(idFile) == 'a'):
                print('Validar todos')
                #validate_file()
            if int(option) == 1 and ( str(idFile) != 'A' and str(idFile) != 'a'):
                validate_file(idFile, files)
            if int(option) == 2 and ( str(idFile) == 'A' or str(idFile) == 'a' ):
                print('Validar y aplicar todos')
                #validate_file()
            if int(option) == 2 and ( str(idFile) != 'A' and str(idFile) != 'a' ):
                print(f'Validar y aplicar {idFile}')
        else:
            print(f'Opcion no valida')
    else:
        print(f'Opcion no valida')


if __name__ == "__main__":
    main()
