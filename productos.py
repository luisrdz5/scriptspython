import os
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import psycopg2

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
         "https://www.googleapis.com/auth/drive"]
credsSheets = ServiceAccountCredentials.from_json_keyfile_dict(json_credenciales, scope)
client = gspread.authorize(creds)

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


products_catalog_str= [
    {
        "name": "PROCESADOR para csv",
        "model": "BasesBocina",
        "table":"Procesadores",
    },{
        "name": "AMPLIFICADORES para csv",
        "model": "BasesBocina",
        "table":"Amplificadores",
    },{
        "name": "Estereos (2022) para csv",
        "model": "BasesBocina",
        "table":"Estereos",
    },{
        "name": "ACCESORIOS DE INSTALACIÓN para csv",
        "model": "BasesBocina",
        "table":"Accesorios",
    },{
        "name": "BOCINAS para csv",
        "model": "BasesBocina",
        "table":"Bocinas",
    },{
        "name": "EPICENTROS para csv",
        "model": "BasesBocina",
        "table":"Epicentros",
    },{
        "name": "TWEETERS para csv (2022)",
        "model": "BasesBocina",
        "table":"Tweeters",
    },{
        "name": "BASES para csv",
        "model": "BasesBocina",
        "table":"Bases",
    },{
        "name": "BASES PARA BOCINA para csv",
        "model": "BasesBocina",
        "table":"basesBocina",
    },{
        "name": "SET DE MEDIOS para csv",
        "model": "BasesBocina",
        "table":"SetMedios",
    },{
        "name": "ADAPTADORES DE IMPEDANCIA para csv",
        "model": "BasesBocina",
        "table":"AdaptadoresImpedancia",
    },{
        "name": "ADAPTADORES ANTENA para csv",
        "model": "BasesBocina",
        "table":"AdaptadoresAntena",
    },{
        "name": "MEDIOS RANGOS para csv",
        "model": "BasesBocina",
        "table":"MediosRangos",
    },{
        "name": "WOOFERS para csv",
        "model": "BasesBocina",
        "table":"Woofers",
    },{
        "name": "CAJONES para csv",
        "model": "BasesBocina",
        "table":"Cajones",
    },{
        "name": "KIT DE CABLES para csv",
        "model": "BasesBocina",
        "table":"KitsCables",
    },{
        "name": "ARNESES para csv",
        "model": "BasesBocina",
        "table":"Arneses",
    },{
        "name": "ECUALIZADORES para csv",
        "model": "BasesBocina",
        "table":"Ecualizadores",
    },
                   
]









def get_files(folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="nextPageToken, files(parents,name, id, mimeType)").execute()
    items = results.get("files", [])
    i=0
    data=[]
    
    for item in items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            print(f" {i+1} .- {item['name']},  el nombre del id es: {item['id']}")
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
    try:
        file = get_file_object(idFile, files)
        client = gspread.authorize(credsSheets)
        #id_archivo=file["uuid"]
        nombre_archivo=file["name"]
        print(f'validando : {nombre_archivo}')        
        fileSheet = client.open(nombre_archivo).get_worksheet(0)
        #print(f'fileSheet es {fileSheet}')
        #hoja_calculo = fileSheet.sheet1
        tableSheet = get_table_sheet(nombre_archivo)
        print (f'tablesheet es: {tableSheet["table"]}')
        filas = fileSheet.get_all_values()
        #print(f'filas es: {filas}')
        df = pd.DataFrame(filas[1:], columns=filas[0]) 
        validate = validar_dataframe(df, tableSheet["table"] )
        if validate :
            print (f"Ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        else:
            print (f"No ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")

        return validate

        #for index, fila in enumerate(filas):
            # Realizar acciones con cada fila
        #    if index != 0:
        #        print(fila)
    except (Exception) as error:
        print(error)


    # prompt utilizado como lees archivos de google sheets en python para validar si pueden ser insertados en una bd postgresql


def validar_dataframe(df, table ):
    error = False
    try:
        # Realiza las validaciones en el DataFrame
        # Puedes agregar aquí tu lógica de validación
        cursor = conn.cursor()
        # Ejemplo: Verifica si todas las columnas existen en la tabla de la base de datos
        query= f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}'"
        print(f"query es: {query}")
        cursor.execute(query)
        columnas_tabla = [registro[0] for registro in cursor.fetchall()]
        #print(f"Se encontraron estas columnas en la tabla: {columnas_tabla}")

        
        # Realiza la validación fila por fila
        for index, fila in df.iterrows():
            # Verifica si cada columna existe en la tabla de la base de datos
            print(f"validando el id {index} ")
            columnas_faltantes = set(fila.index.str.lower()) - set(columnas_tabla)
            if columnas_faltantes:
                print("Las siguientes columnas faltan en la tabla de la base de datos:", columnas_faltantes)
                error = True

        # Si todas las validaciones pasan, retorna True
        cursor.close()
        conn.close()        
        return not error
    except (Exception) as error:
        print(error)
        return False




def get_file_object(idFile, files):
    for i in range(len(files)):
        if files[i]['id'] == int(idFile):
            return files[i]
    return

def get_table_sheet(nombre_archivo):
    for i in range(len(products_catalog_str)):
        if products_catalog_str[i]['name'] == nombre_archivo:
            return products_catalog_str[i]
    return


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
