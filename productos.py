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


#Generamos la informacion del archivo de logs 

uuid_log= str(shortuuid.ShortUUID().random(length=6))
fecha = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
nombre_archivo = 'logs/validacion_archivos/log_' + fecha[:10] + uuid_log + '.txt' 

def get_files(folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(q=query, fields="nextPageToken, files(parents,name, id, mimeType)").execute()
    items = results.get("files", [])
    i=0
    data=[]
    
    for item in items:
        if item["mimeType"] != "application/vnd.google-apps.folder":
            print(f" {i+1} .- {item['name']}")
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
        nombre_archivo=file["name"]
        print(f'validando : {nombre_archivo}')        
        fileSheet = client.open(nombre_archivo).sheet1
        tableSheet = get_table_sheet(nombre_archivo)
        filas = fileSheet.get_all_values()
        
        validate = validar_dataframe(filas, tableSheet["table"], nombre_archivo )

        if validate :
            print (f"Ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        else:
            print (f"No ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        return validate
    except (Exception) as error:
        print(error)
        return error

    # prompt utilizado como lees archivos de google sheets en python para validar si pueden ser insertados en una bd postgresql

def validar_dataframe(filas, table, nombre_archivo):
    error = False
    errores = 0 
    cadena = f" ******* Se ha iniciado la validación del archivo {nombre_archivo} contra la tabla {table} ********** \n"
    write_log(cadena)
    try:
        # Realiza las validaciones en el DataFrame
        cursor = conn.cursor()
        query= f"SELECT column_name, data_type  FROM information_schema.columns WHERE table_name = '{table}'"
        cursor.execute(query)
        schema = cursor.fetchall()
        num_columns_postgres = len(schema)
        for i, row in enumerate(filas[1:], start=2):
            # Comprueba si el número de columnas en la fila coincide con el número de columnas en la tabla PostgreSQL
            if len(row) != num_columns_postgres:
                write_log(f" La fila {i} no coincide con el número de columnas en la tabla PostgreSQL. ya que el google sheet tiene {len(row)} y la base de datos tiene {num_columns_postgres}")
                errores = errores + 1
                error = True
                continue
            else:
                 write_log(f"La fila {i} si coincide con el número de columnas en la tabla PostgreSQL.")

            for j, value in enumerate(row):
                column_name = schema[j][0]
                column_type = schema[j][1]

                # Si el valor está vacío, saltamos la validación
                if value == '':
                    continue
                
                if column_type == "integer":
                    try:
                        int(value)
                    except ValueError:
                        write_log(f"El valor {value} en la fila {i}, columna {column_name} no es un integer válido.")
                        error = True
                        errores = errores + 1
                elif column_type == "text":
                    if not isinstance(value, str):
                        write_log(f"El valor {value} en la fila {i}, columna {column_name} no es un string válido.")
                        error = True
                        errores = errores + 1
                elif column_type == "boolean":
                    if value.lower() not in ["true", "false", "1", "0"]:
                        write_log(f"El valor {value} en la fila {i}, columna {column_name} no es un boolean válido.")
                        error = True
                        errores = errores + 1
                # Se pueden agregar aquí más tipos de datos según sea necesario

        cursor.close()
        conn.close()     
        # Si todas las validaciones pasan, retorna True  
        if error == False :
          write_log(f" ******* No se encontro error en la validación del archivo {nombre_archivo} contra la tabla {table} ********** \n")
        else:
            write_log(f" ******* Se encontraron {errores} errores en la validación del archivo {nombre_archivo} contra la tabla {table} ********** \n")
        return not error
    except (Exception) as error:
        print(error)
        return False



def upload_file(idFile, files):
    try:
        file = get_file_object(idFile, files)
        client = gspread.authorize(credsSheets)
        nombre_archivo=file["name"]
        print(f'cargando : {nombre_archivo}')        
        fileSheet = client.open(nombre_archivo).sheet1
        tableSheet = get_table_sheet(nombre_archivo)
        filas = fileSheet.get_all_values()
        
        validate = update_dataframe(filas, tableSheet["table"], nombre_archivo )

        if validate :
            print (f"Ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        else:
            print (f"No ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        return validate
    except (Exception) as error:
        print(error)
        return error


def update_dataframe(filas, table, nombre_archivo):
    error = False
    errores = 0 
    cadena = f" ******* Se ha iniciado la inserción del archivo {nombre_archivo} en la tabla {table} ********** \n"
    write_log(cadena)
    try:
        cur = conn.cursor()
        # Obtén el esquema de la tabla en PostgreSQL
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = %s;
        """, (table,))
        schema = cur.fetchall()
        
        # Cierra el cursor y la conexión


        column_names = [col[0] for col in schema]
        
        # Preparación de las declaraciones INSERT
        insert_statements = []
        for row in filas[1:]:  # Ignora la primera fila (encabezados)
            # Las comillas simples dentro de los valores deben escaparse para evitar errores de sintaxis SQL
            escaped_values = [str(value).replace("'", "''") for value in row]
            # Utiliza la función format para insertar los valores en la declaración SQL
            stmt = "INSERT INTO {} ({}) VALUES ({});".format(table, ', '.join(column_names), ', '.join(["'" + value + "'" for value in escaped_values]))
            insert_statements.append(stmt)
        cur.close()
        conn.close()
        return insert_statements
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

def write_log(cadena):
    # crea la carpeta si no existe
    os.makedirs(os.path.dirname(nombre_archivo), exist_ok=True) 
    with open(nombre_archivo, 'a') as archivo:
        archivo.write(fecha + ': ' + cadena + '\n')

def clear_screen():
    # Comprueba si el sistema operativo es Windows
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

def main():
    print(f'this is an aplication that  walks through the product files')
    print(f'please select the file')
    print(f' A .- All')
    files = get_files(folder_id)
    #print(f'files contiene: {files}')
    idFile = input("Please select an option : ")
    clear_screen()
    if str(idFile) == 'A' or str(idFile) == 'a' or validate_folder(idFile, files):
        print(f'What do you want to do:')
        print(f'1.-Validate')
        print(f'2.-Validate / Insert')
        option = input("Please select an option: ")
        clear_screen()
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
                print(f'Validando y aplicando {idFile}')
                validation=validate_file(idFile, files)
                if(validation == True):
                    upload_file(idFile, files)
                else:
                    print(f'no se puede procesar el archivo {idFile} por que la validación fue incorrecta')
        else:
            print(f'Opcion no valida')
    else:
        print(f'Opcion no valida')

if __name__ == "__main__":
    main()