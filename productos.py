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
    },{
        "name": "EPICENTROS para csv",
        "table":"Epicentros",
    },{
        "name": "TWEETERS para csv (2022)",
        "table":"Tweeters",
    },{
        "name": "BASES para csv",
        "table":"Bases",
    },{
        "name": "BASES PARA BOCINA para csv",
        "table":"basesBocina",
    },{
        "name": "COMPONENTES para csv",
        "table":"Componentes",
    },{
        "name": "ADAPTADORES DE IMPEDANCIA para csv",
        "table":"AdaptadoresImpedancia",
    },{
        "name": "ADAPTADORES ANTENA para csv",
        "table":"AdaptadoresAntena",
    },{
        "name": "MEDIOS RANGOS para csv",
        "table":"MediosRangos",
    },{
        "name": "WOOFERS para csv",
        "table":"Woofers",
    },{
        "name": "CAJONES para csv",
        "table":"Cajones",
    },{
        "name": "KIT DE CABLES para csv",
        "table":"KitsCables",
    },{
        "name": "ARNESES para csv",
        "table":"Arneses",
    },{
        "name": "ECUALIZADORES para csv",
        "table":"Ecualizadores",
    },{
        "name": "AUDIO MARINO para csv",
        "table":"audioMarino",
    },{
        "name": "SEGURIDAD para csv",
        "table":"seguridad",
    },{
        "name": "ACCESORIOS DE CAMIONETA para csv",
        "table":"accesoriosCamioneta",
    },{
        "name": "SERVICIOS para csv",
        "table":"servicios",
    },{
        "name": "VIDEO para csv",
        "table":"video",
    },{
        "name": "ILUMINACION para csv",
        "table":"iluminacion",
    },            
]


#Generamos la informacion del archivo de logs 

uuid_log= str(shortuuid.ShortUUID().random(length=6))
now = datetime.datetime.now()
fecha = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
hora =  str(now.hour)+'_'+str(now.second)


nombre_archivo = 'logs/validacion_archivos/log_' 
archivo_uuid= fecha[:10]+ '-' + hora + '-' + uuid_log + '.txt' 

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
        print(f'validando file: {file}')
        client = gspread.authorize(credsSheets)
        nombre_archivo=file["name"]
        fileSheet = client.open(nombre_archivo).sheet1
        tableSheet = get_table_sheet(nombre_archivo)
        filas = fileSheet.get_all_values()
        
        validate = validar_dataframe(filas, tableSheet["table"], nombre_archivo )

        if validate :
            print (f"Ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        else:
            print (f"No ha sido correcta la validación del archivo {nombre_archivo} se ha validado contra la tabla {tableSheet['table']}")
        return validate
    except (Exception, gspread.exceptions) as error:
        print(error)
        return error




def validar_dataframe(filas, table, nombre_archivo):
    error = False
    errores = 0 
    cadena = f" ******* Se ha iniciado la validación del archivo {nombre_archivo} contra la tabla {table} ********** \n"
    print(cadena)
    write_log(cadena)
    try:
        cursor = conn.cursor()
        # Solo obtenemos los tipos de datos en orden
        query = """
            SELECT data_type
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """
        cursor.execute(query, (table,))
        schema = cursor.fetchall()
        num_columns_postgres = len(schema)
        
        # Validar número de columnas
        if len(filas[0]) != num_columns_postgres:
            write_log(f"El número de columnas no coincide. Archivo: {len(filas[0])}, PostgreSQL: {num_columns_postgres}")
            return False

        # Validar tipos de datos por cada fila
        for i, row in enumerate(filas[1:], start=2):
            if len(row) != num_columns_postgres:
                write_log(f"La fila {i} tiene un número incorrecto de columnas: {len(row)}")
                errores += 1
                error = True
                continue

            for j, value in enumerate(row):
                column_type = schema[j][0]
                
                # Si el valor está vacío, saltamos la validación
                if value == '':
                    continue
                
                # Validar tipo de dato
                if column_type == "integer":
                    try:
                        int(value)
                    except ValueError:
                        write_log(f"Error en fila {i}, columna {j+1}: '{value}' no es un integer válido")
                        error = True
                        errores += 1
                elif column_type == "text":
                    if len(str(value)) > 254:
                        write_log(f"Error en fila {i}, columna {j+1}: texto excede 254 caracteres")
                        error = True
                        errores += 1
                elif column_type == "boolean":
                    if str(value).lower() not in ["true", "false", "1", "0"]:
                        write_log(f"Error en fila {i}, columna {j+1}: '{value}' no es un boolean válido")
                        error = True
                        errores += 1

        if error:
            write_log(f"Se encontraron {errores} errores en la validación")
        else:
            write_log("Validación completada sin errores")
            
        return not error

    except Exception as error:
        print(error)
        write_log(f"Error en la validación: {str(error)}")
        return False



def update_database_with_file(idFile, files):
    try:
        file = get_file_object(idFile, files)
        client = gspread.authorize(credsSheets)
        nombre_archivo=file["name"]
        print(f'cargando : {nombre_archivo}')        
        fileSheet = client.open(nombre_archivo).sheet1
        tableSheet = get_table_sheet(nombre_archivo)
        filas = fileSheet.get_all_values()
        
        validate = upload_dataframe(filas, tableSheet["table"], nombre_archivo )
        if validate :
            print (f"Ha sido correcta la actualización de la tabla {tableSheet['table']} desde el  archivo {nombre_archivo}  ")
            write_log(f"Ha sido correcta la actualización de la tabla {tableSheet['table']} desde el  archivo {nombre_archivo}  ")
        else:
            print (f"No ha sido correcta la actualización de la tabla {tableSheet['table']} desde el  archivo {nombre_archivo} ")
            write_log(f"No ha sido correcta la actualización de la tabla {tableSheet['table']} desde el  archivo {nombre_archivo} ")
        return validate
    except (Exception) as error:
        print(error)
        write_log(f"Se encontro el siguiente error {error} .")
        return error


def upload_dataframe(filas, table, nombre_archivo):
    error = False
    errores = 0
    cadena = f" ******* Se ha iniciado la inserción del archivo {nombre_archivo} en la tabla {table} ********** \n"
    write_log(cadena)
    stmt = ''
    try:
        cur = conn.cursor()
        # Obtén el esquema de la tabla en PostgreSQL
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns 
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table,))
        schema = cur.fetchall()
        cur.execute(f'TRUNCATE TABLE "{table}"')
        conn.commit()

        #column_names = [{ col[0]  if k != 'id'} for col in schema]
        column_names = [col[0] for col in schema if col[0] != "id"]
        #print (column_names)
        sku_index = column_names.index('sku')
        idFotos_index = column_names.index('fotosId')
        column_names = ['"'+ col[0] + '"' for col in schema  if col[0] != "id"]

        for row in filas[1:]:  # Ignora la primera fila (encabezados)
            row = row[1:]
            #print(f"el row es: {row}")
            sku=row[sku_index]
            #print(f"sku  es: {row[sku_index]}")
            idFotos=get_idFotos(sku)
            #print(f"if foto s es: {int(idFotos[0]) } ")
            if idFotos == "":
                row[idFotos_index] = "null"
            else:
                row[idFotos_index] = int(idFotos[0])
            
            # Las comillas simples dentro de los valores deben escaparse para evitar errores de sintaxis SQL
            escaped_values = []
            for value in row:
                try:
                    #print(f"quitando comillas a: {value } ")
                    escaped_values.append(str(value).replace("'", "''"))
                except Exception as e:
                    print(f"Error with value {value}: {e}")
            #print(f"el segundo row es: {row[idFotos_index] } y el row quedo {escaped_values}")
            if idFotos == "":
                escaped_values[idFotos_index] = ""
            else:
                escaped_values[idFotos_index] = int(idFotos[0])

            # Utiliza la función format para insertar los valores en la declaración SQL
            cadenaValues = ""
            
            for value in escaped_values:
                if cadenaValues == "":
                    cadenacoma = ''
                else:
                     cadenacoma= ", "
                if value == '':
                    cadenaValues = cadenaValues + cadenacoma + "null"
                else:
                    if isinstance(value, int):
                        cadenaValues = cadenaValues + cadenacoma + "'" +  str(value) + "'"
                    else:
                        value = value[:254]
                        cadenaValues = cadenaValues + cadenacoma + "'" + str(value.replace('\'', '').replace('\"', '')) + "'"
                        
                   
            #stmt = 'INSERT INTO "{}" ({}) VALUES ({});'.format(table, ', '.join(column_names), ', '.join(['"' + value.replace('\'', '').replace('\"', '') + '"' for value in escaped_values]))
            stmt = 'INSERT INTO "{}" ({}) VALUES ({});'.format(table, ', '.join(column_names), cadenaValues)
            #print(f'insertando: {stmt}')
            #insert_statements.append(stmt)
            cur.execute(stmt)
            conn.commit()  
        cur.close()
        #print (f" se han insertado : {insert_statements}")
        return not error
    except (Exception) as error:
        print(error)
        write_log(f"Se encontro el siguiente error {error} .")
        write_log(f"Se encontro el error en el siguiente producto  {stmt} .")
        return False


def get_idFotos(sku):
    try:
        # busco el sku en el modelo 
        cadena = f'select id from "CatalogoFotos" where sku=\'{sku}\';' 
        cur = conn.cursor()
        cur.execute(cadena) 
        id = cur.fetchone() 
        if(id):
           return id
        else:
            return ""
        cur.close()      
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)        



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
    global nombre_archivo
    clear_screen()
    print(f'this is an aplication that  walks through the product files')
    print(f'please select the file')
    print(f' A .- All')
    files = get_files(folder_id)
    idFile = input("Please select an option : ")
    clear_screen()
    if str(idFile) == 'A' or str(idFile) == 'a' or validate_folder(idFile, files):
        filename = get_file_object(idFile, files)
        print(f' Se procesara el archivo : {filename["name"]} ')        
        print(f'Que quieres hacer:')
        print(f'1.-Validate')
        print(f'2.-Validate / Insert')
        option = input("Please select an option: ")
        clear_screen()
        if option.isnumeric():
            if int(option) == 1 and (str(idFile) == 'A' or str(idFile) == 'a'):
                print('Validar todos')
                #validate_file()
            if int(option) == 1 and ( str(idFile) != 'A' and str(idFile) != 'a'):
                nombre_archivo = nombre_archivo + filename["name"].replace(" ", "_")  + archivo_uuid
                validate_file(idFile, files)
            if int(option) == 2 and ( str(idFile) == 'A' or str(idFile) == 'a' ):
                print('Validar y aplicar todos')
                #validate_file()
            if int(option) == 2 and ( str(idFile) != 'A' and str(idFile) != 'a' ):
                print(f'Validando y aplicando {idFile}')
                print(f'Insertando Archivo {filename["name"]} en la base de datos ')
                nombre_archivo = nombre_archivo + filename["name"].replace(" ", "_") +"_"  + archivo_uuid
                validation=validate_file(idFile, files)
                if(validation == True):
                    update_database_with_file(idFile, files)
                else:
                    print(f'no se puede procesar el archivo {idFile} por que la validación fue incorrecta')
            conn.close()   
        else:
            print(f'Opcion no valida')
    else:
        print(f'Opcion no valida')

if __name__ == "__main__":
    main()
