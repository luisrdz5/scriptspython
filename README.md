###### scriptspython


Este es un proyecto donde se generaran 2 aplicativos en Python los cuales se describen a continuación


### Photo Uploader

Tomara fotografias de una carpeta en google drive, las almacenara en s3 las categorizara y las guardara en una base de datos en postgresql 

### Product Uploader 

Este aplicativo tomara unas hojas de google shets categorizadas y las insertara en las tablas correspondientes dentro de una base de datos en postgresql 


# Como crear un ambiente virtual 

se debe ejecutar el siguiente comando 

#python3 -m venv env
(env)#



# Ingresar al entorno virtual 

se debe ejecutar el siguiente comando 

#source env/bin/activate


# Instalar paquetes en el ambiente virtual 

(env)# pip3 install boto3


# Como generar archivo requeriments.txt 

Cuando ya se hallan instalado los paquetes necesarios se debera ejecutar el comando 

(env)# pip3 freeze > requeriments.txt 


# Como instalar el proyecto desde cero con las dependencias usando el archivo requeriments.txt 

Se debera ejecutar el comando: 


```sh
git clone git@github.com:luisrdz5/scriptspython.git
python3 -m venv env
source env/bin/activate
pip3 install -r requirements.txt
python3 main.py
```





