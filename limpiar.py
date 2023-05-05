#El programa recibe un argumento: La carpeta desde la cual se obtendrán los archivos JSON

import json
import numpy as np
import pandas as pd
import csv
import os
import sys
import glob

#Creamos las carpetas necesarias para cada uno de los pasos expuestos en las reuniones
ruta = os.getcwd()
directorios = ['1JSON_limpios','2JSON_CSV','3unidos']
for directorio in directorios:
    path = os.path.join(ruta,directorio)
    try:
        os.mkdir(path)
    except OSError as error:
        print('El directorio '+directorio+' ya existe')

###-----------------------------------------------------------------------------------------###
#La siguiente sección realiza la primer limpieza: Solo mantiene los 'jams' del archivo JSON,
#guarda la fecha y elimina los atributos que no se usarán.
###-----------------------------------------------------------------------------------------###

ruta_ = sys.argv[1] #Aquí se pasa el nombre de la carpeta del año como argumento del programa
subdirectories = [f.path for f in os.scandir(ruta_) if f.is_dir()] #Se obtienen los subdirectorios (los meses)

for directory in subdirectories:
    # Checking if the directory is empty or not
    if not os.listdir(directory): #Si está vacío se elimina de la lista subdirectories
        subdirectories.remove(directory)

#La primer limpieza se hará para todos los subdirectorios (meses) y se guardará en una misma carpeta
for directory in subdirectories:

    #almacenar nombre de los archivos de la carpeta en una lista
    nombres_archivos = [nombre for nombre in os.listdir(directory) if nombre.endswith(".json")]

    for i in range(len(nombres_archivos) ): #len(nombres_archivos)

        nombre_archivo = nombres_archivos[i]

        arch = directory + "/" + nombre_archivo
        tam = os.stat(arch).st_size
        if tam < 50:
            continue

        fecha_archivo = nombres_archivos[i][5:-5] #Extraccion de la fecha del archivo

        with open(directory + "/" + nombre_archivo, "r") as archivo_original:
            datos = json.load(archivo_original)

        datos_modificado = datos
        del datos_modificado["alerts"]
        del datos_modificado["endTimeMillis"]
        del datos_modificado["startTimeMillis"]
        del datos_modificado["startTime"]
        del datos_modificado["endTime"]
        del datos_modificado["users"]

        #Agregar tiempo
        datos_modificado['fecha'] = fecha_archivo

        for elemento in range(len(datos["jams"]) ):

            if "country" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["country"]
            if "segments" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["segments"]
            if "id" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["id"]
            if "blockingAlertID" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["blockingAlertID"]
            if "blockExpiration" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["blockExpiration"]
            if "blockStartTime" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["blockStartTime"]
            if "blockUpdate" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["blockUpdate"]
            if "blockingAlertUuid" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["blockingAlertUuid"]
            if "blockDescription" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["blockDescription"]
            if "causeAlert" in datos_modificado["jams"][elemento]:
                del datos_modificado["jams"][elemento]["causeAlert"]


        #Crear archivo JSON y escribir
        with open("1JSON_limpios/" + nombre_archivo,"w") as archivo_modificado:
            json.dump(datos_modificado, archivo_modificado)
###-----------------------------------------------------------------------------------------###
#La siguiente sección realiza la transformación de archivos JSON a CSV.
###-----------------------------------------------------------------------------------------###

ruta_2 = "1JSON_limpios"
nombres_archivos = [nombre for nombre in os.listdir(ruta_2) if nombre.endswith(".json")]

for i in range(len(nombres_archivos)): #len(nombres_archivos)
    nombre_archivo = nombres_archivos[i]
    nombre_nuevo = nombres_archivos[i][0:-5]    
    # abrir  JSON
    with open(ruta_2 + '/' + nombre_archivo, 'r') as f:
        data = json.load(f)
    jams = data['jams']
    fecha = data['fecha']
    #Se convierte la lista jams a dataframe
    df = pd.DataFrame(jams)
    #Se agrega la fecha al dataframe
    df['time'] = np.repeat(fecha, len(df))
    #Se convierte el dataframe a CSV
    df.to_csv('2JSON_CSV/'+nombre_nuevo+'.csv',index=False)

###-----------------------------------------------------------------------------------------###
#La siguiente sección junta todos los archivos CSV en uno solo.
###-----------------------------------------------------------------------------------------###

# Se define la ruta de acceso de los archivos CSV a unir
ruta = '2JSON_CSV'

# Se usa la biblioteca glob para obtener la lista de archivos CSV
todos_los_archivos = glob.glob(ruta + "/*.csv")
#Se ordenan los nombres
todos_los_archivos.sort()

# Se concatenan todos los archivos CSV en un DataFrame pandas
li = []
for archivo in todos_los_archivos:
    df = pd.read_csv(archivo, index_col=None, header=0)
    li.append(df)
df_concatenado = pd.concat(li, axis=0, ignore_index=True)

# Se escribe el DataFrame concatenado en un nuevo archivo CSV
df_concatenado.to_csv("3unidos/archivos_unidos.csv", index=False)

###-----------------------------------------------------------------------------------------###
#La siguiente sección elimina los atributos que tienen un alto porcentaje de valores Nulos y los renglones con valores nulos
###-----------------------------------------------------------------------------------------###

#Se cargan los datos
datos = pd.read_csv("3unidos/archivos_unidos.csv")
#Se convierten los valores con NONE a NaN para poder contarlos y borrarlos
datos = datos.replace('NONE', np.nan)
#La siguiente linea es para observar el porcentaje de valores nulos en cada columna
null_pct = datos.apply(pd.isnull).sum()/datos.shape[0]
#Quitar el comentario para observalos
print(null_pct)
#Se guardan los atributos que tengan menos del 10% de datos faltante
valid_columns = datos.columns[null_pct < .10]
#Los atributos con suficientes datos se guardan, los demás se desechan
datos = datos[valid_columns].copy()
#Se dejan todos los nombres de atributos con minusculas para facilidad de visualización/manejo
datos.columns = datos.columns.str.lower()
#Se eliminan los renglones con datos faltantes
datos = datos.dropna()
#La siguiente linea es para visualizar el número de valores nulos por atributo (opcional visualizarlo)
#print(datos.apply(pd.isnull).sum())
datos.to_csv('3unidos/unidos_sin_nulos.csv')
