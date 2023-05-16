#! /usr/bin/env python3

import cudf as pd
import numpy as np
from os import path
from glob import glob
from tqdm import tqdm
import os
import re
import csv
import argparse
import sys

COLUMNAS = ["city", "speedKMH", "uuid", "endNode", "speed", "severity", "level", "length", "roadType", "delay", "updateMillis", "pubMillis"]

def main():
    """
    Punto de entrada del programa
    """

    # Obtiene el parser de línea de comandos
    parser = get_parser()
    # Obtiene los argumentos de la línea de comandos
    args = parser.parse_args()

    # Lee los argumentos de línea de comandos en variables locales
    entrada: list[str] = args.input
    archivo_salida: str = args.output
    tam_max: int = args.tam_bloque * 1_000_000
    columnas: list[str] = args.columnas + ["tiempo_min", "tiempo_max", "x1", "y1", "x2", "y2"]

    # Cada archivo en la lista 'entrada' es tratado como un patrón glob
    # Se usa la función 'glob' para convertir cada patrón en una lista,
    # obteniendo así una lista de listas, la cual es aplanada usando la
    # función 'sum', obteniendo así una única lista de archivos
    archivos = sum(map(glob, entrada), [])

    # Obtener los archivos agrupados en bloques de, por defecto, 1.5GB
    bloques_de_archivos = obtener_bloques_de_archivos(archivos, tam_max)
        
    # Crear el directorio en el que se guardará el archivo de salida,
    # en caso de que no exista
    out_dir = path.abspath(archivo_salida)
    out_dir = path.dirname(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    # Crea el archivo de salida y escribe los encabezados de las columnas
    with open(archivo_salida, "w") as f:
        csv.writer(f).writerow(columnas)

    # Itera sobre los bloques de archivos. 'tqdm' es una función proveniente de
    # la biblioteca del mismo nombre, la cual permite envolver cualquier lista
    # e imprimir automáticamente el progreso de su iteración en la consola
    for archivos in tqdm(bloques_de_archivos):
        # Procesa individualmente cada bloque de archivos. Ver implementación
        # de la función 'procesar_parcial'
        procesar_parcial(archivos, archivo_salida, columnas)

    # Una vez procesado individualmente cada bloque, se espera que el tamaño se
    # haya reducido lo suficiente para que los datos completos quepan en memoria
    df: pd.DataFrame = pd.read_csv(archivo_salida)

    # Convertir valores de tiempo a objetos de tipo datetime
    df["tiempo_min"] = pd.to_datetime(df["tiempo_min"])
    df["tiempo_max"] = pd.to_datetime(df["tiempo_max"])

    # Eliminar posibles duplicados, recalculando el tiempo mínimo y máximo
    tiempos = df.groupby("uuid").agg({"tiempo_min": "min", "tiempo_max": "max"}).reset_index()

    df = df.drop_duplicates("uuid")
    df = df.drop(columns=["tiempo_min", "tiempo_max"])

    df = df.merge(tiempos, how="left", on="uuid")

    # Se guarda el archivo final
    with open(archivo_salida, "w") as f:
        df.to_csv(f, columns=columnas, index=False, chunksize=1_000_000)


def encontrar_archivo(archivos: list[str], byte: int):
    """
    Dado la posición de un byte, encuentra dentro de una lista el archivo
    al cual corresponde dicho byte.

    Por ejemplo, si se tienen los siguientes archivos y tamaños en bytes

    | Nombre    	| Tamaño 	|
    |-----------	|--------	|
    | Archivo 1 	| 15     	|
    | Archivo 2 	| 5      	|
    | Archivo 3 	| 10     	|
    | Archivo 4 	| 20     	|

    El byte 25 corresponde al archivo 3
    """
    # Se obtiene una lista con el tamaño de cada archivo
    tams = [os.stat(archivo).st_size for archivo in archivos]
    # Se calcula el arreglo de sumas acumuladas de los tamaños y se realiza una
    # búsqueda binaria dentro del arreglo
    indice = np.searchsorted(np.cumsum(tams), byte + 1)
    return indice

def leer_archivos(archivos: list[str]):
    """
    Intenta leer el bloque de archivos proporcionado.

    Si alguno de los archivos contiene un error de sintaxis, devuelve
    `None` como valor del Dataframe y el índice del dicho archivo que
    ocasionó el error. De lo contrario, devuelve el Dataframe y `None`
    como valor del error
    """
    try:
        # Intenta leer el archivo
        return pd.read_json(archivos, lines=True), None
    except RuntimeError as e:
        # Atrapa el error y busca la palabra 'location' dentro del mensaje de
        # error para obtener la posición en la que ocurrió el error
        match = re.findall(r"location (\d+)", str(e))
        # Si no encuentra la palabra 'location', probablemente quiere decir que el error
        # fur provocado por una causa distinta a un error de sintaxis en un archivo, por
        # lo que se vuelve a arrojar la excepción
        if match is None:
            raise e
        # Convierte la posición del error de str a int
        byte = int(match[0])
        # Usa la posición del error para encontrar el archivo en el cual se originó el error
        return None, encontrar_archivo(archivos, byte)

def obtener_bloques_de_archivos(archivos: list[str], tam_max: int) -> list[list[str]]:
    """
    Agrupa los archivos en bloques de máximo 'tam_max' bytes
    """
    bloques_de_archivos = [[]]

    suma_actual = 0
    for archivo in archivos:
        # os.stat devuelve información acerca del archivo, incluyendo su tamaño en bytes
        tam = os.stat(archivo).st_size

        # En esta caso, los archivos con tamaño menor a 50 bytes suelen contener datos basura,
        # por lo que no se les toma en cuenta
        if tam < 50:
            continue

        nueva_suma = suma_actual + tam
        # Si agregar este archivo al bloque actual no hace que el bloque
        # rebase el tamaño máximo, agregarlo
        if nueva_suma <= tam_max:
            suma_actual = nueva_suma
            bloques_de_archivos[-1].append(archivo)
        # De lo contrario, crear un nuevo bloque y agregar el archivo a ese bloque a ese bloque
        else:
            suma_actual = tam 
            bloques_de_archivos.append([archivo])

    return bloques_de_archivos

def procesar_parcial(archivos: list[str], archivo_salida: str, columnas):
    """
    Procesa un bloque de archivos.

    Realiza los siguientes pasos:

    - Lee el bloque de archivos en memoria, saltando archivos con formato inválido
    - Expande el arreglo `jams` pa convertirlo en un Dataframe
    - Añade el dato de `tiempo` a cada fila del dataframe
    - Convierte la columna `tiempo` a objetos de tipo datetime
    - Elimina datos con UUIDs duplicados, manteniendo en dos columnas adicionales,
      `tiempo_min` y `tiempo_max`, la primera y última hora en la cual apareció el UUID correspondiente
    - Convierte el arreglo `line`, el cual contiene información de georeferencia, a 4 columnas, `x1`, `y1`, `x2` y `y2`,
      las cuales contienen información del primer y del último punto del arreglo `line`
    - Elimina columnas no deseadas
    """
    df: pd.DataFrame

    while True:
        # Intenta leer el bloque de archivos, si encuentra un error,
        # elimina del bloque al archivo que provocó el error y vuelve a intentarlo,
        df, err = leer_archivos(archivos)
        if df is not None:
            break
        archivo = path.abspath(archivos[err])
        print(f"No se pudo leer el archivo {archivo}", file=sys.stderr)
        archivos.pop(err)

    # Extraer los datos de jams y juntarlos con la fecha
    df = df.loc[:, ["jams", "tiempo "]]
    df = df.drop(columns="jams").join(df["jams"].explode()).reset_index(drop=False)
    df = df.drop(columns="jams").join(df["jams"].struct.explode())

    # Convertir el formato de la fecha
    df["tiempo"] = pd.to_datetime(df["tiempo "])

    # Obtener los valores de mínimo y máximo de tiempo para cada id
    tiempos = df.groupby("uuid").agg({"tiempo": ["min", "max"]}).reset_index()
    tiempos.columns = ["uuid", "tiempo_min", "tiempo_max"]

    # Eliminar filas con ids duplicadas
    df = df.drop_duplicates("uuid")

    # Pegar la información de tiempo de vuelta en el dataframe original
    df = df.merge(tiempos, how="left", on="uuid")

    # Separar la información de georeferencia en 4 columnas: x1, x2, y1, y2
    line = df["line"].list
    df.drop(inplace=True, columns="line")
    df = df.join(line.get(0).struct.explode())
    df.rename(columns={"x": "x1", "y": "y1"}, inplace=True)
    df = df.join(line.get(-1).struct.explode())
    df.rename(columns={"x": "x2", "y": "y2"}, inplace=True)
    
    # Eliminar columnas no requeridas
    df = df.loc[:, columnas]

    # Guardar el los datos en el archivo de salida. Nótese que el modo
    # de apertura del archivo es "a", append, por lo que no se sobrescriben
    # los datos anteriores, sino que los datos nuevos son concatenados a los
    # datos viejos
    with open(archivo_salida, "a") as f:
        df.to_csv(f, columns=columnas, index=False, header=False)

def get_parser():
    """
    Obtiene un parser que permite obtener los parámetros de la línea de comandos usando la biblioteca argparse
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="+", help="Lista o glob de archivos en formato JSON para procesar")
    parser.add_argument("--output", "-o", required=True, help="Archivo de salida")
    parser.add_argument("--tam-bloque", "-b", default=1500, type=int, help="Los archivos se leeran en bloques del tamaño especificado. Disminuir este valor si se producen errores de memoria. Unidad: MB")
    parser.add_argument("--columnas", "-c", default=COLUMNAS, nargs="+", help="Las columnas que se de desea conservar")
    return parser


if __name__ == "__main__":
    main()
