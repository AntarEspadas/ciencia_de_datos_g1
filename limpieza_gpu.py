#! /usr/bin/env python3

# from pynvml import *
import cudf as pd
import numpy as np
from os import path
from glob import glob
from tqdm import tqdm
import os
import re
import csv
import argparse

COLUMNAS = ["city", "speedKMH", "uuid", "endNode", "speed", "severity", "level", "length", "roadType", "delay", "updateMillis", "pubMillis", "tiempo_min", "tiempo_max"]

def encontrar_archivo(archivos: list[str], byte: int):
    tams = [os.stat(archivo).st_size for archivo in archivos]
    indice = np.searchsorted(np.cumsum(tams), byte + 1)
    return indice

def leer_archivos(archivos: list[str]):
    try:
        return pd.read_json(archivos, lines=True), None
    except RuntimeError as e:
        match = re.findall(r"location (\d+)", str(e))
        if match is None:
            raise e
        byte = int(match[0])
        return None, encontrar_archivo(archivos, byte)

def procesar_parcial(archivos: list[str], archivo_salida: str, columnas):
    df: pd.DataFrame

    while True:
        df, err = leer_archivos(archivos)
        if df is not None:
            break
        archivo = path.abspath(archivos[err])
        print(f"No se pudo leer el archivo {archivo}")
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
    
    # Eliminar columnas no requeridas
    df = df.loc[:, columnas]

    with open(archivo_salida, "a") as f:
        df.to_csv(f, columns=columnas, index=False, header=False)

def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs="+", help="Lista o glob de archivos en formato JSON para procesar")
    parser.add_argument("--output", "-o", required=True, help="Archivo de salida")
    parser.add_argument("--tam-bloque", "-b", default=1500, type=int, help="Los archivos se leeran en bloques del tamaño especificado. Disminuir este valor si se producen errores de memoria. Unidad: MB")
    parser.add_argument("--columnas", "-c", default=COLUMNAS, nargs="+", help="Las columnas que se de desea conservar")
    return parser

def main():

    parser = get_parser()
    args = parser.parse_args()

    entrada: list[str] = args.input
    archivo_salida: str = args.output
    tam_max: int = args.tam_bloque * 1_000_000
    columnas: list[str] = args.columnas

    archivos = sum(map(glob, entrada), [])

    bloques_de_archivos = [[]]

    suma_actual = 0
    for archivo in archivos:
        tam = os.stat(archivo).st_size

        if tam < 50:
            continue

        nueva_suma = suma_actual + tam
        if nueva_suma <= tam_max:
            suma_actual = nueva_suma
            bloques_de_archivos[-1].append(archivo)
        else:
            suma_actual = tam 
            bloques_de_archivos.append([archivo])
        
    out_dir = path.abspath(archivo_salida)
    out_dir = path.dirname(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    with open(archivo_salida, "w") as f:
        csv.writer(f).writerow(columnas)

    for archivos in tqdm(bloques_de_archivos, total=len(bloques_de_archivos)):
        procesar_parcial(archivos, archivo_salida, columnas)

    df: pd.DataFrame = pd.read_csv(archivo_salida)

    df["tiempo_min"] = pd.to_datetime(df["tiempo_min"])
    df["tiempo_max"] = pd.to_datetime(df["tiempo_max"])

    tiempos = df.groupby("uuid").agg({"tiempo_min": "min", "tiempo_max": "max"}).reset_index()

    df = df.drop_duplicates("uuid")
    df = df.drop(columns=["tiempo_min", "tiempo_max"])

    df = df.merge(tiempos, how="left", on="uuid")

    with open(archivo_salida + "-2", "w") as f:
        df.to_csv(f, columns=columnas, index=False)


if __name__ == "__main__":
    main()
