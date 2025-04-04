from typing import Optional, Tuple
from icecream import ic
import pandas as pd
import os
import sqlite3
import json
import logging
from observatorio_ceplan import Ficha, FichaQueries, FichaRegex
from observatorio_ceplan import FichaQueries, VistasQueries
import re
from pylnk3 import Lnk

# Variables globales
script_dir = os.path.dirname(os.path.abspath(__file__))
databases = os.path.join(script_dir, "databases")
log_path = os.path.join(script_dir, "logs", "observatorio.log")

logging.basicConfig(
    level=logging.INFO,  # Nivel de registro (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  
    handlers=[
    logging.FileHandler(log_path, mode='a', encoding='utf-8'),  # Archivo en UTF-8
    logging.StreamHandler()  # También mostrar logs en la consola
    ]
)

# ============================================================
#  1. Definir funciones subordinadas
# ============================================================
def regexp(pattern, item):
    return re.search(pattern, item) is not None

def read_lnk_target(lnk_path):
    lnk = Lnk(lnk_path)
    return lnk.path


def connect(db_name: str = "observatorio", register_regex: bool = False):
    database = os.path.join(databases, f"{db_name}.db")
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    if register_regex:
        conn.create_function("REGEXP", 2, regexp)  # Registrar la función
    return cursor, conn


# ============================================================
#  2. Definir funciones principales
# ============================================================
def delete_table(table_name: str):
    cursor, conn = connect()
    queries = FichaQueries(table_name)
    cursor.execute(queries.drop_table)
    conn.commit()
    

def insert_fichas_raw():
    # Define variables
    with open(os.path.join(databases, "info_obs.json"), "r", encoding="utf-8") as file:
        info_obs = json.load(file)
    queries = FichaQueries("info_fichas")
    cursor, conn = connect("observatorio")

    # Create table if not exists
    cursor.execute(queries.create_table)

    # Iterate over fichas
    for code, metadata in info_obs.items():
        # Para campos tags que son listas, convertirlos a string
        if isinstance(metadata["tags"], list):
            metadata["tags"] = ", ".join(metadata["tags"])
        
        # Insertion
        cursor.execute(queries.insert, [
            code,
            metadata.get("titulo_corto", ""),       
            metadata.get("titulo_largo", ""),
            metadata.get("sumilla", ""),
            metadata.get("fecha_publicacion", ""),   
            metadata.get("ultima_actualizacion", ""),
            metadata.get("tags", ""),                
            metadata.get("estado", ""),              
            metadata.get("tematica", "")
        ])
    conn.commit()


def insert_fichas():
    # Defining variables
    lnk_path = os.path.join(databases, "info_obs_prueba.lnk")
    json_path = read_lnk_target(lnk_path)
    with open(json_path, "r", encoding="utf-8") as file:
        info_obs = json.load(file)
    queries = FichaQueries("info_fichas")
    cursor, conn = connect("observatorio")
    fichas = []
    fichas_not_validated = []

    ic(info_obs)
    # Create table if not exists
    cursor.execute(queries.create_table)

    for code, metadata in info_obs.items():
        # Para campos tags que son listas, convertirlos a string
        if isinstance(metadata["tags"], list):
            metadata["tags"] = ", ".join(metadata["tags"])
        
        # Validation
        try:
            ficha = Ficha(
                codigo = code,
                titulo_corto=metadata.get("titulo_corto", ""),       
                titulo_largo=metadata.get("titulo_largo", ""),
                sumilla=metadata.get("sumilla", ""),
                fecha_publicacion=metadata.get("fecha_publicacion", ""),   
                ultima_actualizacion=metadata.get("ultima_actualizacion", ""),
                tags=metadata.get("tags", ""),                
                estado=metadata.get("estado", ""),              
                tematica=metadata.get("tematica", "")
            )
            fichas.append(ficha)
        except Exception as e:
            logging.error(f"Check validation: {e}")
            fichas_not_validated.append(code)

    for ficha in fichas:
        ficha: Ficha
        ficha.clean_tags()
        cursor.execute(queries.insert, [
            ficha.codigo,
            ficha.titulo_corto,
            ficha.titulo_largo,
            ficha.sumilla,
            ficha.fecha_publicacion,
            ficha.ultima_actualizacion,
            ficha.tags,
            ficha.estado,
            ficha.tematica
        ])
    #conn.commit()

def obtain_duplicates(queries: FichaQueries):
    cursor, conn = connect("observatorio")
    cursor.execute(queries.select_duplicates)
    duplicates = cursor.fetchall()
    len(duplicates)
    logging.info(duplicates)


def validate_codes(from_json: bool = True, table_name: str = "",):
    if from_json:
        lnk_path = os.path.join(databases, "info_obs_prueba.lnk")
        json_path = read_lnk_target(lnk_path)
        with open(json_path, "r", encoding="utf-8") as file:
            data_raw = json.load(file)

        data = [code for code in data_raw.keys()]

    elif table_name:
        cursor, conn = connect("observatorio")
        cursor.execute(f"SELECT codigo FROM {table_name}")
        data_raw = cursor.fetchall()

        data = [row[0] for row in data_raw]

    data_validated = []
    data_not_validated = []
    for code in data:
        try:
            ficha = Ficha(codigo=str(code))
            data_validated.append(ficha.model_dump())
        except ValueError as e:
            logging.error(e)
            data_not_validated.append(code)
            continue

    ic(len(data_validated))
    ic(len(data_not_validated))


def filter_fichas():
    # Defining variables
    queries = FichaQueries("fichas")
    cursor, conn = connect("observatorio")
    ficha_regex = FichaRegex()

    # Fetching data from queries
    cursor.execute(f"{queries.select_where_regex(ficha_regex.tendencia_territorial)}")
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['codigo', 'titulo_corto', 'titulo_largo', 'sumilla', 'fecha_publicacion', 
                                     'ultima_actualizacion', 'tags', 'estado', 'tematica', 'vistas', 'usuarios_activos', 'eventos'])


def join_tables(new_table: str, fichas_table: str = "info_fichas", vistas_table: str = "vistas"):
    cursor, conn = connect()
    queries = FichaQueries()
    cursor.execute(queries.left_join(new_table, fichas_table, vistas_table))
    conn.commit()
    logging.info(f"Se creó la tabla '{new_table}' a partir de un left join entre '{vistas_table}' y '{fichas_table}'")


def add_rubro_subrubro(table_name: str = "fichas_vistas", generate_excel: bool = False):
    # Defining variables
    queries = FichaQueries("fichas_vistas")
    cursor, conn = connect("observatorio")
    with open(os.path.join(databases, "rubros_subrubros_simple.json"), "r", encoding="utf-8") as file:
        rubros_subrubros = json.load(file)

    # Fetching data from queries
    cursor.execute(queries.select_all)
    data = cursor.fetchall()
    codigos = [row[0] for row in data]

    rubros = []
    subrubros = []

    for codigo in codigos:
        found = False
        for rubro, details in rubros_subrubros.items():
            for subrubro, regex in details.items():
                if re.match(regex, codigo):
                    found = True
                    rubros.append(rubro)
                    subrubros.append(subrubro)
                    break
            if found:
                break    
        if not found:
            rubros.append("")
            subrubros.append("")

    cursor.execute(queries.add_column("rubro"))
    cursor.execute(queries.add_column("subrubro"))
                        
    for rubro, subrubro, codigo in zip(rubros, subrubros, codigos):
        cursor.execute(f"""
                        UPDATE {table_name}
                        SET rubro = ?, subrubro = ?
                        WHERE codigo = ?""", (rubro, subrubro, codigo))
    
    conn.commit()
    logging.info(f"Se añadieron rubros y subrubros a la tabla '{table_name}'")

    if generate_excel:
        df = pd.DataFrame(data, columns=['codigo', 'titulo_corto', 'titulo_largo', 'sumilla', 'fecha_publicacion', 
                                        'ultima_actualizacion', 'tags', 'estado', 'tematica', 'vistas', 'usuarios_activos', 'eventos'])
        df["rubro"] = rubros
        df["subrubro"] = subrubros

        df.to_excel(os.path.join(databases, "fichas_rubro_subrubro.xlsx"), index=False)

def exportar_tabla(table_name: str = "ficha_vistas"):
    # Defining variables
    queries = FichaQueries(table_name)
    cursor, conn = connect("observatorio")
    path = os.path.join(databases, f"{table_name}.xlsx")

    df = pd.read_sql_query(queries.select_all, conn)
    df.to_excel(path, index=False)
    conn.close()

    logging.info(f"Se exportó la tabla '{table_name} en {path}")


# AÑADIR COLUMNAS (SOLO TENDENCIAS TERRITORIALES)
# def add_columns():
#     # Defining variables
#     queries = FichaQueries("fichas")
#     cursor, conn = connect("observatorio")
#     with open(os.path.join(datasets, "rubros_subrubros.json"), "r", encoding="utf-8") as file:
#         rubros_subrubros = json.load(file)

#     # Fetching data from queries
#     cursor.execute(queries.select_all)
#     rubro = []
#     subrubro = []
#     departamentos = []
#     codigos = df["codigo"].tolist()

#     territoriales = rubros_subrubros["Tendencias"]["Tendencias Territoriales"]
#     for codigo in codigos:
#         departamento_encontrado = False
#         for departamento, regex in territoriales.items():
#             if re.match(regex, codigo):
#                 departamento_encontrado = True
#                 departamentos.append(departamento)
#                 break
#         if not departamento_encontrado:
#             departamentos.append("Otro")
#     ic(len(departamentos))
#     df["departamento"] = departamentos

#     df.to_excel(os.path.join(datasets, "fichas.xlsx"), index=False)


#cursor.execute(queries.create_table("info_fichas_new"))
# with open(os.path.join(datasets, "info_obs_list_prueba.json"), "w", encoding="utf-8") as file:
#     json.dump(info_obs_final, file, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    #obtain_duplicates(FichaQueries("ficha"))
    #delete_table("fichas")
    #add_rubro_subrubro()
    #join_tables("fichas_vistas", fichas_table="info_fichas", vistas_table="vistas")
    validate_codes(table_name="fichas_vistas")
    #insert_fichas_raw()
    #insert_fichas()
    #exportar_tabla("fichas_vistas")

    #cursor.execute(queries.create_table)
