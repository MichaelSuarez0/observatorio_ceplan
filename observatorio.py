from typing import Optional, Tuple
from icecream import ic
import pandas as pd
import os
import sqlite3
import json
import logging
from observatorio_ceplan import Ficha, FichaQueries
from observatorio_ceplan import FichaQueries, VistasQueries
from observatorio_ceplan import FichaRegex
import re

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


def connect(db_name: str = "observatorio"):
    database = os.path.join(databases, f"{db_name}.db")
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    conn.create_function("REGEXP", 2, regexp)  # Registrar la función
    return cursor, conn


# ============================================================
#  2. Definir funciones principales
# ============================================================
def insert_fichas_raw():
    # Define variables
    with open(os.path.join(databases, "info_obs.json"), "r", encoding="utf-8") as file:
        info_obs = json.load(file)
    queries = FichaQueries("info_fichas_prueba")
    cursor, conn = connect("observatorio")

    # Create table if not exists
    cursor.execute(queries.create_table)

    # Iterate over fichas
    for details in info_obs:
        # Para campos tags que son listas, convertirlos a string
        if isinstance(details["tags"], list):
            details["tags"] = ", ".join(details["tags"])
        
        # Insertion
        cursor.execute(queries.insert, [
            details["codigo"],
            details["titulo_corto"],
            details["titulo_largo"],
            details["sumilla"],
            details["fecha_publicacion"],
            details["ultima_actualizacion"],
            details["tags"],
            details["estado"],
            details["tematica"]
        ])
    conn.commit()


def insert_fichas():
    # Defining variables
    with open(os.path.join(databases, "info_obs_list_prueba.json"), "r", encoding="utf-8") as file:
        info_obs = json.load(file)
    queries = FichaQueries()
    cursor, conn = connect("observatorio")

    # Create table if not exists
    cursor.execute(queries.create_table("info_fichas_new"))
    #fichas = [Ficha(**ficha) for ficha in info_obs]

    #
    for ficha in info_obs:
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
    conn.commit()

def obtain_duplicates(queries: FichaQueries):
    cursor, conn = connect("observatorio")
    cursor.execute(queries.select_duplicates)
    duplicates = cursor.fetchall()
    len(duplicates)
    logging.info(duplicates)


def validate_db():
    cursor, conn = connect("observatorio")

    cursor.execute("SELECT codigo FROM fichas")
    data = cursor.fetchall()
    data_validated = []
    data_not_validated = []
    for r in data[:10]:
        ic(r)
        try:
            codigo = Ficha(codigo=r[0])
            data_validated.append(codigo.model_dump())
        except ValueError as e:
            logging.error(e)
            data_not_validated.append(r[0])
            continue

    #conn.commit()
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
    
def add_rubro_subrubro():
    # Defining variables
    queries = FichaQueries("fichas")
    cursor, conn = connect("observatorio")
    with open(os.path.join(databases, "rubros_subrubros_admin.json"), "r", encoding="utf-8") as file:
        rubros_subrubros = json.load(file)

    # Fetching data from queries
    cursor.execute(queries.select_all)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['codigo', 'titulo_corto', 'titulo_largo', 'sumilla', 'fecha_publicacion', 
                                    'ultima_actualizacion', 'tags', 'estado', 'tematica', 'vistas', 'usuarios_activos', 'eventos'])
    rubros = []
    subrubros = []
    codigos = df["codigo"].tolist()

    for codigo in codigos:
        found = False
        for rubro, details in rubros_subrubros.items():
            if rubro not in ["Megatendencias", "Fuerzas primarias"]:
                for subrubro, regex in details.items():
                    if re.match(regex, codigo):
                        found = True
                        rubros.append(rubro)
                        subrubros.append(subrubro)
                        break
                if found:
                    break
            else:
                if re.match(details, codigo):
                    found = True
                    rubros.append(rubro)
                    subrubros.append(rubro)
                    break
        if not found:
            rubros.append("")
            subrubros.append("")

    cursor.executescript("""
                   ALTER TABLE fichas ADD COLUMN rubro TEXT;
                   ALTER TABLE fichas ADD COLUMN subrubro TEXT
                   """)
    for rubro, subrubro, codigo in zip(rubros, subrubros, codigos):
        cursor.execute("""
                        UPDATE fichas
                        SET rubro = ?, subrubro = ?
                        WHERE codigo = ?""", (rubro, subrubro, codigo))
    
    conn.commit()
    logging.info("Se añadieron rubros y subrubros a la tabla 'fichas'")

    # df["rubro"] = rubros
    # df["subrubro"] = subrubros

    # df.to_excel(os.path.join(datasets, "fichas_rubro_subrubro.xlsx"), index=False)

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

    #add_rubro_subrubro()
    validate_db()

    #cursor.execute(queries.create_table)
