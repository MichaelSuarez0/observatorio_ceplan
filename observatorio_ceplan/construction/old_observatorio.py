from icecream import ic
import pandas as pd
import os
import logging
from observatorio_ceplan.utils.ficha import Ficha
from observatorio_ceplan.utils.queries import FichaQueries, VistasQueries

# Set up basic configuration for logging
logging.basicConfig(filename='sql.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variables
script_dir = os.path.dirname(os.path.abspath(__file__))
datasets = os.path.join(script_dir, "..", "datasets")
database = os.path.join(datasets, "observatorio.db")
# info_obs_path = os.path.join(datasets, "info_obs_list.json")
# info_obs_prueba_path = os.path.join(datasets, "info_obs_list_prueba.json")
#info_obs_list = os.path.join(datasets, "info_obs_list.json")

# with open(info_obs_prueba_path, "r", encoding="utf-8") as file:
#     info_obs_prueba = json.load(file)

# #Refactoring info_obs
# info_obs_final = []
# for details in info_obs_prueba.values():
#     ficha = Ficha(
#         codigo=details["codigo"],
#         titulo_corto=details["titulo_corto"],
#         titulo_largo=details["titulo_largo"],
#         sumilla=details["sumilla"],
#         fecha_publicacion=details["fecha_publicacion"],
#         ultima_actualizacion=details["ultima_actualizacion"],
#         tags=details["tags"],
#         estado=details["estado"],
#         tematica=details["tematica"]
#     )
#     ficha.convert_tags_to_list()
#     info_obs_final.append(ficha.model_dump())


# info_obs_clean = []
# for details in info_obs_prueba:
#     ficha = Ficha(
#         codigo=details["codigo"],
#         titulo_corto=details["titulo_corto"],
#         titulo_largo=details["titulo_largo"],
#         sumilla=details["sumilla"],
#         fecha_publicacion=details["fecha_publicacion"],
#         ultima_actualizacion=details["ultima_actualizacion"],
#         tags=details["tags"],
#         estado=details["estado"],
#         tematica=details["tematica"]
#     )

#     info_obs_clean.append(ficha)
