import pandas as pd
import os
import numpy as np
import sqlite3

# Global variables
script_dir = os.path.dirname(os.path.abspath(__file__))
datasets = os.path.join(script_dir, "..", "..", "datasets")
file = os.path.join(datasets, "vistas_20240101_20250317.xlsx")

# Reading dataframe
df = pd.read_excel(file)
columns_to_insert = ["ruta", "vistas", "usuarios_activos", "eventos"] # Considerar eventos_clave
df = df[columns_to_insert]
df = df[df["ruta"].str.contains("/ficha/", case=False, na=False)]

#print(df.head())

#df.to_excel(os.path.join(products_folder, 'data-export-clean.xlsx'), index=False)
#df.iloc[:,0] = df.iloc[:,0] .apply(lambda x: x.capitalize())
#df.fillna(0, inplace=True)


# Conexión SQL
conn = sqlite3.connect(os.path.join(datasets, "vistas.db"))
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS vistas")

create_query = """ CREATE TABLE IF NOT EXISTS vistas (
    id INTEGER PRIMARY KEY,
    ruta TEXT,
    vistas INTEGER,
    usuarios_activos INTEGER,
    eventos INTEGER)
"""
insert_query = """ 
INSERT INTO vistas (ruta, vistas, usuarios_activos, eventos) 
VALUES (?, ?, ?, ?)
"""

delete_query = """ 
DELETE FROM vistas 
WHERE ruta LIKE '%adm%'
OR ruta LIKE '%test%'
"""

update_query = """ 
UPDATE vistas
SET ruta = REPLACE(REPLACE(ruta, 'ficha', ''), '/', '')
"""

# Creación tabla SQL
cursor.execute(create_query)
cursor.executemany(insert_query, df.itertuples(False, name=None))
cursor.execute(delete_query)
cursor.execute(update_query)
conn.commit()

#df.to_excel(os.path.join(products_folder, "energia_regiones.xlsx"), index= False)