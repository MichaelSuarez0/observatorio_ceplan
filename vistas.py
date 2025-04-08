import pandas as pd
import os
import sqlite3
from observatorio_ceplan.utils.queries import VistasQueries

# Global variables
script_dir = os.path.dirname(os.path.abspath(__file__))
resources = os.path.join(script_dir, "resources")

def read_vistas(file: str):
# Reading dataframe
    df = pd.read_excel(f"{file}.xlsx")
    try:
        df.columns = ["ruta","vistas", "usuarios_activos", "vistas_por_usuario_activo", "tiempo_interaccion_medio", "eventos", "eventos_clave", "total_ingresos"]
    except ValueError:
        df.columns = ["ruta","vistas", "usuarios_activos", "vistas_por_usuario_activo", "tiempo_interaccion_medio", "eventos", "eventos_clave"]

        columns_to_insert = ["ruta", "vistas", "usuarios_activos", "eventos"] # Considerar eventos_clave
        df = df[columns_to_insert]
    df = df[df["ruta"].str.contains("/ficha/", case=False, na=False)]
    return df


def normalizar_codigo(codigo: str):
    """Actualiza los códigos desactualizados"""
    if pd.isna(codigo):
        return codigo
    
    prefix = codigo.split('_')[0]  # Obtiene la parte antes del _
    
    if prefix.startswith('o'):
        # Para códigos 'o', mantener solo los que terminan en 'mad'
        if codigo.endswith('_mad'):
            return codigo.split('_')[0] + '_madre'
    
    elif prefix.startswith('t'):
        # Para códigos 't', mantener solo 'ama' o 'hnc'
        if codigo.endswith(('_madre', '_smt', '_lmt', '_hnc', '_ama', '_huanca')):
            return codigo # Ya está correcto
        elif '_mad' in codigo:
            return codigo.replace('mad', 'madre')
        elif '_limametr' in codigo:
            return codigo.replace('limametr', 'lmt')
        elif codigo.endswith('_huan'):
            return codigo.replace('huan', 'hnc')
        elif '_sanmar' in codigo:
            return codigo.replace('sanmar', 'smt')
        elif '_amaz' in codigo:
            return codigo.replace('amaz', 'ama')
        else:
            return codigo
    
    elif prefix.startswith('e'):
        # Para códigos 't', mantener solo 'ama' o 'hnc'
        if '_' not in codigo:
            if len(codigo) == 2 and codigo[1].isdigit() and int(codigo[1]) <= 6:
                return f"{codigo}_cp"
            elif len(codigo) in (2, 3):
                return f"{codigo}_lp"
            else:
                return codigo
        else:
            return codigo
    
    elif prefix.startswith('r'):
        # Para códigos 'r', mantener 'hcv', 'caj' o 'madre'
        if '_hua' in codigo or '_huan' in codigo:
            return codigo.split('_')[0] + '_hcv'
        elif '_caja' in codigo:
            return codigo.replace('caja', 'caj')
        elif '_madre' in codigo:
            return codigo # ya está correcto
        elif '_mad' in codigo:
            return codigo.replace('mad', 'madre')  # ya está correcto
        else:
            return codigo
        
    return codigo

#df.to_excel(os.path.join(products_folder, 'data-export-clean.xlsx'), index=False)
#df.iloc[:,0] = df.iloc[:,0] .apply(lambda x: x.capitalize())
#df.fillna(0, inplace=True)

# Conexión SQL
def update_to_db(df: pd.DataFrame):
    conn = sqlite3.connect(os.path.join(resources, "observatorio.db"))
    cursor = conn.cursor()
    cursor.execute(queries.drop_table)

    # Creación tabla SQL
    cursor.execute(queries.create_table)
    cursor.executemany(queries.insert, df.itertuples(False, name=None))
    cursor.execute(queries.delete_where_codigo)
    cursor.execute(queries.clean_code)
    conn.commit()

    return conn

def main(file: str):
    # Primera limpieza (básica)
    df = read_vistas(file)
    conn = update_to_db(df)

    # Segunda limpieza (duplicados por actualización)
    df = pd.read_sql(queries.select_all, conn)
    df['codigo'] = df["codigo"].apply(normalizar_codigo)
    df = df.groupby(by="codigo").sum(numeric_only=True).reset_index()
    conn = update_to_db(df)
    

if __name__ == "__main__":
    queries = VistasQueries("vistas") # Nombre de la tabla
    file_name = "vistas_20240101_20250317" # Más actualizada
    #file_name = "vistas_20240701_20250331"
    file = os.path.join(resources, file_name)

    main(file)