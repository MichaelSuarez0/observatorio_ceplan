from abc import ABC, abstractmethod
from observatorio_ceplan.utils.ficha import FichaRegex
from typing import Literal

class Queries(ABC):
    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = table_name

    @property
    def drop_table(self) -> str:
        return f"""DROP TABLE IF EXISTS {self.table_name}"""
    
    @property
    def select_all(self) -> str:
        return f"""SELECT * FROM {self.table_name}"""

    @property
    @abstractmethod    
    def create_table(self) -> str:
        pass

    @property
    @abstractmethod    
    def insert(self) -> str:
        pass


class FichaQueries(Queries):
    def __init__(self, table_name: str = "info_fichas_prueba"):    
        self.table_name = table_name
        self.regex = FichaRegex()

    def add_column(self, column_name: str):
        return f"""
        ALTER TABLE {self.table_name} ADD COLUMN {column_name} TEXT;
        """
        
    def join(self, new_table: str, fichas_table: str = "info_fichas", vistas_table: str = "vistas", key: Literal["left", "inner"] = "inner") -> str:
        return f"""
        CREATE TABLE {new_table} AS
        SELECT
            {vistas_table}.codigo,
            {fichas_table}.titulo_corto,
            {fichas_table}.titulo_largo,
            {fichas_table}.sumilla,
            {fichas_table}.fecha_publicacion,
            {fichas_table}.ultima_actualizacion,
            {fichas_table}.tags,
            {fichas_table}.estado,
            {fichas_table}.tematica,
            {vistas_table}.vistas,
            {vistas_table}.usuarios_activos,
            {vistas_table}.eventos
        FROM
            {vistas_table}
        {key.upper()} JOIN
            {fichas_table} ON {vistas_table}.codigo = {fichas_table}.codigo
        """
    
    @property
    def select_duplicates(self):
        return f"""
        SELECT codigo, COUNT(*) AS cantidad
            FROM {self.table_name}
            GROUP BY codigo
            HAVING COUNT(*) > 1
        """
    
    
    def select_where_regex(self, regex: str = "") -> str:
        return f"""
        {self.select_all} WHERE codigo REGEXP '{regex}'")
        """
    
    @property
    def create_table(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            codigo TEXT,
            titulo_corto TEXT,
            titulo_largo TEXT,
            sumilla TEXT,
            fecha_publicacion TEXT,
            ultima_actualizacion TEXT,
            tags TEXT,
            estado TEXT,
            tematica TEXT
            );
            """
    @property
    def insert(self):
        return f"""
        INSERT INTO {self.table_name} (
            codigo, titulo_corto, titulo_largo, sumilla, fecha_publicacion, ultima_actualizacion, tags, estado, tematica)
            VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """



class VistasQueries(Queries):
    def __init__(self, table_name: str = "vistas"):   
        self.table_name = table_name

    @property
    def create_table(self):
        return f"""
        CREATE TABLE IF NOT EXISTS {self.table_name}(
            codigo TEXT,
            vistas INTEGER,
            usuarios_activos INTEGER,
            eventos INTEGER
            )
            """
    @property
    def insert(self):
        return f"""
        INSERT INTO {self.table_name}(
            codigo, vistas, usuarios_activos, eventos) 
            VALUES 
            (?, ?, ?, ?)
        """

    @property
    def delete_where_codigo(self):
        return f"""
        DELETE FROM {self.table_name}
            WHERE codigo LIKE '%%adm%%'
            OR codigo LIKE '%%test%%'
        """
    
    @property
    def clean_code(self):
        return f"""
        UPDATE {self.table_name}
        SET codigo = REPLACE(REPLACE(codigo, 'ficha', ''), '/', '')
        """



