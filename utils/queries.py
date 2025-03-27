from abc import ABC, abstractmethod
from observatorio_ceplan.utils.ficha import FichaRegex

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

    @property
    def left_join(self) -> str: 
        return f"""
        SELECT codigo, COUNT(*)
            FROM {self.table_name}
            GROUP BY codigo
            HAVING COUNT(*) > 1"""
        
    @property
    def left_join(self) -> str:
        return f"""
        CREATE TABLE fichas AS
        SELECT
            vistas.codigo,
            info_fichas_prueba.titulo_corto,
            info_fichas_prueba.titulo_largo,
            info_fichas_prueba.sumilla,
            info_fichas_prueba.fecha_publicacion,
            info_fichas_prueba.ultima_actualizacion,
            info_fichas_prueba.tags,
            info_fichas_prueba.estado,
            info_fichas_prueba.tematica,
            vistas.vistas,
            vistas.usuarios_activos,
            vistas.eventos
        FROM
            vistas
        LEFT JOIN
            info_fichas_prueba ON vistas.codigo = info_fichas_prueba.codigo

        """
    
    @property
    def select_duplicates(self):
        return f"""
        SELECT codigo, COUNT(*) AS cantidad
        FROM fichas
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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            id, codigo, vistas, usuarios_activos, eventos) 
            VALUES 
            (?, ?, ?, ?, ?)"""

