from typing import Any, Literal, Tuple
from pathlib import Path
from functools import lru_cache
import re
import json
import os

# Configuración de recursos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESOURCE_DIR = os.path.join(BASE_DIR, 'resources')
_RESOURCE_FILES = {
    'info_obs': "info_obs.json",
    'rubros_subrubros': "rubros_subrubros.json",
    'rubros_subrubros_simple': "rubros_subrubros_simple.json"
}

class Observatorio:
    def __init__(self):
        self.info_obs = None
        self.rubros_subrubros = None

    @lru_cache(maxsize=None)
    def _load_resource(self, resource_name: str) -> dict[str, Any]:
        """
        Carga un recurso JSON desde el directorio de recursos con cache.
        
        Args:
            resource_name: Nombre clave del recurso (debe estar en _RESOURCE_FILES)
        
        Returns:
            Diccionario con los datos del JSON
        
        Raises:
            FileNotFoundError: Si el recurso no existe
            json.JSONDecodeError: Si el archivo no es JSON válido
        """
        if resource_name not in _RESOURCE_FILES:
            raise ValueError(f"Recurso no definido. Opciones: {list(_RESOURCE_FILES.keys())}")
        
        file_path = os.path.join(RESOURCE_DIR, _RESOURCE_FILES[resource_name])
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Recurso no encontrado: {file_path}") from e

    # Funciones específicas con documentación clara
    # TODO: Aplicar validación pydantic al importar (o en otra función)
    def load_info_obs(self) -> dict[str, dict[str, str]]:
        """Carga el archivo info_obs.json con información del observatorio.
        
        Returns:
            dict: Diccionario con la información de todas las fichas del observatorio
        
        Example:
            >>> info_obs = load_info_obs()
            >>> print(info_obs['t1'])
        """
        self.info_obs = self._load_resource('info_obs')
        return self.info_obs
    
    def load_info_obs_subset(
        self, 
        rubro: Literal["megatendencias", "tendencias", "riesgos", "oportunidades", "eventos", "escenarios"],
        subrubro: Literal["global", "nacional", "territorial", "carta", "señal", "tecnología", "corto plazo", "largo plazo"] | None = None,
    ) -> dict[str, dict[str, str]]:
        
        if not self.info_obs:
            self.load_info_obs()
            
        info_obs_subset = {}
        for code, details in self.info_obs.items():
            clasif = self.get_code_classification(code)
            if not subrubro:
                if clasif[0] == rubro:
                    info_obs_subset[code] = details
            else:
                if clasif[0] == rubro and subrubro in clasif[1]:
                    info_obs_subset[code] = details
        
        if not info_obs_subset:
            if not subrubro:
                raise ValueError(f"No se encontraron fichas de {rubro}, verificar typing")
            else:
                raise ValueError(f"No se encontraron fichas de {rubro} del subrubro '{subrubro}', verificar que se ingresó una combinación válida")
        
        return info_obs_subset

    def load_rubros_subrubros(self, *, simplified: bool = False) -> dict[str, Any]:
        """Carga los rubros y subrubros del sistema.
        
        Returns:
            dict: Diccionario de regex para identificar el rubro y subrubro de un código de ficha.

        Args:
            simplified: Si es True, carga la versión simplificada (sin información específica para subrubros territoriales).
        
        Example:
            >>> subrubros_simple = load_rubros_subrubros(simplified=True)
            >>> for details in subrubros_simple.values():
            >>>     for subrubro, regex in details.items():
            >>>         match = re.match(regex, 't1')
            >>>         if match:
            >>>             print(f"- t1 pertenece a {subrubro}")
            >>>             break
        """
        self.rubros_subrubros = self._load_resource('rubros_subrubros_simple') if simplified else self._load_resource('rubros_subrubros')
        return self.rubros_subrubros
    
    # TODO: Evaluar poner break
    # TODO: Evaluar el return
    def get_code_classification(self, code: str)-> Tuple[str, str, str | None]:
        """Retorna una tupla con el rubro, subrubro y departamento del código de una ficha a partir del dict rubros_subrubros.
        
        Returns:
            Tuple: tupla de 3 elementos: (rubro, subrubro, departamento). En caso de no ser territorial, departamento es None.
        
        Example:
        """
        if not self.rubros_subrubros:
            self.load_rubros_subrubros()
        
        for rubro, subdict in self.rubros_subrubros.items():
            for subrubro, regex in subdict.items():
                
                # Caso 1: nivel simple (nacional, global)
                if isinstance(regex, str) and re.match(regex, code, re.IGNORECASE):  # No diferenciar entre mayúsculas y minúsculas
                    return (rubro.lower(), subrubro.lower(), None)
                
                # Caso 2: nivel complejo (es territorial)
                if isinstance(regex, dict):
                    for departamento, true_regex in regex.items():
                        if isinstance(true_regex, str) and re.match(true_regex, code, re.IGNORECASE):
                            return (rubro.lower(), subrubro.lower(), departamento)

        #print(f"No se encontró coincidencia para: {code}")
        return (None, None, None)

    def get_resource_path(self, resource_name: str) -> Path:
        """Devuelve la ruta completa a un recurso para usos avanzados."""
        if resource_name not in _RESOURCE_FILES:
            raise ValueError(f"Recurso no definido. Opciones: {list(_RESOURCE_FILES.keys())}")
        
        file_path = os.path.join(RESOURCE_DIR, _RESOURCE_FILES[resource_name])
        resource_path = Path(file_path)
        if not resource_path.exists():
            raise FileNotFoundError(f"No se pudo obtener ruta para {resource_name}")
        return resource_path
