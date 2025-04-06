from typing import Dict, Any
from pathlib import Path
import json
import importlib.resources
from functools import lru_cache

# Importaciones de módulos internos
from .utils.ficha import Ficha, FichaRegex
from .utils.queries import FichaQueries, VistasQueries

# Configuración de recursos
_RESOURCE_PACKAGE = "observatorio_ceplan.resources"
_RESOURCE_FILES = {
    'info_obs': "info_obs.json",
    'rubros_subrubros': "rubros_subrubros.json",
    'rubros_subrubros_simple': "rubros_subrubros_simple.json"
}

@lru_cache(maxsize=None)
def _load_resource(resource_name: str) -> Dict[str, Any]:
    """
    Carga un recurso JSON desde el paquete de recursos con cache.
    
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
    
    try:
        with importlib.resources.open_text(_RESOURCE_PACKAGE, _RESOURCE_FILES[resource_name]) as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Recurso no encontrado: {_RESOURCE_FILES[resource_name]}") from e

# Funciones específicas con documentación clara
# TODO: Aplicar validación pydantic al importar (o en otra función)
def load_info_obs() -> Dict[str, Any]:
    """Carga el archivo info_obs.json con información del observatorio.
    
    Returns:
        dict: Diccionario con la información de todas las fichas del observatorio
    
    Example:
        >>> info_obs = load_info_obs()
        >>> print(info_obs['t1'])
    """
    return _load_resource('info_obs')

def load_rubros_subrubros(*, simplified: bool = False) -> Dict[str, Any]:
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
    return _load_resource('rubros_subrubros_simple' if simplified else 'rubros_subrubros')

def get_resource_path(resource_name: str) -> Path:
    """Devuelve la ruta completa a un recurso para usos avanzados."""
    try:
        return Path(str(importlib.resources.files(_RESOURCE_PACKAGE).joinpath(_RESOURCE_FILES[resource_name])))
    except Exception as e:
        raise FileNotFoundError(f"No se pudo obtener ruta para {resource_name}") from e

# Exportar las clases y funciones principales
__all__ = [
    'Ficha',
    'FichaRegex',
    'FichaQueries',
    'VistasQueries',
    'load_info_obs',
    'load_rubros_subrubros',
    'get_resource_path'
]