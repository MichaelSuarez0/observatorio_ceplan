from .models.ficha import Ficha, FichaRegex           
from .models.observatorio import Observatorio         
from .models.departamentos import Departamentos      
from .models.queries import FichaQueries, VistasQueries  

__all__ = [
    'Ficha',
    'FichaRegex',
    'FichaQueries',
    'VistasQueries',
    'Observatorio',
    'Departamentos'
]
