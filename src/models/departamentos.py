import unicodedata

def eliminar_acentos(texto: str)-> str:
    # Normaliza el texto en la forma NFKD (descompone los caracteres acentuados)
    texto_normalizado = unicodedata.normalize('NFKD', texto)
    # Filtra solo los caracteres que no son signos diacríticos
    texto_sin_acentos = ''.join(c for c in texto_normalizado if not unicodedata.combining(c))
    return texto_sin_acentos


class Departamentos:
    """Clase para gestión centralizada de departamentos y códigos"""
    
    _DATA = {
        "Amazonas": "ama",
        "Ancash": "an",
        "Apurímac": "apu",
        "Arequipa": "are",
        "Ayacucho": "aya",
        "Cajamarca": "caj",
        "Callao": "callao",
        "Cusco": "cus",
        "Huancavelica": "hcv",
        "Huanuco": "hnc",
        "Ica": "ica",
        "Junín": "jun",
        "La Libertad": "lali",
        "Lambayeque": "lamb",
        "Lima Metropolitana": "lmt",
        "Lima Region": "lim",
        "Loreto": "lore",
        "Madre de Dios": "madre",
        "Moquegua": "moq",
        "Pasco": "pas",
        "Piura": "piu",
        "Puno": "pun",
        "San Martín": "smt",
        "Tacna": "tac",
        "Tumbes": "tum",
        "Ucayali": "uca"
    }

    # TODO: Agregar aliases
    @classmethod
    def get_codigo(cls, nombre_departamento: str) -> str:
        """Obtiene el código del departamento (case-insensitive con fuzzy matching)"""
        nombre = eliminar_acentos(nombre_departamento).strip().lower()
        
        # Búsqueda exacta
        for dpto, codigo in cls._DATA.items():
            dpto = eliminar_acentos(dpto.strip().lower())
            if dpto == nombre:
                return codigo
        
        # Búsqueda parcial como fallback
        # for dpto, codigo in cls._DATA.items():
        #     if nombre in dpto.lower():
        #         return codigo
        
        raise ValueError(
            f"Departamento no encontrado: '{nombre_departamento}'. "
            f"Opciones válidas: {list(cls._DATA.keys())}"
        )

    @classmethod
    def get_nombre(cls, codigo_departamento: str) -> str:
        """Obtiene el nombre completo del departamento a partir de su código"""
        codigo = codigo_departamento.strip().lower()
        
        for nombre, cod in cls._DATA.items():
            if cod == codigo:
                return nombre
        
        raise ValueError(
            f"Código no encontrado: '{codigo_departamento}'. "
            f"Opciones válidas: {list(cls._DATA.values())}"
        )

    @classmethod
    def normalize_departamento(cls, nombre_departamento: str) -> str:
        """Normaliza y valida el nombre del departamento"""
        for dpto in cls._DATA.keys():
            if eliminar_acentos(nombre_departamento).strip().lower() == eliminar_acentos(dpto).strip().lower():
                return dpto
        
        raise ValueError(
            f"Departamento no encontrado: '{nombre_departamento}'. "
            f"Opciones válidas: {list(cls._DATA.keys())}"
        )
    
    @classmethod
    def listar_departamentos(cls) -> list[str]:
        """Devuelve una lista ordenada de todos los nombres de departamentos"""
        return sorted(cls._DATA.keys())

    @classmethod
    def listar_codigos(cls) -> list[str]:
        """Devuelve una lista ordenada de todos los códigos"""
        return sorted(cls._DATA.values())

    @classmethod
    def get_dict(cls) -> dict:
        """Devuelve una copia del diccionario original"""
        return cls._DATA.copy()
    