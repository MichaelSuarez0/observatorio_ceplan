import re
import os
from pydantic import BaseModel, field_validator
from typing import Literal, Optional
from datetime import datetime
from .observatorio import Observatorio

script_dir = os.path.join(os.path.dirname(__file__))
observatorio = Observatorio()

class FichaRegex:
    # Riesgos
    riesgo_territorial = r"^r\d+_\w+"
    riesgo_nacional = r"^r\d+$"
    
    # Oportunidades
    oportunidad_territorial = r"^o\d+_\w+"
    oportunidad_nacional = r"^o\d+$"
    
    # Tendencias
    tendencia_territorial = r"^t\d+_\w+"
    tendencia_nacional = r"^t\d+$"
    tendencia_global = r"^tg\d+$"
    tendencia_sectorial = r"^ts_\d+_\w+"
    
    # Eventos futuros
    señal_debil = r"^S\d+$"
    carta_salvaje = r"^s\d+$"
    tecnologia_emergente = r"^TE_\d+$"
    
    # Otros
    megatendencias = r"^t\d+$"
    fuerzas_primarias = r"^fp\d+$"

class Vistas(BaseModel):
    vistas: Optional[int] = None
    usuarios_activos: Optional[int] = None
    eventos: Optional[int] = None


# TODO: Field for temática
# TODO: considerar tags: list[str] = Field(default_factory=list)
class Ficha(BaseModel):
    codigo: str
    titulo_corto: Optional[str | None] = None
    titulo_largo: Optional[str | None] = None
    sumilla: Optional[str | None] = None
    fecha_publicacion: Optional[datetime | str | None] = None
    ultima_actualizacion: Optional[datetime | str | None] = None
    tags: Optional[str | list[str]] = None
    estado: Optional[Literal["ACTIVO", "INACTIVO", ""]] = None
    tematica: Optional[Literal["Social", "Ambiental", "Ética", "Económica", "Tecnológica", "Política", "General"]] = None
    # rubro: Optional[Literal["Tendencias", "Megatendencias", "Riesgos", "Oportunidades", "Eventos futuros", "Fuerzas primarias"]] = None
    # subrubro: Optional[Literal[
    # "Riesgo Territorial",
    # "Riesgo nacional",
    # "Oportunidad territorial",
    # "Oportunidad nacional",
    # "Tendencia territorial",
    # "Tendencia nacional",
    # "Tendencia global",
    # "Tendencia sectorial",
    # "Señal débil",
    # "Carta salvaje",
    # "Tecnología emergente",
    # "Megatendencias",
    # "Fuerzas primarias",
    # ""
    # ]] = None
    
    @field_validator("codigo")
    @classmethod
    def validate_code(cls, value):
        rubros_subrubros = observatorio.load_rubros_subrubros()

        # Función recursiva
        def check_patterns(data: dict | str) -> bool:
            if isinstance(data, str):
                return bool(re.match(data, value))
            elif isinstance(data, dict):
                return any(check_patterns(item) for item in data.values())
            return False

        if not check_patterns(rubros_subrubros):
            raise ValueError(f"El código '{value}' no coincide con ningún patrón válido")
    
        return value
    
    @field_validator("fecha_publicacion", "ultima_actualizacion", mode="before")
    @classmethod
    def parsear_fecha(cls, value):
        return str(datetime.strptime(value, "%Y-%m-%d").date())

    @field_validator("tags", mode="before")
    @classmethod
    def clean_tags(cls, value):
        if isinstance(value, list):
            value = ", ".join(value)

        if isinstance(value, str):
            value = value.replace(".", "")
            value = value.split(",")
            value = [tag.strip() for tag in value]
            value = ", ".join(value)
        
        return value


