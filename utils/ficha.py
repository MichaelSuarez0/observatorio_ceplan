import re
from pydantic import BaseModel, field_validator
from typing import Literal, Optional
from datetime import datetime
import os
import json  

script_dir = os.path.join(os.path.dirname(__file__))

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
    tematica: Optional[Literal["Social", "Ambiental", "Ética", "Económica", "Tecnológica", "Política", "General"]] = None
    estado: Optional[Literal["Activo", "Inactivo"]] = None
    rubro: Optional[Literal["Tendencias", "Megatendencias", "Riesgos", "Oportunidades", "Eventos futuros", "Fuerzas primarias"]] = None
    subrubro: Optional[Literal[
    "Riesgo Territorial",
    "Riesgo nacional",
    "Oportunidad territorial",
    "Oportunidad nacional",
    "Tendencia territorial",
    "Tendencia nacional",
    "Tendencia global",
    "Tendencia sectorial",
    "Señal débil",
    "Carta salvaje",
    "Tecnología emergente",
    "Megatendencias",
    "Fuerzas primarias",
    ""
    ]] = None
    
    @field_validator("codigo")
    @classmethod
    def validate_code(cls, value):
        valid = False
        with open(os.path.join(script_dir, "databases", "rubros_subrubros.json"), "r", encoding="utf-8") as file:
            rubros_subrubros = json.load(file)
        for details in rubros_subrubros.values():
            if isinstance(details, dict):
                for regex in details.values():
                    #if any(re.match(regex, value) for subrubro in rubros_subrubros.values() for regex in subrubro.values()):
                    if re.match(regex, value):
                        valid = True
                        break  # No need to check further
        
        if not valid:
            raise ValueError(f"Invalid 'codigo': {value}. Must match one of the predefined formats.")
        return value
    
    @field_validator("fecha_publicacion", "ultima_actualizacion", mode="before")
    @classmethod
    def parsear_fecha(cls, value):
        return datetime.strptime(value, "%Y-%m-%d")

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


