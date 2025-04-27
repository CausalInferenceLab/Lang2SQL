from typing import List

from pydantic import BaseModel


class Persona(BaseModel):
    name: str
    department: str
    role: str
    background: str


class PersonaList(BaseModel):
    personas: List[Persona]
