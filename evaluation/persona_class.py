from pydantic import BaseModel
from typing import List


class Persona(BaseModel):
    name: str
    department: str
    role: str
    background: str


class PersonaList(BaseModel):
    personas: List[Persona]
