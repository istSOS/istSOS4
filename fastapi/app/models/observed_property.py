from pydantic import BaseModel


class Constraint(BaseModel):
    max: int | None = None
    role: str
    min: int | None = None
    list: list | None = None


# da verificare standard minimo per OWL
class ObservedProperty(BaseModel):
    name: str
    description: str
    definition: str
    constraint: Constraint | None = None