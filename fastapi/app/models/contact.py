from enum import Enum
from pydantic import BaseModel


class ContactType(str, Enum):
    owner = "owner"
    manufacturer = "manufacturer"
    operator = "operator"


class Contact(BaseModel):
    type: ContactType
    person: str | None = None
    telephone: str | None = None
    fax: str | None = None
    email: str | None = None
    web: str | None = None
    address: str | None = None
    city: str | None = None
    admin_area: str | None = None
    postal_code: str | None = None
    country: str | None = None
