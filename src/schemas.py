# src/schemas.py

from pydantic import BaseModel


class HellowRequest(BaseModel):
    names: list[str]