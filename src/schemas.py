# src/schemas.py

from pydantic import BaseModel


class HellowRequest(BaseModel):
    names: list[str]


class CreateDBConnectionResponse(BaseModel):
    success: bool
    message: str


class CreateDBConnectionRequest(BaseModel):
    connection_schema: str
    connection_name: str
    db_name: str
    host: str
    port: int
    ssl: bool
    db_user: str
    db_password: str


class DeleteDBConnectionRequest(BaseModel):
    connection_id: int


class DeleteDBConnectionResponse(BaseModel):
    success: bool
    message: str
