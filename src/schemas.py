from pydantic import BaseModel
from typing import List, Dict, Any

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


class DBConnectionResponse(BaseModel):
    id: int
    db_name: str
    connection_name: str | None


class DBConnectionListResponse(BaseModel):
    connections: list[DBConnectionResponse]


class TablesListResponse(BaseModel):
    tables: list[str]


class ColumnsListResponse(BaseModel):
    columns: list[str]


class ForecastConfigRequest(BaseModel):
    connection_id: int
    data_name: str
    source_table: str
    time_column: str
    target_column: str
    count_time_points_predict: int
    target_db: str
    methods: list[str]


class ForecastConfigResponse(BaseModel):
    success: bool
    message: str
    sample_data: list[Any]


class ScheduleForecastingResponse(BaseModel):
    id: int
    organization_id: int
    connection_id: int
    data_name: str


class DeleteForecastResponse(BaseModel):
    success: bool
    message: str


class ForecastMethodsResponse(BaseModel):
    methods: List[str]