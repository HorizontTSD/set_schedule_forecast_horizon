from pydantic import BaseModel, RootModel
from typing import List, Dict, Any, Optional
from datetime import datetime


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


class ScheduleForecastingFullResponse(BaseModel):
    id: int
    organization_id: int
    connection_id: int
    data_name: str
    source_table: str
    time_column: str
    target_column: str
    discreteness: Optional[str]
    count_time_points_predict: Optional[int]
    target_db: Optional[str]
    methods_predict: Optional[List[dict]]
    is_deleted: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True


class TextTranslation(BaseModel):
    en: str
    ru: str
    zh: str
    it: str
    fr: str
    de: str


class LegendLine(BaseModel):
    text: TextTranslation
    color: str


class Legend(BaseModel):
    last_know_data_line: LegendLine
    real_data_line: LegendLine
    LSTM_data_line: LegendLine
    XGBoost_data_line: LegendLine
    Ensemble_data_line: LegendLine


class MapData(BaseModel):
    data: Any
    last_know_data: Any
    legend: Legend


class MetricTableText(BaseModel):
    en: str
    ru: str
    zh: str
    it: str
    fr: str
    de: str


class MetricTable(BaseModel):
    metrics_table: Any
    text: MetricTableText


class MetrixTables(BaseModel):
    XGBoost: MetricTable
    LSTM: MetricTable


class SensorData(BaseModel):
    description: dict
    map_data: dict
    table_to_download: list
    metrix_tables: dict


class Sensor(RootModel):
    root: Dict[str, "SensorData"]


class GenerateResponse(RootModel):
    root: List[Sensor]


class MethodMetrics(BaseModel):
    MAE: float
    RMSE: float
    R2: float
    MAPE: float


class MetricsResponse(BaseModel):
    metrics: Dict[str, MethodMetrics]


class DateRangeResponse(BaseModel):
    earliest_date: datetime
    max_date: datetime
    start_default_date: datetime
    end_default_date: datetime


class MetricsByMethod(BaseModel):
    MAE: float
    RMSE: float
    R2: float
    MAPE: float


class MetricsResponse(BaseModel):
    metrics: dict[str, MetricsByMethod]


class GenerateDateResponse(RootModel):
    root: Dict[str, DateRangeResponse]


class ForecastMethodsResponse(BaseModel):
    methods: list[str]
