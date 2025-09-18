from fastapi import APIRouter

api_router = APIRouter()

from src.api.v1.get_tables_info import router as get_tables_info

api_router.include_router(get_tables_info, prefix="/tables-info", tags=["Check Test Connection"])

from src.api.v1.dbconnection_endpoints import router as db_connections_endpoints
api_router.include_router(db_connections_endpoints, prefix="/db_connection", tags=["DB Connection Area"])


from src.api.v1.set_forecast_enpoints import router as set_forecast_enpoints
api_router.include_router(set_forecast_enpoints, prefix="/schedule_forecast", tags=["Schedule Forecast Area"])


from src.api.v1.metrics_enpoints import router as metrix_enpoints
api_router.include_router(metrix_enpoints, prefix="/metrics", tags=["Metrix Area"])
