from fastapi import APIRouter

api_router = APIRouter()

from src.api.v1.get_tables_info import router as get_tables_info

api_router.include_router(get_tables_info, prefix="/tables-info", tags=["Check Test Connection"])

from src.api.v1.dbconnection_endpoints import router as db_connections_endpoints
api_router.include_router(db_connections_endpoints, prefix="/db_connection", tags=["DB Connection Area"])