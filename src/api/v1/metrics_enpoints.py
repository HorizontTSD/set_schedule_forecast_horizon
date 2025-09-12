from fastapi import APIRouter, HTTPException, Depends, Body, Path
from src.core.token import jwt_token_validator

from src.services.metrix_service import fetch_possible_date_for_metrix, fetch_metrics_by_date
from src.schemas import GenerateDateResponse, MetricsResponse

router = APIRouter()



@router.get(
    "/get_possible_date_for_metrix",
)
async def get_forecast_data(
        data_name: str,
        user: dict = Depends(jwt_token_validator)) -> GenerateDateResponse:
    """
    Возвращает возможные даты для замера точности модели
    """
    permissions = user.get("permissions", [])
    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    data = await fetch_possible_date_for_metrix(user=user, data_name=data_name)

    return data



@router.get(
    "/get_metrix_by_date",
)
async def get_forecast_data(
        data_name: str,
        start_date: str,
        end_date: str,
        user: dict = Depends(jwt_token_validator)
) -> MetricsResponse:
    """
    Возвращает возможные даты для замера точности модели.

    **data_name** можно получить из api/v1/schedule_forecast/list
    **start_date** можно получить из api/v1/metrix/get_possible_date_for_metrix?data_name=
    **end_date** можно получить из api/v1/metrix/get_possible_date_for_metrix?data_name=

    Пример запроса:

        GET /set_schedule_forecast/api/v1/schedule_forecast/get_metrix_by_date?data_name=example_data_name&start_date=2025-09-01&end_date=2025-09-12
    """
    permissions = user.get("permissions", [])
    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    data = await fetch_metrics_by_date(
        user=user, data_name=data_name, start_date=start_date, end_date=end_date)

    return data