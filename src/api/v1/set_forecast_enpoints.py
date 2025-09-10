from fastapi import APIRouter, HTTPException, Depends, Body, Path
from src.core.token import jwt_token_validator
from src.schemas import (ForecastConfigRequest, ForecastConfigResponse,
                         ScheduleForecastingResponse, DeleteForecastResponse)
from src.services.set_forecast_service import create_forecast_config, get_forecast_configs, delete_forecast
from src.core.logger import logger

router = APIRouter()


@router.get(
    "/forecast_methods",
)
async def get_forecast_methods_list(user: dict = Depends(jwt_token_validator)):
    """
    Возвращает список возможных методов прогноза
    """
    permissions = user.get("permissions", [])
    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")
    sample_data = ["XGBoost", "LSTM"]
    return sample_data


@router.post(
    "/create",
    response_model=ForecastConfigResponse,
    summary="Создание настройки прогнозирования"
)
async def func_create_forecast_config(
        payload: ForecastConfigRequest = Body(
            ...,
            example={
                "connection_id": 3,
                "data_name": "Тестовое",
                "source_table": "electrical_consumption_amurskaya_obl",
                "time_column": "datetime",
                "target_column": "vc_fact",
                "count_time_points_predict": 10,
                "target_db": "self_host",
                "methods": [
                    "XGBoost",
                    "LSTM"
                ]
            }
        ),
        user: dict = Depends(jwt_token_validator)
):
    """
    Эндпоинт для создания настройки прогнозирования.

    Description:
    - Сохраняет параметры прогнозирования для указанной таблицы
    - Проверяет роль пользователя (только admin или superuser)
    - Проверяет наличие права 'forecast.create'
    - Возвращает подтверждение успешного создания или текст ошибки

    Raises:
    - **HTTPException 403**: Если пользователь не имеет роли или права
    - **HTTPException 500**: Если произошла ошибка при сохранении настройки
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)


    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await create_forecast_config(payload=payload, organization_id=organization_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при создании настройки прогнозирования: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось создать настройку прогнозирования")



@router.get(
    "/list",
    response_model=list[ScheduleForecastingResponse],
    summary="Получение списка настроек прогнозирования"
)
async def func_get_forecast_configs(user: dict = Depends(jwt_token_validator)):
    """
    Эндпоинт для получения списка настроек прогнозирования.

    Description:
    - Возвращает все настройки прогнозирования для организации пользователя
    - Проверяет роль пользователя (только admin или superuser)
    - Проверяет наличие права 'forecast.view'
    - Возвращает список настроек или текст ошибки

    Raises:
    - **HTTPException 403**: Если пользователь не имеет роли или права
    - **HTTPException 404**: Если настройки не найдены
    - **HTTPException 500**: Если произошла ошибка при получении данных
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для просмотра настроек")

    try:
        return await get_forecast_configs(organization_id=organization_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка настроек прогнозирования: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось получить настройки прогнозирования")


@router.delete(
    "/delete/{forecast_id}",
    response_model=DeleteForecastResponse,
    summary="Удаление настройки прогноза"
)
async def func_delete_forecast(
        forecast_id: int = Path(..., description="ID настройки прогноза для удаления"),
        user: dict = Depends(jwt_token_validator)
):
    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await delete_forecast(org_id=organization_id, forecast_id=forecast_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при удалении настройки прогноза {forecast_id} для организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось удалить настройку прогноза")