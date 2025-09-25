from fastapi import APIRouter, HTTPException, Depends, Body, Path, Query

from src.core.token import jwt_token_validator
from src.schemas import (ForecastConfigRequest, ForecastConfigResponse,
                         ScheduleForecastingResponse, DeleteForecastResponse,
                         FetchSampleResponse, FetchSampleDataRequest)
from src.services.set_forecast_service import (create_forecast_config, get_forecast_configs,
                                               delete_forecast, get_forecast_methods, fetch_sample_data_and_discreteness)
from src.services.get_forecast_service import data_fetcher

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
    if "schedule_forecast.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")
    methods = await get_forecast_methods()
    return methods


@router.get(
    "/fetch_sample_and_discreteness",
    response_model=FetchSampleResponse,
    summary="Получение примера и дискретности временного ряда"
)
async def func_fetch_sample_and_discreteness(
        connection_id: int = Query(..., example=3),
        data_name: str = Query(..., example="Тестовое"),
        source_table: str = Query(..., example="electrical_consumption_amurskaya_obl"),
        time_column: str = Query(..., example="datetime"),
        target_column: str = Query(..., example="vc_fact"),
        user: dict = Depends(jwt_token_validator)
):
    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.create" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    payload = FetchSampleDataRequest(
        connection_id=connection_id,
        data_name=data_name,
        source_table=source_table,
        time_column=time_column,
        target_column=target_column,
    )

    try:
        return await fetch_sample_data_and_discreteness(
            payload=payload,
            organization_id=organization_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении тестовых данных: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Ошибка при получении тестовых данных")




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
                "horizon_count": 36,
                "time_interval": "hour",
                "discreteness": 600,
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

    if "schedule_forecast.view" not in permissions:
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

    if "schedule_forecast.delete" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await delete_forecast(org_id=organization_id, forecast_id=forecast_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при удалении настройки прогноза {forecast_id} для организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось удалить настройку прогноза")



@router.get(
    "/get_forecast_data",
)
async def get_forecast_data(
        data_name: str,
        user: dict = Depends(jwt_token_validator)):
    """
        Возвращает данные о датчиках в структурированном формате.

        **data_name** можно взять из метода api/v1/schedule_forecast/list

        Формат возвращаемых данных:
        {
            "sensor_1": data,
            "sensor_2": data,
            ...
        }

        Где "data" содержит следующие ключи:

        1. **description** (описание датчика):
            - "sensor_name" (str) — отображаемое имя датчика на фронте.
            - "sensor_id" (str) — отображаемый ID датчика на фронте.

        2. **map_data** (данные для визуализации):
            - "data" (dict) — данные для отрисовки графиков:
                - "last_real_data" — последние известные реальные данные из БД.
                - "actual_prediction_lstm" — актуальный прогноз модели LSTM.
                - "actual_prediction_xgboost" — актуальный прогноз модели XGBoost.
                - "ensemble" — актуальный прогноз модели Ensemble.
            - "last_know_data" (str) — последняя известная дата в БД реальных данных.
            - "legend" (dict) — легенда к графику:
                - "last_know_data_line" (dict) — линия последней известной даты, разделяет график на "Реальные данные" и "Прогноз":
                    - "text" (dict):
                        - "en" — английский текст.
                        - "ru" — русский текст.
                    - "color" (str) — цвет линии.
                - "real_data_line" (dict) — линия реальных данных:
                    - "text" (dict):
                        - "en" — английский текст.
                        - "ru" — русский текст.
                    - "color" (str) — цвет линии.
                - "LSTM_data_line" (dict) — линия прогноза LSTM:
                    - "text" (dict):
                        - "en" — английский текст.
                        - "ru" — русский текст.
                    - "color" (str) — цвет линии.
                - "XGBoost_data_line" (dict) — линия прогноза XGBoost:
                    - "text" (dict):
                        - "en" — английский текст.
                        - "ru" — русский текст.
                    - "color" (str) — цвет линии.
                - "Ensemble_data_line" (dict) — линия прогноза Ensemble:
                    - "text" (dict):
                        - "en" — английский текст.
                        - "ru" — русский текст.
                    - "color" (str) — цвет линии.

        3. **table_to_download** (таблица прогноза) — таблица данных, доступная для скачивания пользователем.

        4. **metrix_tables** (метрики моделей за последние сутки):
            - "XGBoost" (dict) — метрики для модели XGBoost:
                - "metrics_table" (dict):
                    - "text" (dict):
                        - "en" — английский текст подписи.
                        - "ru" — русский текст подписи.
            - "LSTM" (dict) — метрики для модели LSTM:
                - "metrics_table" (dict):
                    - "text" (dict):
                        - "en" — английский текст подписи.
                        - "ru" — русский текст подписи.
        """
    permissions = user.get("permissions", [])
    if "dashboard.view" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    data = await data_fetcher(user=user, data_name=data_name)

    return data
