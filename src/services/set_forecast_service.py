import logging
from typing import Any, Dict, List

import pandas as pd
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from src.core.security.password import decrypt_password
from src.models.organization_models import ConnectionSettings, ScheduleForecasting
from src.models.user_models import ForecastModel

from src.schemas import (ForecastConfigResponse, ForecastConfigRequest,
                         ScheduleForecastingResponse, DeleteForecastResponse, ForecastMethodsResponse,
                         FetchSampleDataRequest, FetchSampleResponse)
from src.session import db_manager


logger = logging.getLogger(__name__)


def calculate_time_interval(df: pd.DataFrame, time_column: str) -> int:
    """
    Вычисляет средний временной интервал в секундах между записями.

    :param df: DataFrame с временными метками
    :param time_column: Название колонки с временными метками
    :return: Средний временной интервал в секундах
    """
    df = df.sort_values(time_column).reset_index(drop=True)
    df[time_column] = pd.to_datetime(df[time_column])
    time_interval = df[time_column].diff().dt.total_seconds().iloc[1:].mean()
    return round(time_interval)


async def fetch_postgres_sample_data(
        username: str,
        password: str,
        host: str,
        port: int,
        db_name: str,
        table_name: str,
        time_column: str,
        target_column: str,
        limit: int = 100
) -> List[Dict]:
    db_url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_async_engine(db_url)
    sample_data: List[Dict] = []

    try:
        async with engine.connect() as conn:
            query = text(
                f'SELECT "{time_column}", "{target_column}" '
                f'FROM "{table_name}" '
                f'ORDER BY "{time_column}" DESC '
                f'LIMIT {limit}'
            )
            result = await conn.execute(query)
            rows = result.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail="В таблице нет данных")

            columns = result.keys()
            for row in rows:
                sample_data.append(dict(zip(columns, row)))
    except Exception as e:
        logger.error(f"Ошибка при выборке данных из PostgreSQL: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось получить данные из таблицы")
    finally:
        await engine.dispose()

    return sample_data


async def fetch_postgres_table_count(
        username: str,
        password: str,
        host: str,
        port: int,
        db_name: str,
        table_name: str
) -> int:
    db_url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_async_engine(db_url)

    try:
        async with engine.connect() as conn:
            query = text(f'SELECT COUNT(*) AS cnt FROM "{table_name}"')
            result = await conn.execute(query)
            row = result.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Не удалось получить количество записей")
            return row[0]
    except Exception as e:
        logger.error(f"Ошибка при подсчёте записей в PostgreSQL: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось получить количество записей в таблице")
    finally:
        await engine.dispose()


async def get_seconds(horizon_count: int, time_interval: str) -> int:
    mapping = {
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "month": 2592000
    }
    if time_interval not in mapping:
        raise ValueError(f"Некорректный интервал времени: {time_interval}")
    return horizon_count * mapping[time_interval]


async def fetch_sample_data_and_discreteness(payload: FetchSampleDataRequest, organization_id: int) -> FetchSampleResponse:

    async with db_manager.get_db_session() as session:
        stmt = select(ConnectionSettings).where(
            ConnectionSettings.id == payload.connection_id,
            ConnectionSettings.organization_id == organization_id,
            ConnectionSettings.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        connection = result.scalar_one_or_none()

        if not connection:
            raise HTTPException(status_code=404, detail="Соединение не найдено или не принадлежит организации")

        if connection.connection_schema.lower() != "postgresql":
            raise HTTPException(status_code=400, detail=f"Схема {connection.connection_schema} пока не поддерживается")

        try:
            password = decrypt_password(connection.db_password)
            sample_data = await fetch_postgres_sample_data(
                username=connection.db_user,
                password=password,
                host=connection.host,
                port=connection.port,
                db_name=connection.db_name,
                table_name=payload.source_table,
                time_column=payload.time_column,
                target_column=payload.target_column
            )

            if not sample_data:
                raise HTTPException(status_code=404, detail="В таблице нет данных ")

            df = pd.DataFrame(sample_data)
            discreteness = calculate_time_interval(df=df, time_column=payload.time_column)

        except Exception as e:
            logger.error(f"Ошибка при получении данных: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Не удалось получить данные")

        return FetchSampleResponse(
            sample_data=sample_data,
            discreteness=discreteness
        )


async def create_forecast_config(payload: ForecastConfigRequest, organization_id: int) -> ForecastConfigResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(ConnectionSettings).where(
            ConnectionSettings.id == payload.connection_id,
            ConnectionSettings.organization_id == organization_id,
            ConnectionSettings.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        connection = result.scalar_one_or_none()

        if not connection:
            raise HTTPException(status_code=404, detail="Соединение не найдено или не принадлежит организации")

        if connection.connection_schema.lower() != "postgresql":
            raise HTTPException(status_code=400, detail=f"Схема {connection.connection_schema} пока не поддерживается")

        stmt_name = select(ScheduleForecasting).where(
            ScheduleForecasting.organization_id == organization_id,
            ScheduleForecasting.data_name == payload.data_name,
            ScheduleForecasting.is_deleted.is_not(True)
        )
        result_name = await session.execute(stmt_name)
        if result_name.first():
            raise HTTPException(status_code=400, detail=f"Прогноз с именем '{payload.data_name}' уже существует")

        # Проверка 2: совпадение connection_id, time_column, target_column
        stmt_unique = select(ScheduleForecasting).where(
            ScheduleForecasting.organization_id == organization_id,
            ScheduleForecasting.connection_id == payload.connection_id,
            ScheduleForecasting.time_column == payload.time_column,
            ScheduleForecasting.target_column == payload.target_column,
            ScheduleForecasting.is_deleted.is_not(True)
        )
        result_unique = await session.execute(stmt_unique)
        if result_unique.first():
            raise HTTPException(status_code=400, detail="Настройка прогноза с таким соединением, колонкой времени и целевой колонкой уже существует")

        try:
            password = decrypt_password(connection.db_password)

            count_data = await fetch_postgres_table_count(
                    username=connection.db_user,
                    password=password,
                    host=connection.host,
                    port=connection.port,
                    db_name=connection.db_name,
                    table_name=payload.source_table,
            )

            if count_data < 10:
                raise HTTPException(status_code=404, detail="В таблице недостаточно или нет данных ")

            methods_predict = [
                {
                    "method": method,
                    "target_table": f"_{organization_id}_{payload.connection_id}_{method}_target_{payload.target_column}_{payload.source_table}"
                }
                for method in payload.methods
            ]
            discreteness = payload.discreteness

            seconds = await get_seconds(
                horizon_count=payload.horizon_count, time_interval=payload.time_interval)

            count_time_points_predict = int(seconds/discreteness)

            if count_time_points_predict < 5: # минимальная длина предсказания в точках
                count_time_points_predict = 5

            if count_time_points_predict > 5000: # максимальная длина предсказания в точках
                count_time_points_predict = min(5000, int(count_data * 0.2))

            new_forecast = ScheduleForecasting(
                organization_id=connection.organization_id,
                connection_id=connection.id,
                data_name=payload.data_name,
                source_table=payload.source_table,
                time_column=payload.time_column,
                target_column=payload.target_column,
                discreteness=discreteness,
                count_time_points_predict=count_time_points_predict,
                target_db=payload.target_db,
                methods_predict=methods_predict,
                is_deleted=False
            )
            session.add(new_forecast)
            await session.commit()

        except Exception as e:
            logger.error(f"Ошибка при создании конфигурации прогноза: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Не удалось создать конфигурацию прогноза")

        return ForecastConfigResponse(
            success=True,
            message="Настройка успешно создана"
        )



async def get_forecast_configs(organization_id: int) -> ForecastConfigResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(ScheduleForecasting).where(
            ScheduleForecasting.organization_id == organization_id,
            ScheduleForecasting.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        configs: List[ScheduleForecasting] = result.scalars().all()

        if not configs:
            raise HTTPException(status_code=404, detail="Настройки прогноза не найдены")

        configs_data = [ScheduleForecastingResponse(
            id=cfg.id,
            organization_id=cfg.organization_id,
            connection_id=cfg.connection_id,
            data_name=cfg.data_name
        ) for cfg in configs]

        return configs_data


async def delete_forecast(org_id: int, forecast_id: int) -> DeleteForecastResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(ScheduleForecasting).where(
            ScheduleForecasting.id == forecast_id,
            ScheduleForecasting.organization_id == org_id,
            ScheduleForecasting.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        forecast = result.scalar_one_or_none()

        if not forecast:
            raise HTTPException(status_code=404, detail="Настройка прогноза не найдена или уже удалена")

        forecast.is_deleted = True
        session.add(forecast)
        await session.commit()

        return DeleteForecastResponse(
            success=True,
            message=f"Настройка прогноза успешно удалена"
        )

async def get_forecast_methods() -> ForecastMethodsResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(ForecastModel.method).where(
            ForecastModel.is_deleted.is_(False),
            ForecastModel.is_active.is_(True)
        )
        result = await session.execute(stmt)
        methods: list[str] = [row[0] for row in result.all()]

        if not methods:
            raise HTTPException(status_code=404, detail="Методы прогнозирования не найдены")

        return ForecastMethodsResponse(methods=methods)




