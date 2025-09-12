import asyncio
from datetime import timedelta

from fastapi import HTTPException

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sqlalchemy import text

from src.core.security.password import decrypt_password
from src.session import db_manager, DBManager
from src.services.get_forecast_service import (
    get_forecast_config_by_name_full,
    dbconnection_by_org_and_connection,
)
from src.schemas import MetricsResponse, GenerateDateResponse


async def get_min_max_dates(
        table_name: str,
        db_manager,
        time_column: str
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    async with db_manager.get_db_session() as session:
        table_name_safe = f'"{table_name}"'
        time_col_safe = f'"{time_column}"'
        query = text(
            f'SELECT MIN({time_col_safe}) AS min_date, MAX({time_col_safe}) AS max_date '
            f'FROM {table_name_safe};'
        )
        try:
            result = await session.execute(query)
            row = result.mappings().first()
            if row is None:
                return None, None
            return pd.to_datetime(row['min_date']), pd.to_datetime(row['max_date'])
        except Exception:
            return None, None


async def fetch_possible_date_for_metrix(user, data_name) -> GenerateDateResponse:
    response = {}

    try:
        organization_id = user.get("organization_id")
        if not organization_id:
            raise HTTPException(status_code=400, detail="Organization ID не указан в токене")

        data = await get_forecast_config_by_name_full(
            organization_id=organization_id,
            data_name=data_name
        )
        if not data:
            raise HTTPException(status_code=404, detail=f"Конфигурация не найдена")

        methods_predict = data.get("methods_predict", [])
        if not methods_predict:
            raise HTTPException(status_code=404, detail=f"Нет методов прогнозирования")

        time_column = data.get("time_column")
        source_table = data.get("source_table")
        target_db = data.get("target_db")
        connection_id = data.get("connection_id")

        data_connection = await dbconnection_by_org_and_connection(
            organization_id=organization_id,
            connection_id=connection_id
        )
        if not data_connection:
            raise HTTPException(status_code=404, detail=f"Соединение не найдено")

        db_password = decrypt_password(data_connection.get("db_password"))

        if data_connection["connection_schema"] == "PostgreSQL":
            source_url_db = f"postgresql+asyncpg://{data_connection['db_user']}:{db_password}@{data_connection['host']}:{data_connection['port']}/{data_connection['db_name']}"
            source_db_manager = DBManager(source_url_db)
        else:
            raise HTTPException(status_code=400, detail=f"Подключение со схемой {data_connection['connection_schema']} не поддерживается")

        min_date, max_date = await get_min_max_dates(
            table_name=source_table,
            db_manager=source_db_manager,
            time_column=time_column,
        )
        if min_date is None or max_date is None:
            raise HTTPException(status_code=404, detail=f"Нет данных в таблице {source_table}")

        target_db_manager = source_db_manager if target_db == "self_host" else db_manager

        earliest_date = None
        for method_predict in methods_predict:
            target_table = method_predict.get("target_table")
            min_date_predict, max_date_predict = await get_min_max_dates(
                table_name=target_table,
                db_manager=target_db_manager,
                time_column=time_column,
            )
            if min_date_predict and (earliest_date is None or min_date_predict < earliest_date):
                earliest_date = min_date_predict

        response[data_name] = {
            "earliest_date": earliest_date,
            "max_date": max_date,
            "start_default_date": max_date - timedelta(days=1),
            "end_default_date": max_date,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при получении данных")

    return response


async def fetch_data_in_range(
        table_name: str,
        db_manager,
        time_column: str,
        target_column: str,
        start_date,
        end_date
) -> pd.DataFrame:
    start_date = pd.to_datetime(start_date).to_pydatetime()
    end_date = pd.to_datetime(end_date).to_pydatetime()

    async with db_manager.get_db_session() as session:
        table_name_safe = f'"{table_name}"'
        time_col_safe = f'"{time_column}"'
        target_col_safe = f'"{target_column}"'
        query = text(
            f'''
            SELECT {time_col_safe}, {target_col_safe}
            FROM {table_name_safe}
            WHERE {time_col_safe} BETWEEN :start_date AND :end_date
            '''
        )
        result = await session.execute(query, {"start_date": start_date, "end_date": end_date})
        df = pd.DataFrame(result.mappings().all())
        if df.empty:
            return pd.DataFrame(columns=[time_column, target_column])
        return df


async def calculate_metrics(df_merged, target_column, exist_methods):
    y_true = df_merged[target_column]

    results = {}

    def _safe_metric(value):
        return 0 if value is None or (isinstance(value, float) and np.isnan(value)) else value

    for method in exist_methods:
        y_pred = df_merged[method]

        mae = _safe_metric(await asyncio.to_thread(mean_absolute_error, y_true, y_pred))
        mse = _safe_metric(await asyncio.to_thread(mean_squared_error, y_true, y_pred))
        rmse = _safe_metric(np.sqrt(mse))
        r2 = _safe_metric(await asyncio.to_thread(r2_score, y_true, y_pred))
        mape = _safe_metric(await asyncio.to_thread(np.mean, np.abs((y_true - y_pred) / y_true)) * 100)

        results[method] = {
            "MAE": round(mae, 2),
            "RMSE": round(rmse, 2),
            "R2": round(r2, 2),
            "MAPE": float(round(mape, 2)),
        }

    return results


async def fetch_metrics_by_date(user, data_name, start_date, end_date) -> MetricsResponse:
    response = []

    try:
        organization_id = user.get("organization_id")
        if not organization_id:
            raise HTTPException(status_code=400, detail="Ошибка запроса")

        data = await get_forecast_config_by_name_full(
            organization_id=organization_id,
            data_name=data_name
        )
        if not data:
            raise HTTPException(status_code=404, detail="Данные не найдены")

        methods_predict = data.get("methods_predict", [])
        if not methods_predict:
            raise HTTPException(status_code=404, detail="Методы прогнозирования не найдены")

        time_column = data.get("time_column")
        target_column = data.get("target_column")
        source_table = data.get("source_table")
        target_db = data.get("target_db")
        connection_id = data.get("connection_id")

        data_connection = await dbconnection_by_org_and_connection(
            organization_id=organization_id,
            connection_id=connection_id
        )
        if not data_connection:
            raise HTTPException(status_code=404, detail="Соединение с базой данных не найдено")

        db_password = decrypt_password(data_connection.get("db_password"))

        if data_connection["connection_schema"] == "PostgreSQL":
            source_url_db = f"postgresql+asyncpg://{data_connection['db_user']}:{db_password}@{data_connection['host']}:{data_connection['port']}/{data_connection['db_name']}"
            source_db_manager = DBManager(source_url_db)
        else:
            raise HTTPException(status_code=400, detail="Подключение не поддерживается")

        df_real_data = await fetch_data_in_range(
            table_name=source_table,
            db_manager=source_db_manager,
            time_column=time_column,
            target_column=target_column,
            start_date=start_date,
            end_date=end_date
        )

        df_real_data[time_column] = pd.to_datetime(df_real_data[time_column])
        df_real_data = df_real_data.sort_values(time_column)
        df_real_data[time_column] = df_real_data[time_column].dt.tz_localize(None)

        df_merged = df_real_data.copy()
        target_db_manager = source_db_manager if target_db == "self_host" else db_manager

        tolerance = pd.Timedelta(seconds=300)
        all_methods = []

        for method_predict in methods_predict:
            target_table = method_predict.get("target_table")
            method = method_predict.get("method")
            all_methods.append(method)
            df = await fetch_data_in_range(
                table_name=target_table,
                db_manager=target_db_manager,
                time_column=time_column,
                target_column=target_column,
                start_date=start_date,
                end_date=end_date
            )
            if not df.empty:
                df[time_column] = pd.to_datetime(df[time_column])
                df = df.sort_values(time_column)
                df[time_column] = df[time_column].dt.tz_localize(None)
                df_merged = pd.merge_asof(
                    df,
                    df_merged.rename(columns={target_column: method}),
                    on=time_column,
                    direction='nearest',
                    tolerance=tolerance
                )

        exist_methods = [col for col in df_merged.columns if col not in [time_column, target_column]]
        df_merged = df_merged.dropna()
        data = await calculate_metrics(df_merged, target_column, exist_methods)
        response.append(data)

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Ошибка при обработке данных")

    return response
