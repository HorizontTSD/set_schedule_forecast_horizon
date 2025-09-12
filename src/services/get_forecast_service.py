import json
import logging
import pandas as pd
from src.utils.calc_error_metrix import metrix_all

from fastapi import HTTPException
from sqlalchemy import select, text

from src.models.organization_models import ConnectionSettings
from src.models.organization_models import ScheduleForecasting
from src.core.security.password import decrypt_password
from src.session import db_manager, DBManager
from src.schemas import GenerateResponse

from datetime import datetime

logger = logging.getLogger(__name__)


LIMIT = 300 # Лимит на выгрузку строк

async def get_forecast_config_by_name_full(data_name: str, organization_id: int):
    async with db_manager.get_db_session() as session:
        stmt = select(ScheduleForecasting).where(
            ScheduleForecasting.data_name == data_name,
            ScheduleForecasting.organization_id == organization_id,
            ScheduleForecasting.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        cfg = result.scalars().first()

        if not cfg:
            raise HTTPException(status_code=404, detail="Настройки прогноза не найдены")

        return {k: v for k, v in cfg.__dict__.items() if k != "_sa_instance_state"}


async def dbconnection_by_org_and_connection(organization_id: int, connection_id: int) -> dict:
    async with db_manager.get_db_session() as session:
        stmt = select(ConnectionSettings).where(
            ConnectionSettings.organization_id == organization_id,
            ConnectionSettings.id == connection_id,
            ConnectionSettings.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        conn = result.scalars().first()

        if not conn:
            raise HTTPException(status_code=404, detail="Соединение не найдено")

        return {k: v for k, v in conn.__dict__.items() if k != "_sa_instance_state"}


async def get_table_data_df(
        table_name: str,
        source_db_manager,
        time_column: str,
        target_column: str,
        limit: int = 500,
) -> pd.DataFrame:
    async with source_db_manager.get_db_session() as session:
        table_name_safe = f'"{table_name}"'
        time_col_safe = f'"{time_column}"'
        target_col_safe = f'"{target_column}"'
        query = text(
            f'SELECT {time_col_safe}, {target_col_safe} '
            f'FROM {table_name_safe} '
            f'ORDER BY {time_col_safe} DESC '
            f'LIMIT {limit};'
        )
        try:
            result = await session.execute(query)
            rows = result.mappings().all()
            df = pd.DataFrame(rows)
            return df.sort_values(by=time_column).reset_index(drop=True) if not df.empty else df
        except Exception:
            return pd.DataFrame()

async def get_forecast_data_df_from_date(
        table_name: str,
        source_db_manager,
        time_column: str,
        target_column: str,
        first_real_date,
        limit: int = 500,
) -> pd.DataFrame:
    if isinstance(first_real_date, str):
        try:
            first_real_date = datetime.fromisoformat(first_real_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Неверный формат даты")

    async with source_db_manager.get_db_session() as session:
        table_name_safe = f'"{table_name}"'
        time_col_safe = f'"{time_column}"'
        target_col_safe = f'"{target_column}"'

        query = text(
            f'SELECT {time_col_safe}, {target_col_safe} '
            f'FROM {table_name_safe} '
            f'WHERE {time_col_safe} >= :last_date '
            f'ORDER BY {time_col_safe} ASC '
            f'LIMIT {limit};'
        )

        try:
            result = await session.execute(query, {"last_date": first_real_date})
            rows = result.mappings().all()
            df = pd.DataFrame(rows)
            return df.reset_index(drop=True)
        except Exception as e:
            logger.error("Ошибка:", e)
            return pd.DataFrame()


def method_metrix_table(df_real_data_to_comparison, df_previous_prediction_to_comparison, target_column, time_col, type):
    df_merged = metrix_all(
        col_time=time_col,
        col_target=target_column,
        df_evaluetion=df_previous_prediction_to_comparison,
        df_comparative=df_real_data_to_comparison)

    df_merged = df_merged.rename(columns={f"{target_column}_pred": type})
    data = df_merged.to_dict(orient="records")
    return data


def generane_responce(data_name, description, data, last_know_data, metrics_table_XGBoost, metrics_table_LSTM, table_to_download):
    response = []
    sensor = {}
    sensor[data_name] = {}

    map_data = {
        "data": data,
        "last_know_data": last_know_data,
        "legend": {
            "last_know_data_line": {
                "text": {
                    "en": "Last known date",
                    "ru": "Последняя известная дата",
                    "zh": "最后已知日期",
                    "it": "Ultima data conosciuta",
                    "fr": "Dernière date connue",
                    "de": "Letztes bekanntes Datum"
                },
                "color": "#A9A9A9"
            },
            "real_data_line": {
                "text": {
                    "en": "Real data",
                    "ru": "Реальные данные",
                    "zh": "真实数据",
                    "it": "Dati reali",
                    "fr": "Données réelles",
                    "de": "Echte Daten"
                },
                "color": "#0000FF"
            },
            "LSTM_data_line": {
                "text": {
                    "en": "LSTM current forecast",
                    "ru": "LSTM актуальный прогноз",
                    "zh": "LSTM 当前预测",
                    "it": "Previsione attuale LSTM",
                    "fr": "Prévision actuelle LSTM",
                    "de": "Aktuelle LSTM-Vorhersage"
                },
                "color": "#FFA500"
            },
            "XGBoost_data_line": {
                "text": {
                    "en": "XGBoost current forecast",
                    "ru": "XGBoost актуальный прогноз",
                    "zh": "XGBoost 当前预测",
                    "it": "Previsione attuale XGBoost",
                    "fr": "Prévision actuelle XGBoost",
                    "de": "Aktuelle XGBoost-Vorhersage"
                },
                "color": "#a7f3d0"
            },
            "Ensemble_data_line": {
                "text": {
                    "en": "Ensemble forecast",
                    "ru": "Ансамбль прогноз",
                    "zh": "集成预测",
                    "it": "Previsione dell'ensemble",
                    "fr": "Prévision d'ensemble",
                    "de": "Ensemble-Vorhersage"
                },
                "color": " #FFFF00"
            },
        }
    }

    metrix_tables = {
        "XGBoost": {
            "metrics_table": metrics_table_XGBoost,
            "text": {
                "en": "Forecast accuracy metrics for XGBoost",
                "ru": "Метрики точности прогноза для XGBoost",
                "zh": "XGBoost 预测准确性指标",
                "it": "Metriche di accuratezza delle previsioni per XGBoost",
                "fr": "Métriques de précision des prévisions pour XGBoost",
                "de": "Prognosegenauigkeitsmetriken für XGBoost"
            },
        },
        "LSTM": {
            "metrics_table": metrics_table_LSTM,
            "text": {
                "en": "Forecast accuracy metrics for LSTM",
                "ru": "Метрики точности прогноза для LSTM",
                "zh": "LSTM 预测准确性指标",
                "it": "Metriche di accuratezza delle previsioni per LSTM",
                "fr": "Métriques de précision des prévisions pour LSTM",
                "de": "Prognosegenauigkeitsmetriken für LSTM"
            },
        },
    }

    sensor[data_name]["description"] = description
    sensor[data_name]["map_data"] = map_data
    sensor[data_name]["table_to_download"] = table_to_download
    sensor[data_name]["metrix_tables"] = metrix_tables

    response.append(sensor)

    return response


async def data_fetcher(data_name, user) -> GenerateResponse:

    """
    Служит для получения данных для главного графика прогноза,
    который можно увидеть на странице [Horizon Tool](https://horizon-tool.ru).

    Данные извлекаются по конкретным `data_names`.
    В перспективе функция будет обращаться к базе данных, где `id` организации
    и связанные с ней настроенные датчики будут использоваться для выборки данных.

    Планируемый пайплайн работы:
    1. При регистрации пользователь создает свою организацию.
    2. Переходит во вкладку **DB connections** и настраивает подключение к БД.
    3. Выбирает таблицу, затем в ней — колонку времени и целевую метрику (таргет) для прогноза.
    4. Нажимает кнопку наподобие **"Настроить прогноз"**.
    5. На стороне сервиса (или клиента, решение будет принято позже) создается таблица
       с пометкой `forecast_<initial_table_name>`.
    6. Прогноз пишется в `forecast_<initial_table_name>`

    При вызове этого метода:
    - Из основной таблицы организации берутся *n* последних строк для левой (синей) части графика.
    - Из таблицы с прогнозом берутся *n* строк для правой (бирюзовой) части графика.

    Параметры:
        data_names (list | str): Идентификаторы датчиков, по которым необходимо получить данные.

    Возвращает:
        str: Статус выполнения с указанными `data_names`.
    """
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

        description = {"sensor_name": data_name, "data_name": None}
        time_column = data.get("time_column")
        target_column = data.get("target_column")
        target_db = data.get("target_db")
        source_table = data.get("source_table")
        methods_predict = data.get("methods_predict", [])

        connection_id = data.get("connection_id")
        data_connection = await dbconnection_by_org_and_connection(
            organization_id=organization_id,
            connection_id=connection_id
        )
        if not data_connection:
            raise HTTPException(status_code=404, detail="Соединение с базой данных не найдено")

        db_password = decrypt_password(data_connection.get("db_password"))

        if data_connection["connection_schema"] == "PostgreSQL":
            source_url_db = (f"postgresql+asyncpg://{data_connection['db_user']}:"
                             f"{db_password}@{data_connection['host']}:"
                             f"{data_connection['port']}/{data_connection['db_name']}")
            source_db_manager = DBManager(source_url_db)
        else:
            raise HTTPException(status_code=400, detail="Подключение не поддерживается")

        df_last_real_data = await get_table_data_df(
            table_name=source_table,
            source_db_manager=source_db_manager,
            time_column=time_column,
            target_column=target_column,
            limit=LIMIT,
        )
        if df_last_real_data.empty:
            raise HTTPException(status_code=404, detail="Нет данных для анализа")

        data_result = {}
        last_real_data = json.loads(df_last_real_data.to_json(orient="records", force_ascii=False))
        df_last_real_line = df_last_real_data.iloc[-1]

        data_result["last_real_data"] = last_real_data
        last_real_date = pd.to_datetime(df_last_real_data[time_column]).max()
        first_real_date = pd.to_datetime(df_last_real_data[time_column]).min()

        df_table_to_download = pd.DataFrame()
        metrics_table_XGBoost = {}
        metrics_table_LSTM = {}

        predicted_data_db_manager = source_db_manager if target_db == "self_host" else db_manager

        for method_predict in methods_predict:
            method = method_predict.get("method")
            target_table = method_predict.get("target_table")

            df_predict_data = await get_forecast_data_df_from_date(
                table_name=target_table,
                source_db_manager=predicted_data_db_manager,
                time_column=time_column,
                target_column=target_column,
                limit=LIMIT,
                first_real_date=first_real_date
            )

            if df_predict_data.empty:
                continue

            df_predict_data[time_column] = pd.to_datetime(df_predict_data[time_column])
            df_last_predict_data = df_predict_data[df_predict_data[time_column] <= last_real_date].reset_index(drop=True)
            df_real_predict_data = df_predict_data[df_predict_data[time_column] > last_real_date].reset_index(drop=True)
            df_table_to_download = df_real_predict_data.copy()

            df_predict_data = pd.concat([df_last_real_line.to_frame().T, df_real_predict_data], ignore_index=True)
            predict_data = json.loads(df_predict_data.to_json(orient="records", force_ascii=False))

            if method == "XGBoost" and predict_data:
                data_result["actual_prediction_xgboost"] = predict_data
                df_table_to_download = df_predict_data.copy()
                metrics_table_XGBoost = method_metrix_table(
                    df_real_data_to_comparison=df_last_real_data,
                    df_previous_prediction_to_comparison=df_last_predict_data,
                    target_column=target_column,
                    time_col=time_column,
                    type="XGBoost"
                )

            elif method == "LSTM" and predict_data:
                data_result["actual_prediction_lstm"] = predict_data
                df_table_to_download["LSTM"] = df_real_predict_data[target_column]
                metrics_table_LSTM = method_metrix_table(
                    df_real_data_to_comparison=df_last_real_data,
                    df_previous_prediction_to_comparison=df_last_predict_data,
                    target_column=target_column,
                    time_col=time_column,
                    type="XGBoost"
                )

        table_to_download = json.loads(df_table_to_download.to_json(orient="records", force_ascii=False))

        response = generane_responce(
            data_name=data_name,
            description=description,
            data=data_result,
            last_know_data=last_real_date,
            metrics_table_XGBoost=metrics_table_XGBoost,
            metrics_table_LSTM=metrics_table_LSTM,
            table_to_download=table_to_download
        )
        return response

    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=500, detail="Ошибка при обработке данных")
