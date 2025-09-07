from logging import getLogger
from sqlalchemy import select, func
from sqlalchemy.exc import SQLAlchemyError
from src.models.user_models import Tables
from src.session import db_manager

logger = getLogger(__name__)
tables = Tables()

async def check_tables_info():
    result = {}

    async with db_manager.get_db_session() as session:
        for attr_name, table in tables.__dict__.items():
            if attr_name.startswith("__"):
                continue
            try:
                query = select(func.count()).select_from(table)
                res = await session.execute(query)
                count = res.scalar_one()
                result[attr_name] = f"Connection OK, rows: {count}"

            except SQLAlchemyError as e:
                logger.error(f"Ошибка при доступе к таблице {attr_name}: {e}")
                result[attr_name] = f"Error accessing table: {e}"

            except Exception as e:
                logger.error(f"Внутренняя ошибка при таблице {attr_name}: {e}")
                result[attr_name] = f"Internal error: {e}"

    return result
