from contextlib import asynccontextmanager
from logging import getLogger
from typing import Dict, Tuple
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import create_engine, text


from src.db_clients.config import db_settings
from sqlalchemy.exc import SQLAlchemyError, DBAPIError


logger = getLogger(__name__)


def postgres_check_connection(credentials_data: Dict) -> Tuple[bool, str]:
    connection_schema = credentials_data.get("connection_schema", "public")
    db_name = credentials_data["db_name"]
    host = credentials_data["host"]
    port = credentials_data.get("port", 5432)
    ssl = credentials_data.get("ssl", True)
    db_user = credentials_data["db_user"]
    db_password = credentials_data["db_password"]

    ssl_mode = "require" if ssl else "disable"
    db_url = f"postgresql://{db_user}:{db_password}@{host}:{port}/{db_name}?options=-csearch_path%3D{connection_schema}&sslmode={ssl_mode}"

    engine = create_engine(db_url, echo=False)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, ""
    except DBAPIError as e:
        return False, str(e.orig)
    except SQLAlchemyError as e:
        return False, str(e)
    finally:
        engine.dispose()

class DBManager:
    def __init__(self, db_url: str):
        self.engine = create_async_engine(
            db_url,
            pool_pre_ping=True,             # проверяет соединение перед использованием
            pool_recycle=1800,              # обновляет соединение каждые 30 мин
            connect_args={"timeout": 160},
        )
        self.session_factory = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def get_db_session(self):
        async with self.session_factory() as session:
            try:
                yield session
            except DatabaseError as e:
                await session.rollback()
                logger.error(f'Ошибка подключения к базе данных: {e}')
                raise
            finally:
                await session.close()


db_manager = DBManager(db_settings.db.get_async_url())