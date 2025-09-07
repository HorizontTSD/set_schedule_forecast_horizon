from contextlib import asynccontextmanager
from logging import getLogger

from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.db_clients.config import db_settings

logger = getLogger(__name__)


class DBManager:
    def __init__(self, db_url: str):
        self.engine = create_async_engine(db_url)
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