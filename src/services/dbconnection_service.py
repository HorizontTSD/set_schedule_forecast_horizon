import logging
from fastapi import HTTPException, status
from sqlalchemy import select, insert, update, inspect
from sqlalchemy.ext.asyncio import create_async_engine

from src.core.security.password import encrypt_password, decrypt_password

from src.schemas import (
CreateDBConnectionResponse, CreateDBConnectionRequest, DeleteDBConnectionResponse,
DBConnectionListResponse, DBConnectionResponse, TablesListResponse, ColumnsListResponse
)
from src.models.organization_models import ConnectionSettings
from src.session import db_manager, postgres_check_connection

logger = logging.getLogger(__name__)


dict_db_checker_functions = {
    "PostgreSQL": postgres_check_connection
}


async def create_dbconnection(org_id: int, payload: CreateDBConnectionRequest) -> CreateDBConnectionResponse:
    connection_schema = payload.connection_schema
    func_check_test_connection = dict_db_checker_functions.get(connection_schema)
    if not func_check_test_connection:
        raise HTTPException(status_code=400, detail=f"Нет такой схемы подключения {connection_schema}")

    is_connection, db_message = func_check_test_connection(payload.dict())
    if not is_connection:
        raise HTTPException(status_code=500, detail=f"Не удалось подключиться к базе: {db_message}")

    async with db_manager.get_db_session() as session:
        try:
            # Проверка дублирующего имени
            stmt_name = select(ConnectionSettings).where(
                ConnectionSettings.organization_id == org_id,
                ConnectionSettings.is_deleted == False,
                ConnectionSettings.connection_name == payload.connection_name
            )
            result_name = await session.execute(stmt_name)
            if result_name.scalar_one_or_none():
                raise HTTPException(
                    status_code=400,
                    detail=f"Имя соединения '{payload.connection_name}' уже используется"
                )

            # Проверка дублирования соединения по параметрам
            stmt_conn = select(ConnectionSettings).where(
                ConnectionSettings.organization_id == org_id,
                ConnectionSettings.is_deleted == False,
                ConnectionSettings.db_name == payload.db_name,
                ConnectionSettings.host == payload.host,
                ConnectionSettings.port == payload.port,
                ConnectionSettings.db_user == payload.db_user
            )
            result_conn = await session.execute(stmt_conn)
            existing_connection = result_conn.scalar_one_or_none()
            if existing_connection:
                raise HTTPException(
                    status_code=400,
                    detail=f"Такое соединение уже существует: db_name='{existing_connection.db_name}', connection_name='{existing_connection.connection_name}'"
                )

            stmt_insert = insert(ConnectionSettings).values(
                organization_id=org_id,
                connection_schema=payload.connection_schema,
                connection_name=payload.connection_name,
                db_name=payload.db_name,
                host=payload.host,
                port=payload.port,
                ssl=payload.ssl,
                db_user=payload.db_user,
                db_password=encrypt_password(payload.db_password),
            )
            await session.execute(stmt_insert)
            await session.commit()
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при создании соединения для организации {org_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Не удалось создать соединение в базе данных")

    return CreateDBConnectionResponse(success=True, message="Соединение успешно создано")


async def delete_dbconnection(org_id: int, connection_id: int) -> DeleteDBConnectionResponse:
    async with db_manager.get_db_session() as session:
        try:
            stmt_check = select(ConnectionSettings).where(
                ConnectionSettings.id == connection_id,
                ConnectionSettings.organization_id == org_id,
                ConnectionSettings.is_deleted == False
            )
            result = await session.execute(stmt_check)
            connection = result.scalar_one_or_none()
            if not connection:
                raise HTTPException(status_code=404, detail="Соединение не найдено или уже удалено")

            stmt_update = (
                update(ConnectionSettings)
                .where(
                    ConnectionSettings.id == connection_id,
                    ConnectionSettings.organization_id == org_id
                )
                .values(is_deleted=True)
            )
            await session.execute(stmt_update)
            await session.commit()
        except HTTPException:
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"Ошибка при удалении соединения {connection_id} для организации {org_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Не удалось удалить соединение в базе данных")

    return DeleteDBConnectionResponse(success=True, message="Соединение успешно удалено")


async def dbconnection_list(org_id: int) -> DBConnectionListResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(
            ConnectionSettings.id,
            ConnectionSettings.db_name,
            ConnectionSettings.connection_name
        ).where(
            ConnectionSettings.organization_id == org_id,
            ConnectionSettings.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        rows = result.all()

        response_connections = [
            DBConnectionResponse(
                id=row.id,
                db_name=row.db_name,
                connection_name=row.connection_name
            )
            for row in rows
        ]

        return DBConnectionListResponse(connections=response_connections)


async def fetch_postgres_tables(username: str, password: str, host: str, port: int, db_name: str) -> TablesListResponse:
    db_url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_async_engine(db_url)

    async with engine.connect() as conn:
        tables = await conn.run_sync(lambda sync_conn: inspect(sync_conn.engine).get_table_names())

    return TablesListResponse(tables=tables)


async def get_connection_tables(connection_id: int, org_id: int) -> TablesListResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(ConnectionSettings).where(
            ConnectionSettings.id == connection_id,
            ConnectionSettings.organization_id == org_id,
            ConnectionSettings.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        connection = result.scalar_one_or_none()

        if not connection:
            raise HTTPException(status_code=404, detail="Соединение не найдено или доступ запрещен")

        if connection.connection_schema.lower() == "postgresql":
            password = decrypt_password(connection.db_password)
            return await fetch_postgres_tables(
                username=connection.db_user,
                password=password,
                host=connection.host,
                port=connection.port,
                db_name=connection.db_name
            )
        else:
            raise HTTPException(status_code=400, detail=f"Схема {connection.connection_schema} пока не поддерживается")



async def fetch_postgres_table_columns(username: str, password: str, host: str, port: int, db_name: str, table_name: str) -> ColumnsListResponse:
    from sqlalchemy.ext.asyncio import create_async_engine

    db_url = f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}"
    engine = create_async_engine(db_url)

    async with engine.connect() as conn:
        columns = await conn.run_sync(
            lambda sync_conn: [col['name'] for col in inspect(sync_conn.engine).get_columns(table_name)]
        )

    return ColumnsListResponse(columns=columns)

async def get_connection_table_columns(connection_id: int, table_name: str, org_id: int) -> ColumnsListResponse:
    async with db_manager.get_db_session() as session:
        stmt = select(ConnectionSettings).where(
            ConnectionSettings.id == connection_id,
            ConnectionSettings.organization_id == org_id,
            ConnectionSettings.is_deleted.is_not(True)
        )
        result = await session.execute(stmt)
        connection = result.scalar_one_or_none()

        if not connection:
            raise HTTPException(status_code=404, detail="Соединение не найдено или доступ запрещен")

        if connection.connection_schema.lower() == "postgresql":
            password = decrypt_password(connection.db_password)
            columns = await fetch_postgres_table_columns(
                username=connection.db_user,
                password=password,
                host=connection.host,
                port=connection.port,
                db_name=connection.db_name,
                table_name=table_name
            )

            filtered_columns = [
                col for col in columns.columns
                if col.lower() != "id" and not col.lower().startswith("id_") and not col.lower().endswith("_id")
            ]
            return ColumnsListResponse(columns=filtered_columns)
        else:
            raise HTTPException(status_code=400, detail=f"Схема {connection.connection_schema} пока не поддерживается")
