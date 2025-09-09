import logging
from fastapi import HTTPException, status
from sqlalchemy import select, insert, update

from src.core.security.password import hash_password

from src.schemas import (
CreateDBConnectionResponse, CreateDBConnectionRequest, DeleteDBConnectionRequest, DeleteDBConnectionResponse
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
                db_password=hash_password(payload.db_password),
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