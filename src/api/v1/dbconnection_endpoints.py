# src/api/v1/get_users_by_org.py
from fastapi import APIRouter, HTTPException, Depends, Body, Path
from src.core.token import jwt_token_validator
from src.schemas import (CreateDBConnectionResponse, CreateDBConnectionRequest,
                         DeleteDBConnectionResponse, DBConnectionListResponse, TablesListResponse, ColumnsListResponse)
from src.core.logger import logger
from src.services.dbconnection_service import create_dbconnection, delete_dbconnection, dbconnection_list, get_connection_tables, get_connection_table_columns

router = APIRouter()


@router.post(
    "/create",
    response_model=CreateDBConnectionResponse,
    summary="Get organization's users"
)
async def func_create_dbconnection(
        payload: CreateDBConnectionRequest = Body(
            ...,
            example={
                "connection_schema": "PostgreSQL",
                "connection_name": "Тестовое соеденение",
                "db_name": "my_database",
                "host": "localhost",
                "port": 5432,
                "ssl": True,
                "db_user": "postgres",
                "db_password": "password123"
            }
        ),
        user: dict = Depends(jwt_token_validator)
):
    """
    Эндпоинт для создания соединения с базой данных организации.

    Description:
    - Создает новое соединение с указанной базой данных
    - Проверяет роль пользователя (только admin или superuser)
    - Проверяет наличие права 'connection.create'
    - Возвращает подтверждение успешного подключения или текст ошибки

    Parameters:
    - **payload** (CreateDBConnectionRequest, body): Параметры подключения к БД

    Raises:
    - **HTTPException 403**: Если пользователь не имеет роли или права на создание соединения
    - **HTTPException 500**: Если произошла ошибка при создании соединения
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if not "connection.create" in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await create_dbconnection(org_id=organization_id, payload=payload)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении пользователей организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось создать соеденение")


@router.delete(
    "/delete/{connection_id}",
    response_model=DeleteDBConnectionResponse,
    summary="Удаление соединения организации"
)
async def func_delete_dbconnection(
        connection_id: int = Path(..., description="ID соединения для удаления"),
        user: dict = Depends(jwt_token_validator)
):
    """
    Эндпоинт для логического удаления соединения с базой данных организации.

    Description:
    - Проверяет роль пользователя (только admin или superuser)
    - Проверяет наличие права 'connection.delete'
    - Устанавливает флаг is_deleted=True для соединения
    - Проверяет, что соединение принадлежит организации пользователя

    Parameters:
    - **connection_id** (int, path): ID соединения

    Raises:
    - **HTTPException 403**: Если пользователь не имеет роли или права на удаление
    - **HTTPException 404**: Если соединение не найдено или уже удалено
    - **HTTPException 500**: Если произошла ошибка при удалении
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.delete" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await delete_dbconnection(org_id=organization_id, connection_id=connection_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при удалении соединения {connection_id} для организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось удалить соединение")


@router.get(
    "/list",
    response_model=DBConnectionListResponse,
    summary="Получение списка соединений организации"
)
async def func_list_dbconnections(
        user: dict = Depends(jwt_token_validator)
):
    """
    Эндпоинт для получения списка соединений организации.

    Description:
    - Проверяет роль пользователя (admin или superuser)
    - Проверяет наличие права 'connection.view'
    - Возвращает список соединений для организации пользователя

    Raises:
    - **HTTPException 403**: Если пользователь не имеет роли или права на просмотр
    - **HTTPException 404**: Если соединения не найдены
    - **HTTPException 500**: Если произошла ошибка при получении списка
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id", None)

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.view" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await dbconnection_list(org_id=organization_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка соединений для организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось получить список соединений")


@router.get(
    "/{connection_id}/tables",
    response_model=TablesListResponse,
    summary="Получение списка таблиц соединения"
)
async def func_get_connection_tables(
        connection_id: int = Path(..., description="ID соединения"),
        user: dict = Depends(jwt_token_validator)
):
    """
    Эндпоинт для получения списка таблиц по соединению организации.

    - Проверяет роль пользователя (admin или superuser)
    - Проверяет наличие права 'connection.view'
    - Проверяет, что соединение принадлежит организации пользователя
    - Возвращает список таблиц соединения

    Raises:
    - HTTPException 403: если пользователь не имеет роли или права на просмотр
    - HTTPException 404: если соединение не найдено
    - HTTPException 500: если произошла ошибка при получении таблиц
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id")

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.view" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await get_connection_tables(connection_id=connection_id, org_id=organization_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка таблиц для соединения {connection_id} организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось получить список таблиц")



@router.get(
    "/{connection_id}/{table_name}/columns",
    response_model=ColumnsListResponse,
    summary="Получение списка колонок таблицы соединения"
)
async def func_get_connection_table_columns(
        connection_id: int = Path(..., description="ID соединения"),
        table_name: str = Path(..., description="Название таблицы"),
        user: dict = Depends(jwt_token_validator)
):
    """
    Эндпоинт для получения списка колонок таблицы по соединению организации.

    - Проверяет роль пользователя (admin или superuser)
    - Проверяет наличие права 'connection.view'
    - Проверяет, что соединение принадлежит организации пользователя
    - Возвращает список колонок таблицы соединения

    Raises:
    - HTTPException 403: если пользователь не имеет роли или права на просмотр
    - HTTPException 404: если соединение или таблица не найдены
    - HTTPException 500: если произошла ошибка при получении колонок
    """

    permissions = user.get("permissions", [])
    roles = user.get("roles", [])
    organization_id = user.get("organization_id")

    if not any(role in ["admin", "superuser"] for role in roles):
        raise HTTPException(status_code=403, detail="У вас нет роли для этой операции")

    if "connection.view" not in permissions:
        raise HTTPException(status_code=403, detail="У вас нет доступа для этой операции")

    try:
        return await get_connection_table_columns(
            connection_id=connection_id,
            table_name=table_name,
            org_id=organization_id
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении колонок таблицы {table_name} соединения {connection_id} организации {organization_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Не удалось получить список колонок")