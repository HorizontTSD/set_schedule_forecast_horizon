# src/api/v1/get_users_by_org.py
from fastapi import APIRouter, HTTPException, Depends, Body, Path
from src.core.token import jwt_token_validator
from src.schemas import CreateDBConnectionResponse, CreateDBConnectionRequest, DeleteDBConnectionResponse
from src.core.logger import logger
from src.services.dbconnection_service import create_dbconnection, delete_dbconnection

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