from fastapi import APIRouter, HTTPException
from src.services.check_test_conn import check_tables_info
from src.core.logger import logger

router = APIRouter()

@router.get("/")
async def get_tables_info():
    """
    Эндпоинт для проверки подключения таблиц и количества строк.

    Description:
    - Эта ручка служит примером.
    - Проверяет все таблицы, зарегистрированные в конфиге TablesConfig.
    - Возвращает статус подключения к каждой таблице и количество строк.
    - Удаление или измененять эту ручку не нужно

    Returns:
    - **JSON**: Словарь, где ключи — имена таблиц, а значения — строки с информацией о статусе подключения и количестве строк.

    Example Response:
    ```json
    {
        "users": "Подключение успешно, строк: 42",
        "organizations": "Подключение успешно, строк: 10",
        "refresh_tokens": "Ошибка доступа к таблице: relation does not exist"
    }
    ```

    Raises:
    - **HTTPException 500**: Если произошла ошибка при подключении к базе или получении информации о таблицах.
    """
    try:
        result = await check_tables_info()
        return result
    except Exception as e:
        logger.error(f"Ошибка при получении информации о таблицах: {e}")
        raise HTTPException(
            status_code=500,
            detail="Не удалось получить информацию о таблицах",
            headers={"X-Error": str(e)},
        )
