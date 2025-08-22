# src/server.py
import multiprocessing

import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPBearer

from src.core.configuration.config import settings
from src.core.logger import logger
from src.api.api_routers import api_router
from src.core.token import token_validator


API_PREFIX = "/" + settings.SERVICE_NAME

# Загрузка переменных окружения
load_dotenv()

logger.info("Starting microservice main forecast")

# Настройка CORS
origins = ["http://localhost", "http://77.37.136.11"] if settings.PUBLIC_OR_LOCAL == "LOCAL" else ["http://77.37.136.11"]

# Определение количества воркеров
workers = multiprocessing.cpu_count()
logger.info(f"[WORKERS] Count workers = {workers}")

# Инициализация системы безопасности
security = HTTPBearer()


# Создание FastAPI приложения
docs_url = "/docs"
app = FastAPI(
    docs_url=docs_url,
    openapi_url="/openapi.json",
    root_path=API_PREFIX,
    dependencies=[Depends(token_validator)] if settings.VERIFY_TOKEN else []
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Validation error: {exc.errors()} on {request.url}")
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "detail": exc.errors(),
            "body": exc.body,
            "message": "Ошибка валидации входных данных. Проверьте формат запроса."
        },
    )

# Добавление middleware для CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключение роутеров API
app.include_router(api_router, prefix="/api/v1")


@app.get("/")
def read_root():
    logger.info("Root endpoint accessed.")
    return {"message": "Welcome to the Horizon System API"}

if __name__ == "__main__":
    try:
        logger.info(f"Starting server on http://{settings.HOST}:{settings.PORT}")
        print(f'🚀 Документация http://0.0.0.0:{settings.PORT}{API_PREFIX}/docs')
        uvicorn.run(
            "src.server:app",
            host=settings.HOST,
            port=settings.PORT,
            workers=4,
            # log_level="debug",
        )
    except Exception as e:
        logger.error(f"Failed to start server: {e}")