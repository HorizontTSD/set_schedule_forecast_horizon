# src/server.py
import multiprocessing
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPBearer

from src.core.configuration.config import settings
from src.core.logger import logger
from src.api.api_routers import api_router

from src.core.exceptions import register_exception_handlers

API_PREFIX = "/" + settings.SERVICE_NAME

load_dotenv()

logger.info("Starting microservice main forecast")

origins = ["http://localhost", "http://77.37.136.11"] if settings.PUBLIC_OR_LOCAL == "LOCAL" else ["http://77.37.136.11"]


workers = multiprocessing.cpu_count()
logger.info(f"[WORKERS] Count workers = {workers}")

security = HTTPBearer()

docs_url = "/docs"
app = FastAPI(
    docs_url=docs_url,
    openapi_url="/openapi.json",
    root_path=API_PREFIX
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

register_exception_handlers(app)

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
