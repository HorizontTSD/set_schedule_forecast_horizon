# src/core/logger.py
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from src.core.configuration.config import settings


class LoggerManager:
    def __init__(self):
        self.LOG_DIR = Path("logs")
        self.LOG_DIR.mkdir(exist_ok=True)

        self.FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
        self.DATE_FMT = "%Y-%m-%d %H:%M:%S"

    def _add_console_handler(self, logger: logging.Logger, formatter: logging.Formatter) -> None:
        """Добавляет консольный обработчик"""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    def _add_file_handler(
            self,
            logger: logging.Logger,
            formatter: logging.Formatter,
            handler_type: str,
            level: int,
            filter_func: callable
        ) -> None:
        """
            Добавляет файловый обработчик с ротацией
            handler_type: Тип обработчика (используется в имени файла)
            level: Уровень логирования
            filter_func: Функция фильтрации записей
        """
        try:
            handler = RotatingFileHandler(
                self.LOG_DIR / f"horizon_{handler_type}.log",
                maxBytes=5 * 1024 * 1024,
                backupCount=5,
                encoding="utf-8",
            )
            handler.setLevel(level)
            handler.setFormatter(formatter)
            handler.addFilter(filter_func)
            logger.addHandler(handler)
        except Exception as e:
            print(f"Failed to setup {handler_type} file handler: {e}")

    def setup_logger(self, name: str = "horizon_forecast") -> logging.Logger:
        logger = logging.getLogger(name)
        
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt=self.FORMAT, datefmt=self.DATE_FMT)

        # Консольный хендлер (только INFO и выше)
        self._add_console_handler(logger, formatter)

        # Файловый хендлер для INFO (ротация каждые 5 МБ)
        self._add_file_handler(logger, formatter, "info", logging.INFO, lambda r: r.levelno == logging.INFO)

        # Файловый хендлер для DEBUG (ротация каждые 5 МБ)
        self._add_file_handler(logger, formatter, "debug", logging.DEBUG, lambda r: r.levelno <= logging.DEBUG)

        # Файловый хендлер для ERROR (ротация каждые 5 МБ)
        self._add_file_handler(logger, formatter, "error", logging.ERROR, lambda r: r.levelno >= logging.ERROR)

        return logger
    
logger_manager = LoggerManager()
logger = logger_manager.setup_logger()
logger.setLevel(settings.LOGGER_LEVEL)