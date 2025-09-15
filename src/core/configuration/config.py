# src/core/configuration/config.py
import logging
from environs import Env


class Settings:
    def __init__(self):
        env = Env()
        env.read_env()

        self.LOGGER_LEVEL = logging.DEBUG if env.bool("DEBUG", False) else logging.INFO
        self.PUBLIC_OR_LOCAL = env.str("PUBLIC_OR_LOCAL", "LOCAL")
        self.SERVICE_NAME = env.str("SERVICE_NAME", "db_template")

        self.HOST = env.str("HOST", '0.0.0.0')
        self.PORT = env.int('PORT', 7070)

        self.TOKENS_LIST = env.str('TOKENS_LIST')
        self.VERIFY_TOKEN = env.bool('VERIFY_TOKEN', True)

        self.JWT_SECRET_KEY = env.str("JWT_SECRET_KEY", "")
        self.JWT_ALGORITHM = env.str("JWT_ALGORITHM", "HS256")
        self.ACCESS_TOKEN_EXPIRE_MINUTES = env.int("ACCESS_TOKEN_EXPIRE_MINUTES", 15)
        self.REFRESH_TOKEN_EXPIRE_DAYS = env.int("REFRESH_TOKEN_EXPIRE_DAYS", 30)
        self.CRYPTOGRAPHY_KEY = env.str("CRYPTOGRAPHY_KEY", None)


    def get_origins_urls(self):
        if self.PUBLIC_OR_LOCAL == 'PUBLIC':
            return 'http://11.11.11.11'
        return 'http://localhost'


settings = Settings()