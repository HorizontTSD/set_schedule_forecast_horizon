# src/db_clients/config.py
from environs import Env


class DBConfig:
    def __init__(self):

        env = Env()
        env.read_env()

        self.DEV_MODE = env.bool("DEV_MODE", True)

        _DEV = {
            "DB_NAME": env.str("PG_DB_DEV"),
            "DB_USER": env.str("PG_USER_DEV"),
            "DB_PASSWORD": env.str("PG_PASSWORD_DEV"),
            "DB_HOST": env.str("PG_HOST_DEV"),
            "DB_PORT": env.int("PG_PORT_DEV"),
        }

        _PROD = {
            "DB_NAME": env.str("PG_DB_PROD"),
            "DB_USER": env.str("PG_USER_PROD"),
            "DB_PASSWORD": env.str("PG_PASSWORD_PROD"),
            "DB_HOST": env.str("PG_HOST_PROD"),
            "DB_PORT": env.int("PG_PORT_PROD"),
        }

        active = _DEV if self.DEV_MODE else _PROD

        self.DB_NAME = active["DB_NAME"]
        self.DB_USER = active["DB_USER"]
        self.DB_PASSWORD = active["DB_PASSWORD"]
        self.DB_HOST = active["DB_HOST"]
        self.DB_PORT = active["DB_PORT"]

    def url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    def get_async_url(self):
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


class TablesConfig:
    def __init__(self):
        self.USERS = "users"
        self.ORGANIZATIONS = "organizations"
        self.REFRESH_TOKENS = "refresh_tokens"
        self.CONNECTION_SETTINGS = "connection_settings"
        self.SCHEDULE_FORECASTING = "schedule_forecasting"
        self.ORGANIZATION_ACCESS = "organization_access"
        self.ROLES = "roles"
        self.PERMISSIONS = "permissions"
        self.ROLE_PERMISSIONS = "role_permissions"
        self.USER_ROLES = "user_roles"


class RolesConfig:
    def __init__(self):
        self.SUPERUSER = "superuser"
        self.ADMIN = "admin"
        self.USER = "user"

class DBSettings:
    def __init__(self):
        self.db = DBConfig()
        self.tables = TablesConfig()


db_settings = DBSettings()
