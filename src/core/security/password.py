from passlib.context import CryptContext
from passlib.exc import UnknownHashError

# Единый контекст хеширования паролей для всего сервиса
pwd_context = CryptContext(
    schemes=[
        "bcrypt",
    ],
    deprecated="auto",
)


def hash_password(plain_password: str) -> str:
    return pwd_context.hash(plain_password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
        Безопасно проверяет пароль
        Возвращает False, если хэш не распознан
    """
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except UnknownHashError:
        return False
