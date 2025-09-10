from passlib.context import CryptContext
from passlib.exc import UnknownHashError
from cryptography.fernet import Fernet
from src.core.configuration.config import settings

cipher = Fernet(settings.CRYPTOGRAPHY_KEY)

def encrypt_password(plain_password: str) -> str:
    """
    Шифрует пароль и возвращает строку.
    """
    return cipher.encrypt(plain_password.encode()).decode()

def decrypt_password(encrypted_password: str) -> str:
    """
    Расшифровывает пароль и возвращает исходный plain текст.
    """
    return cipher.decrypt(encrypted_password.encode()).decode()


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
