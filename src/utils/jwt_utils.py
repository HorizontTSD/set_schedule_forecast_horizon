# src/utils/jwt_utils.py
import logging
from datetime import datetime, timedelta
import uuid
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from sqlalchemy import update
from src.models.user_models import RefreshToken
from fastapi import HTTPException, status

from src.core.configuration.config import settings

logger = logging.getLogger(__name__)

# --- Функции для создания токенов ---

async def create_access_token(user_id: int) -> str:
    """Создает JWT access токен."""
    expires_delta = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {
        "sub": str(user_id),
        "exp": datetime.utcnow() + expires_delta,
        "type": "access",
    }
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    logger.debug(f"Created access token for user_id={user_id}")
    return encoded_jwt


async def create_refresh_token(user_id: int) -> tuple[str, str]:
    """Создает JWT refresh токен и возвращает (token, jti)."""
    expires_delta = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    jti = uuid.uuid4().hex
    to_encode = {
        "sub": str(user_id),
        "jti": jti,
        "exp": datetime.utcnow() + expires_delta,
        "type": "refresh",
    }
    encoded_jwt = jwt.encode(
        to_encode,
        settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
    )
    logger.debug(f"Created refresh token for user_id={user_id}, jti={jti}")
    return encoded_jwt, jti


# --- Функции для декодирования и валидации токенов ---

def decode_jwt_token(token: str, expected_type: str = None) -> dict:
    """
    Декодирует и проверяет базовую валидность JWT токена.
    :param token: Сам JWT токен.
    :param expected_type: Ожидаемый тип токена ('access', 'refresh').
    :return: Payload токена.
    :raises HTTPException: Если токен недействителен.
    """
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET_KEY,
            algorithms=[settings.JWT_ALGORITHM],
        )

        if expected_type and payload.get("type") != expected_type:
            logger.warning(
                f"Invalid token type: expected '{expected_type}', got '{payload.get('type')}'"
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token type",
            )

        logger.debug(
            f"Decoded JWT token for sub={payload.get('sub')}, type={payload.get('type')}"
        )
        return payload

    except ExpiredSignatureError:
        logger.warning("JWT token expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
        )
    except InvalidTokenError as e:
        logger.warning(f"Invalid JWT token: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    except Exception as e:
        logger.error(f"Unexpected error during JWT decoding: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal token validation error",
        )


async def revoke_existing_tokens(session, user_id: int):
    """Отзывает все активные refresh-токены пользователя."""
    stmt = (
        update(RefreshToken)
        .where(
            RefreshToken.user_id == user_id,
            RefreshToken.revoked == False,
            RefreshToken.expires_at > datetime.utcnow(),
        )
        .values(revoked=True)
    )
    result = await session.execute(stmt)
    logger.debug(
        f"Revoked {result.rowcount} existing refresh tokens for user_id={user_id}"
    )
