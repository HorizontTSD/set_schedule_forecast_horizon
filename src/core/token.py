# src/core/token.py
import logging
from typing import Dict, Any, Optional
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import pandas as pd
from sqlalchemy import select

from src.core.configuration.config import settings
from src.utils import jwt_utils
from src.models.user_models import User, Role, RolePermissions, Permission, UserRoles
from src.session import db_manager


logger = logging.getLogger(__name__)


# 1. Валидатор JWT-токена (для пользователей)
class JWTTokenValidator:
    def __init__(self):
        self.security = HTTPBearer()

    async def __call__(self, credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())) -> Dict[str, Any]:
        token = credentials.credentials
        try:
            payload = jwt_utils.decode_jwt_token(token, expected_type="access")
            
            user_id_str = payload.get("sub")
            if not user_id_str:
                logger.warning("Missing 'sub' in access token")
                raise HTTPException(status_code=401, detail="Invalid token")

            async with db_manager.get_db_session() as session:
                try:
                    user_id = int(user_id_str)
                except ValueError:
                    logger.warning(f"Invalid user ID '{user_id_str}' in access token")
                    raise HTTPException(status_code=401, detail="Invalid token")

                result = await session.execute(select(User).where(User.id == user_id))
                user_obj = result.scalar_one_or_none()

                if not user_obj:
                    logger.warning(f"User with ID {user_id} not found")
                    raise HTTPException(status_code=401, detail="User not found")

                await session.refresh(user_obj, ["roles"])

                payload["organization_id"] = user_obj.organization_id
                payload["roles"] = [role.name for role in user_obj.roles]

                permissions_query = (
                    select(Permission.code)
                    .join(RolePermissions, Permission.id == RolePermissions.c.permission_id)
                    .join(Role, Role.id == RolePermissions.c.role_id)
                    .join(UserRoles, UserRoles.c.role_id == Role.id)
                    .where(UserRoles.c.user_id == user_id)
                )
                permissions_result = await session.execute(permissions_query)
                payload["permissions"] = [row[0] for row in permissions_result.fetchall()]

            logger.info(f"JWT access token validated and data fetched for user_id={payload['sub']}")
            return payload

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Unexpected error during JWT validation in JWTTokenValidator: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Internal token validation error")

# 2. Статический валидатор
class StaticTokenValidator:
    def __init__(self):
        self.security = HTTPBearer()
        self.valid_tokens: Optional[list[str]] = None

    def load_tokens(self) -> list[str]:
        try:
            tokens_link = settings.TOKENS_LIST
            if not tokens_link:
                raise ValueError("Environment variable TOKENS_LIST is not set or empty.")

            logger.info(f"Loading static tokens from: {tokens_link}")
            df = pd.read_csv(tokens_link, encoding='utf-8')

            valid_tokens = df.loc[df['source'] == settings.SERVICE_NAME, 'token'].tolist()
            logger.info(f"Loaded {len(valid_tokens)} static tokens for {settings.SERVICE_NAME}")

            if not valid_tokens:
                logger.warning(f"No static tokens found for source: {settings.SERVICE_NAME}")
                unique_sources = df['source'].unique().tolist() if 'source' in df.columns else []
                logger.warning(f"Available sources: {unique_sources}")

            return valid_tokens
        except Exception as e:
            logger.error(f"Failed to load static tokens: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail="Token validation failed")

    async def __call__(self, credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
        if self.valid_tokens is None:
            self.valid_tokens = self.load_tokens()

        token = credentials.credentials
        logger.info(f"Validating static token: {token[:10]}...")

        if token not in self.valid_tokens:
            logger.warning(f"Invalid static token: {token[:10]}...")
            raise HTTPException(status_code=401, detail="Unauthorized.")

        logger.info(f"Static token validated: {token[:10]}...")
        return token

jwt_token_validator = JWTTokenValidator()
static_token_validator = StaticTokenValidator()
