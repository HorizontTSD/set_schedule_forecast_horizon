# src/models/user_model.py
from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Table, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db_clients.config import db_settings
from src.models.base_model import ORMBase
from src.models.organization_models import Organization  # noqa: E402

# Таблица связи многие-ко-многим для пользователей и ролей
UserRoles = Table(
    db_settings.tables.USER_ROLES,
    ORMBase.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True),
)

# Таблица связи многие-ко-многим для ролей и разрешений
RolePermissions = Table(
    db_settings.tables.ROLE_PERMISSIONS,
    ORMBase.metadata,
    Column('role_id', Integer, ForeignKey('roles.id'), primary_key=True),
    Column('permission_id', Integer, ForeignKey('permissions.id'), primary_key=True),
)


class User(ORMBase):
    __tablename__ = db_settings.tables.USERS
    
    organization_id: Mapped[int] = mapped_column(ForeignKey('organizations.id'))
    login: Mapped[str] = mapped_column(String)
    first_name: Mapped[str] = mapped_column(String)
    last_name: Mapped[str] = mapped_column(String)
    nickname: Mapped[str | None] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    password: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    last_activity: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_blocked: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    refresh_tokens: Mapped[list['RefreshToken']] = relationship('RefreshToken', back_populates='user')
    organization: Mapped['Organization'] = relationship(
        'Organization',
        back_populates='users',
        foreign_keys=[organization_id],
        primaryjoin='User.organization_id == Organization.id',
    )

    roles: Mapped[list['Role']] = relationship(
        'Role',
        secondary=UserRoles,
        back_populates='users',
        lazy='selectin'
    )


class RefreshToken(ORMBase):
    __tablename__ = db_settings.tables.REFRESH_TOKENS
    
    user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
    token: Mapped[str] = mapped_column(String)
    jti: Mapped[str] = mapped_column(String(255))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)

    user: Mapped['User'] = relationship('User', back_populates='refresh_tokens')


class Role(ORMBase):
    __tablename__ = db_settings.tables.ROLES

    name: Mapped[str] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    users: Mapped[list['User']] = relationship(
        'User',
        secondary=UserRoles,
        back_populates='roles',
    )

    permissions: Mapped[list['Permission']] = relationship(
        'Permission',
        secondary=RolePermissions,
        back_populates='roles',
        lazy='selectin'
    )


class Permission(ORMBase):
    __tablename__ = db_settings.tables.PERMISSIONS

    code: Mapped[str] = mapped_column(String)
    description: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())

    roles: Mapped[list['Role']] = relationship(
        'Role',
        secondary=RolePermissions,
        back_populates='permissions',
    )

class Tables:
    def __init__(self):
        self.User = User
        self.Role = Role
        self.Permission = Permission
        self.RefreshToken = RefreshToken
        self.UserRoles = UserRoles
        self.RolePermissions = RolePermissions
