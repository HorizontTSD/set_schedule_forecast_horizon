
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db_clients.config import db_settings
from src.models.base_model import ORMBase


class Organization(ORMBase):
    __tablename__ = db_settings.tables.ORGANIZATIONS

    name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=func.now())
    owner_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
    
    
    # Явная связь с владельцем организации, чтобы избежать неоднозначности
    owner: Mapped['User'] = relationship(
        'User',
        foreign_keys='Organization.owner_id',
        uselist=False,
    )

    # Явная связь с пользователями по полю User.organization_id
    users: Mapped[list['User']] = relationship(
        'User',
        back_populates='organization',
        foreign_keys='User.organization_id',
        primaryjoin='Organization.id == User.organization_id',
    )