from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Integer, Boolean, JSON, TIMESTAMP, CheckConstraint, func
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
    connections: Mapped[list["ConnectionSettings"]] = relationship(
        "ConnectionSettings", back_populates="organization"
    )
    schedule_forecastings: Mapped[list["ScheduleForecasting"]] = relationship(
        "ScheduleForecasting",
        back_populates="organization",
    )


class ConnectionSettings(ORMBase):
    __tablename__ = db_settings.tables.CONNECTION_SETTINGS

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(Integer, ForeignKey("organizations.id"), nullable=False)
    connection_schema: Mapped[str] = mapped_column(String, nullable=False)
    db_name: Mapped[str] = mapped_column(String, nullable=False)
    connection_name: Mapped[str] = mapped_column(String, nullable=False)
    host: Mapped[str] = mapped_column(String, nullable=False)
    port: Mapped[int] = mapped_column(Integer, default=5432, nullable=False)
    ssl: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    db_user: Mapped[str] = mapped_column(String, nullable=False)
    db_password: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    organization: Mapped["Organization"] = relationship("Organization", back_populates="connections")


class ScheduleForecasting(ORMBase):
    __tablename__ = db_settings.tables.SCHEDULE_FORECASTING

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    organization_id: Mapped[int] = mapped_column(ForeignKey("organizations.id"), nullable=False)
    connection_id: Mapped[int] = mapped_column(Integer, nullable=False)
    data_name: Mapped[str] = mapped_column(String, nullable=False)
    source_table: Mapped[str] = mapped_column(String, nullable=False)
    time_column: Mapped[str] = mapped_column(String, nullable=False)
    target_column: Mapped[str] = mapped_column(String, nullable=False)
    discreteness: Mapped[int] = mapped_column(Integer, nullable=False)
    count_time_points_predict: Mapped[int] = mapped_column(Integer, nullable=False)
    target_db: Mapped[str] = mapped_column(String, nullable=False, default="self_host")
    methods_predict: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP, server_default=func.now(), nullable=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    organization: Mapped["Organization"] = relationship("Organization", back_populates="schedule_forecastings")

    __table_args__ = (
        CheckConstraint("target_db IN ('user','self_host')", name="check_target_db"),
    )



