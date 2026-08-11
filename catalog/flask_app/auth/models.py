"""SQLAlchemy models dedicated to human web users."""

from __future__ import annotations

from flask_security import RoleMixin, UserMixin
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import Mapped, mapped_column, relationship

db = SQLAlchemy()

roles_users = Table(
    "roles_users",
    db.metadata,
    Column("user_id", Integer(), ForeignKey("user.id", ondelete="CASCADE")),
    Column("role_id", Integer(), ForeignKey("role.id", ondelete="CASCADE")),
)


class Role(db.Model, RoleMixin):
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(255))
    permissions: Mapped[list[str]] = mapped_column(
        db.JSON, nullable=False, default=list
    )


class User(db.Model, UserMixin):
    id: Mapped[int] = mapped_column(primary_key=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password: Mapped[str] = mapped_column(String(255), nullable=False)
    active: Mapped[bool] = mapped_column(Boolean(), nullable=False, default=True)
    fs_uniquifier: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    roles = relationship(
        "Role", secondary=roles_users, backref="users", lazy="selectin"
    )
