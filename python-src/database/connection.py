"""
Async SQLAlchemy database connection management.
Replaces MikroORM's Connection.mikro.ts.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from config import CONFIG
from database.models.shared.base_entity import Base

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _build_dsn() -> str:
    cfg = CONFIG["db"]["default"]
    return (
        f"postgresql+asyncpg://{cfg['username']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['name']}"
    )


async def initialize_orm() -> None:
    """Initialize the async SQLAlchemy engine (call once at startup)."""
    global _engine, _session_factory
    if _engine is None:
        _engine = create_async_engine(
            _build_dsn(),
            echo=False,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        _session_factory = async_sessionmaker(
            bind=_engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )


async def close_orm() -> None:
    """Dispose the engine gracefully (call on shutdown)."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


async def create_tables() -> None:
    """Create ORM tables if they don't exist."""
    if _engine is None:
        raise RuntimeError("ORM not initialized. Call initialize_orm() first.")
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("ORM not initialized. Call initialize_orm() first.")
    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Provide a transactional async session."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
