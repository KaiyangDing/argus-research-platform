from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from argus.core.config import ArgusSettings


def build_engine(settings: ArgusSettings) -> AsyncEngine:
    """async engine 工厂：pool_pre_ping 挡掉容器重启后的半死连接。"""
    return create_async_engine(settings.pg_dsn, pool_pre_ping=True)


def build_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """session 工厂：expire_on_commit=False——提交后对象仍可读，async 语境标准姿势。"""
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping(engine: AsyncEngine) -> None:
    """SELECT 1 自检；失败原样抛——没有事实源时不假装能工作（02 §6.3）。"""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
