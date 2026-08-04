"""共享 fixture：PG 可达性、迁移、每测试 engine（施工手册 M0.5 条目）。

Windows 事件循环口径【全项目唯一口径】：保持默认 Proactor 策略——
SelectorEventLoop 不支持 asyncio 子进程，全局固定会炸掉 M3 的 MCP stdio 测试。

pg_engine 为 function 级（手册原写 session 级，实现细则调整并已报告）：
pytest-asyncio 默认每个测试一个事件循环，asyncpg 连接与创建它的循环绑定，
session 级共享 engine 会跨循环报错；改为 per-test engine + session 级一次性可达性探测。
"""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine

from argus.core.config import ArgusSettings
from argus.core.db import build_engine, ping

_DEV_DSN = "postgresql+asyncpg://argus:argus@127.0.0.1:5432/argus"


def _settings(**overrides: Any) -> ArgusSettings:
    return ArgusSettings(_env_file=None, **overrides)  # type: ignore[call-arg]


async def _try_ping(dsn: str) -> None:
    engine = build_engine(_settings(pg_dsn=dsn))
    try:
        await ping(engine)
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    """PG 可达性探测：不可达时本地 skip；CI（ARGUS_TEST_REQUIRE_PG=1）跳过升级为失败。"""
    dsn = os.environ.get("ARGUS_PG_DSN", _DEV_DSN)
    try:
        asyncio.run(_try_ping(dsn))
    except (
        OSError,
        TimeoutError,
        DBAPIError,
    ) as exc:  # 拒连/超时/认证协议错=不可达；DSN 格式错原样抛
        if os.environ.get("ARGUS_TEST_REQUIRE_PG") == "1":
            pytest.fail(f"PG 不可达但 ARGUS_TEST_REQUIRE_PG=1（CI 跳过=失败）：{exc}")
        pytest.skip(f"PG 不可达（docker compose up -d pg 后重跑）：{exc}")
    return dsn


@pytest.fixture(scope="session")
def migrated(pg_dsn: str) -> None:
    """alembic upgrade head（幂等）；env.py 经 get_settings 读 ARGUS_PG_DSN。"""
    from alembic.config import Config

    from alembic import command

    os.environ.setdefault("ARGUS_PG_DSN", pg_dsn)
    root = Path(__file__).resolve().parent.parent
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    command.upgrade(cfg, "head")


@pytest.fixture
async def pg_engine(pg_dsn: str) -> AsyncIterator[AsyncEngine]:
    engine = build_engine(_settings(pg_dsn=pg_dsn))
    try:
        yield engine
    finally:
        await engine.dispose()
