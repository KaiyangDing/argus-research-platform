"""M0.5 PG 访问层测试（施工手册 M0.5 测试清单）。"""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from argus.core.db import ping


@pytest.mark.pg
async def test_engine_ping(pg_engine: AsyncEngine) -> None:
    await ping(pg_engine)  # 不抛即通过：SELECT 1 通路 + 连接池可用
