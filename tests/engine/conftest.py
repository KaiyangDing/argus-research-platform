"""engine 测试专用夹具（M1 册 M1.1/M1.4）。

网络封禁：M1 全部测试零真实 LLM 调用是毕业验收项本身（M1 册第 0 节第 5 行），
autouse fixture 把 httpx 的同步/异步 send 一律替换为抛错——机制保障而非君子协定。
"""

from collections.abc import AsyncIterator

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """engine 测试内任何 httpx 出网即抛 RuntimeError（M1.10 有专项断言用例）。"""

    def _blocked(*args: object, **kwargs: object) -> None:
        raise RuntimeError("M1 tests must not touch network")

    monkeypatch.setattr(httpx.AsyncClient, "send", _blocked)
    monkeypatch.setattr(httpx.Client, "send", _blocked)


@pytest.fixture
async def graph_session(pg_engine: AsyncEngine, migrated: None) -> AsyncIterator[AsyncSession]:
    """图六表清空后的 AsyncSession（engine [pg] 用例专用；evidence_chunks 语料表不动）。"""
    async with pg_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE research_tasks, plan_nodes, plan_edges, artifacts, "
                "node_steps, replan_signals CASCADE"
            )
        )
    async with build_sessionmaker(pg_engine)() as session:
        yield session
